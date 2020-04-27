local _M = {}
local http = require "http"
local json = require "cjson"

local log = ngx.log
local ERR = ngx.ERR
local WARN = ngx.WARN
local INFO = ngx.INFO

local ngx_time = ngx.time
local ngx_timer_at = ngx.timer.at
local ngx_worker_id = ngx.worker.id
local ngx_worker_exiting = ngx.worker.exiting
local ngx_sleep = ngx.sleep


local DEFAULT_HTTP_PORT = 8080
local DEFAULT_HTTP_WEIGHT = 100
local DEFAULT_CHECK_URL = "/health"
local DEFAULT_SLOW_START = 30
local PEER_STATUS_UP = "up"

local WATCH_TIMEOUT = 10000 -- by ms
local WATCH_LOCK_TIMEOUT = WATCH_TIMEOUT / 1000 + 2 -- by second

_M.data = {}

local function info(...)
    log(INFO, "syncer_k8s: ", ...)
end

local function warn(...)
    log(WARN, "syncer_k8s: ", ...)
end

local function errlog(...)
    log(ERR, "syncer_k8s: ", ...)
end

local function peersEqual(ta, tb)
    if not ta or not tb then
        return false
    end

    if #ta ~= #tb then
        return false
    end
    local comp = {}
    for i, a in pairs(ta) do
        comp[a.host .. ":" .. a.port] = true
    end
    for j, b in pairs(tb) do
        if not comp[b.host .. ":" .. b.port] then
            return false
        end
        comp[b.host .. ":" .. b.port] = nil
    end
    return 0 == #comp
end

local function getLock()
    -- only the worker who get the lock can sync from k8s.
    -- the lock keeps 10 seconds, little longer than http fetch timeout
    if _M.lock == true then
        return true
    end

    local ok, err = _M.conf.storage:add("lock", true, WATCH_LOCK_TIMEOUT)
    if not ok then
        if err == "exists" then
            return nil
        end
        errlog("GET LOCK: failed to add key \"lock\": " .. err)
        return nil
    end
    _M.lock = true
    return true
end

local function refreshLock()
    local ok, err = _M.conf.storage:set("lock", true, WATCH_LOCK_TIMEOUT)
    if not ok then
        if err == "exists" then
            return nil
        end
        errlog("REFRESH LOCK: failed to set \"lock\"" .. err)
        return nil
    end
    return true
end

local function releaseLock()
    _M.conf.storage:delete("lock")
    _M.lock = nil
    return true
end

local function newPeers(sets)
    if not sets or #sets == 0 or not sets[1].addresses then
        return nil
    end

    local peers = {}
    local port = DEFAULT_HTTP_PORT
    for _, p in pairs(sets[1].ports) do
        if p.name == "http" then
            port = p.port
            break
        end
    end
    for _, addr in ipairs(sets[1].addresses) do
        table.insert(peers, {
            host   = addr.ip,
            port   = port,
            weight = DEFAULT_HTTP_WEIGHT,
            status = PEER_STATUS_UP,
            check_url  = DEFAULT_CHECK_URL,
            slow_start = DEFAULT_SLOW_START
        })
    end
    return peers
end

local function saveList()
    local allname = {}
    for name, _ in pairs(_M.data) do
        table.insert(allname, name)
    end

    _M.conf.storage:set("_allname", table.concat(allname, "|"))
    return
end

local function updateService(data)
    local name = data.object.metadata.name
    local nver = tonumber(data.object.metadata.resourceVersion)
    if _M.data[name] and _M.data[name].version == nver then
        info("same version, skip update ", name)
        return
    end

    local peers = newPeers(data.object.subsets)
    if _M.data[name] and peersEqual(peers, _M.data[name].peers) then
        info("same peers, skip update ", name)
        return
    end

    _M.data[name] = {
        version = nver,
        peers = peers
    }

    -- update peers before version, in case of picker read version first,
    -- may cause picker get new ver and old peer list
    _M.conf.storage:set(name .. "|peers", json.encode(peers))
    _M.conf.storage:set(name .. "|version", nver)
    saveList()

    errlog("update service: " .. name)
    return
end

local function deleteService(data)
    local name = data.object.metadata.name
    _M.data[name] = nil

    _M.conf.storage:delete(name .. "|version")
    _M.conf.storage:delete(name .. "|peers")
    saveList()

    errlog("delete service: " .. name)
    return
end

local function handleChunk(chunk)
    local err
    for line in chunk:gmatch("[^\n]+") do
        local ok, data = pcall(json.decode, line)
        -- the last line maybe not a full line
        -- return the half line
        if not ok and chunk:sub(-1) ~= "\n" then
            return line, nil
        end

        if data.type == "ERROR" then
            errlog("got error response: ", line)
            err = "error message"
        elseif data.type == "ADDED" or data.type == "MODIFIED" then
            err = updateService(data)
        elseif data.type == "DELETED" then
            err = deleteService(data)
        else
            errlog("got unkown message: ", line)
            err = "unknown message"
        end
    end  
    return "", err
end

local function watch(premature)
    local conf = _M.conf
    if premature or ngx_worker_exiting() then
        releaseLock()
        return
    end

    -- If we cannot acquire the lock, wait 1 second
    if not getLock() then
        errlog("waiting 1s for prev worker to exit...")
        local ok, err = ngx_timer_at(1, watch)
        if not ok then
            errlog("cannot start k8s watcher: ", err)
        end
        return
    end

    refreshLock()

    local client = http:new()
    client:set_timeout(WATCH_TIMEOUT)
    local ok, err = client:connect(conf.apiserver_host, conf.apiserver_port)
    if not ok then
        errlog("cannot connect to apiserver: ", err)
        return
    end

    local session, err = client:ssl_handshake(nil, conf.apiserver_host .. ":" .. conf.apiserver_port, false)
    
    local path = "/api/v1/namespaces/" .. conf.namespace .. "/endpoints/?watch=true"
    local res, err = client:request({
        path = path,
        method="GET",
        headers={ ["Authorization"] = "Bearer " .. conf.token },
        ssl_verify = false
    })

    if err then
        errlog("restart watcher because request fail: ", err)
        ngx_timer_at(1, watch)
        return
    end

    if res.status ~= 200 then
        errlog("restart watcher because status not 200: ", err)
        ngx_timer_at(1, watch)
        return
    end

    local reader = res.body_reader
    local buffer = ""
    local chunk = ""
    local err = nil
    repeat
        chunk, err = reader(8192)
        if err then
            if err ~= "timeout" then
                errlog("cannot read from response: ", err)
            end
            break
        end
        if chunk then
            buffer, err = handleChunk(buffer .. chunk)
            if err then
                errlog("cannot process chunked response: ", buffer .. chunk)
                break
            end
        end
    until not chunk

    -- restart the timer for next loop
    local ok, err = ngx_timer_at(0, watch)
    if not ok then
        errlog("cannot start k8s watcher: ", err)
    end
    return
end

function _M.init(conf)
    -- Only one worker start the syncer, here will use worker_id == 0
    if ngx_worker_id() ~= 0 then
        return
    end

    -- Verify the config
    if conf.apiserver == "" or
       conf.namespace == "" or
       conf.token == "" or 
       conf.storage == "" then
        errlog("syncer_k8s init config invalid")
        return
    end

    _M.conf = conf

    local dict = _M.conf.storage

    local allname = dict:get("_allname")

    -- Load data from shm
    if allname then
        _M.data = {}
        for name in allname:gmatch("[^|]+") do
            upst = dict:get(name .. "|peers")
            vers = dict:get(name .. "|version")

            local ok, data = pcall(json.decode, upst)
            if ok then
                _M.data[name] = {version = vers, peers = data}
            end
        end
    end

    -- Start the k8s watcher
    local ok, err = ngx_timer_at(0, watch)
    if not ok then
        errlog("cannot start k8s watcher: ", err)
    end
end

return _M
