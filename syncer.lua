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

_M.ready = false
_M.data = {}

local function info(...)
    log(INFO, "syncer: ", ...)
end

local function warn(...)
    log(WARN, "syncer: ", ...)
end

local function errlog(...)
    log(ERR, "syncer: ", ...)
end

local function copyTab(st)
    local tab = {}
    for k, v in pairs(st or {}) do
        if type(v) ~= "table" then
            tab[k] = v
        else
            tab[k] = copyTab(v)
        end
    end
    return tab
end

local function indexOf(t, e)
    for i=1,#t do
        if t[i].host == e.host and t[i].port == e.port then
            return i
        end
    end
    return nil
end

local function basename(s)
    local x, y = s:match("(.*)/([^/]*)/?")
    return y, x
end

local function splitAddr(s)
    if not s then
        return "127.0.0.1", 0, "nil args"
    end
    host, port = s:match("(.*):([0-9]+)")

    -- verify the port
    local p = tonumber(port)
    if p == nil then
        return "127.0.0.1", 0, "port invalid"
    elseif p < 1 or p > 65535 then
        return "127.0.0.1", 0, "port invalid"
    end

    -- verify the ip addr
    local chunks = {host:match("(%d+)%.(%d+)%.(%d+)%.(%d+)")}
    if (#chunks == 4) then
        for _,v in pairs(chunks) do
            if (tonumber(v) < 0 or tonumber(v) > 255) then
                return "127.0.0.1", 0, "host invalid"
            end
        end
    else
        return "127.0.0.1", 0, "host invalid"
    end

    -- verify pass
    return host, port, nil
end

local function getLock()
    -- only the worker who get the lock can sync from etcd.
    -- the lock keeps 150 seconds.
    if _M.lock == true then
        return true
    end

    local ok, err = _M.conf.storage:add("lock", true, 12)
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
    local ok, err = _M.conf.storage:set("lock", true, 25)
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

local function newPeer(key, value)
    local name, ipport = key:match(_M.conf.etcd_path .. '([^/]+)/([^/]+)$')
    local h, p, err = splitAddr(ipport)
    if err then
        return {}, name, err
    end

    local ok, cfg = pcall(json.decode, value)

    local w, s, c, t = 1, "up", "/", 0
    if type(cfg) == "table" then
        w = cfg.weight     or 1
        s = cfg.status     or "up"
        t = cfg.slow_start or 0

        if cfg.check_url ~= "" then
            c = cfg.check_url
        end
    end

    return { host   = h,
             port   = tonumber(p),
             weight = w,
             status = s,
             check_url  = c,
             slow_start = t
         }, name, nil
end

local function save(name)
    local dict = _M.conf.storage

    -- no name means save all
    if not name then
        for name, upstream in pairs(_M.data) do
            if name ~= "_version" then
                dict:set(name .. "|peers", json.encode(upstream.peers))
                dict:set(name .. "|version", upstream.version)
            end
        end
    else
        -- remove the deleted upstream
        if not _M.data[name] then
            dict:delete(name .. "|peers")
            dict:delete(name .. "|version")
        -- save the updated upstream
        else
            dict:set(name .. "|peers", json.encode(_M.data[name].peers))
            dict:set(name .. "|version", _M.data[name].version)
        end
    end

    local allname = {}
    for name, _ in pairs(_M.data) do
        if name ~= "_version" then
           allname[#allname+1] = name
        end
    end

    dict:set("_allname", table.concat(allname, "|"))
    dict:set("_version", _M.data._version)

    return
end

local function fetch(url)
    local client = http:new()
    client:set_timeout(10000)
    client:connect(_M.conf.etcd_host, _M.conf.etcd_port)

    local res, err = client:request({path=url, method="GET"})
    if err then
        return nil, err
    end

    local body, err = res:read_body()
    if err then
        return nil, err
    end

    local ok, data = pcall(json.decode, body)
    if not ok then
        return nil, data
    end

    data.etcdIndex = res.headers["x-etcd-index"]
    return data, nil
end

local function watch(premature, index)

    local conf = _M.conf
    if premature or ngx_worker_exiting() then
        releaseLock()
        return
    end

    -- If we cannot acquire the lock, wait 1 second
    if not getLock() then
        info("Waiting 1s for pre-worker to exit...")
        local ok, err = ngx_timer_at(1, watch, index)
        if not ok then
            errlog("Error start watch: ", err)
        end
        return
    end

    refreshLock()

    local nextIndex
    local url = "/v2/keys" .. conf.etcd_path

    -- First time to fetch all the upstreams.
    if index == nil then
        local upstreamList, err = fetch(url .. "?recursive=true")
        if err then
            errlog("When fetch from etcd: " .. err)
            ngx_sleep(1)
            goto continue
        end

        if upstreamList.errorCode then
            errlog("When fetch from etcd: " .. upstreamList.message)
            ngx_sleep(1)
            goto continue
        end

        if not upstreamList.node.nodes then
            warn("Empty dir: " .. upstreamList.message)
            ngx_sleep(1)
            goto continue
        end

        for n, s in pairs(upstreamList.node.nodes) do
            local name = basename(s.key)
            _M.data[name] = {version=tonumber(upstreamList.etcdIndex), peers={}}

            local upstreamInfo, err = fetch(url .. name .. "?recursive=true")
            if err then
                errlog("When fetch from etcd: " .. err)
                ngx_sleep(1)
                goto continue
            end

            if upstreamList.errorCode then
                errlog("When fetch from etcd: " .. upstreamList.message)
                ngx_sleep(1)
                goto continue
            end

            info("full fetching: " .. json.encode(upstreamInfo))
            if upstreamInfo.node.dir then
                if upstreamInfo.node.nodes then
                    for i, j in pairs(upstreamInfo.node.nodes) do
                        local peer, _, err = newPeer(j.key, j.value)
                        if not err then
                            _M.data[name].peers[#_M.data[name].peers+1] = peer
                        end

                    end
                end
                -- Keep the version is the newest response x-etcd-index
                _M.data[name].version = tonumber(upstreamInfo.etcdIndex)
                _M.data._version = _M.data[name].version
            end
        end

        if _M.data._version then
            nextIndex = _M.data._version + 1
        end
        save()
        _M.conf.storage:set("ready", true)

    -- Watch the change and update the data.
    else
        local s_url = url .. "?wait=true&recursive=true&waitIndex=" .. index
        local change, err = fetch(s_url)
        if err == "timeout" then
            nextIndex = _M.data._version + 1
            ngx_sleep(1)
            goto continue
        elseif err ~= nil then
            errlog("Error when watching etcd: ", err)
            ngx_sleep(1)
            goto continue
        end

        if change.errorCode == 401 then
            nextIndex = nil
            ngx_sleep(1)
            goto continue
        end

        info("recv a change: " .. json.encode(change))

        local action = change.action
        if change.node.dir then
            local name = change.node.key:match(_M.conf.etcd_path .. '([^/]+)$')
            if name then
                if action == "delete" then
                    _M.data[name] = nil
                elseif action == "set" or action == "update" or action == "compareAndSwap" then
                    if not _M.data[name] then
                        _M.data[name] = {version=tonumber(change.node.modifiedIndex), peers={}}
                    end
                end
            end
            _M.data._version = tonumber(change.node.modifiedIndex)
            save(name)
        else
            local peer, name, err = newPeer(change.node.key, change.node.value)
            if not err then
                if action == "delete" or action == "expire" then
                    errlog("DELETE [".. name .. "]: " .. peer.host .. ":" .. peer.port)
                    if _M.data[name] then
                        table.remove(_M.data[name].peers, indexOf(_M.data[name].peers, peer))
                        _M.data[name].version = change.node.modifiedIndex
                        if 0 == #_M.data[name].peers then
                            _M.data[name] = nil
                        end
                    end
                elseif action == "create" or action == "set" or action == "update" or action == "compareAndSwap" then
                    if not _M.data[name] then
                        _M.data[name] = {version=tonumber(change.node.modifiedIndex), peers={peer}}
                        errlog("ADD [" .. name .. "]: " .. peer.host ..":".. peer.port)
                    else
                        local index = indexOf(_M.data[name].peers, peer)
                        if index == nil then
                            errlog("ADD [" .. name .. "]: " .. peer.host ..":".. peer.port)
                            peer.start_at = ngx_time()
                            table.insert(_M.data[name].peers, peer)
                        else
                            errlog("MODIFY [" .. name .. "]: " .. peer.host ..":".. peer.port .. " " .. change.node.value)
                            peer.start_at = ngx_time()
                            _M.data[name].peers[index] = peer
                        end
                        _M.data[name].version = tonumber(change.node.modifiedIndex)
                    end
                end
            else
                errlog(err)
            end
            _M.data._version = tonumber(change.node.modifiedIndex)
            save(name)
        end
        nextIndex = _M.data._version + 1
    end

    ::continue::
    -- Start the update cycle.
    local ok, err = ngx_timer_at(0, watch, nextIndex)
    if not ok then
        errlog("Error start watch: ", err)
    end
    return
end

function _M.init(conf)
    -- Only one worker start the syncer, here will use worker_id == 0
    if ngx_worker_id() ~= 0 then
        return
    end
    _M.conf = conf

    local nextIndex

    local dict = _M.conf.storage

    local allname = dict:get("_allname")
    local version = dict:get("_version")

    -- Not synced before, not reload but newly start
    if not allname or not version then
        local ok, err = ngx_timer_at(0, watch, nextIndex)
        if not ok then
            errlog("Error start watch: " .. err)
        end
        return

    -- Load data from shm
    else
        _M.data = {}
        for name in allname:gmatch("[^|]+") do
            upst = dict:get(name .. "|peers")
            vers = dict:get(name .. "|version")

            local ok, data = pcall(json.decode, upst)

            -- Any error occurs, goto full fetch
            if not ok then
                local ok, err = ngx_timer_at(0, watch, nextIndex)
                if not ok then
                    errlog("Error start watch: " .. err)
                end
                return
            else
                _M.data[name] = {version = vers, peers = data}
            end
        end

        _M.data._version = version
        nextIndex = _M.data._version + 1
    end

    -- Start the etcd watcher
    local ok, err = ngx_timer_at(0, watch, nextIndex)
    if not ok then
        errlog("Error start watch: " .. err)
    end

end

return _M
