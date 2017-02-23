local _M = {}
local http = require "http"
local json = require "cjson"

local ngx_log = ngx.log
local ngx_ERR = ngx.ERR
local ngx_time = ngx.time
local ngx_timer_at = ngx.timer.at
local ngx_worker_id = ngx.worker.id
local ngx_worker_exiting = ngx.worker.exiting

_M.ready = false
_M.data = {}

local function log(c)
    ngx_log(ngx_ERR, c)
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
        log("GET LOCK: failed to add key \"lock\": " .. err)
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
        log("REFRESH LOCK: failed to set \"lock\"" .. err)
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
    local ipport, ret = basename(key)
    local name = basename(ret)
    local h, p, err = splitAddr(ipport)
    if err then
        return {}, name, err
    end

    local ok, cfg = pcall(json.decode, value)

    local w, s, c, t = 1, "up", "/", 0
    if type(cfg) == "table" then
        w = cfg.weight     or 1
        s = cfg.status     or "up"
        c = cfg.check_url  or "/"
        t = cfg.slow_start or 0
    end
    return { host   = h,
             port   = tonumber(p),
             weight = w,
             status = s,
             check  = c,
             slow_start = t
         }, name, nil
end

local function save()
    local dict = _M.conf.storage

    local allname = ""
    for name, upstream in pairs(_M.data) do
        if name ~= "_version" then
            dict:set(name .. "|version", upstream.version)
            dict:set(name .. "|peers", json.encode(upstream.peers))
            allname = allname .. "|" .. name
        end
    end
    dict:set("_allname", allname)
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
        log("Waiting 1s for pre-worker to exit...")
        local ok, err = ngx_timer_at(1, watch, index)
        if not ok then
            log("Error start watch: ", err)
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
            log("When fetch from etcd: " .. err)
            goto continue
        end

        if upstreamList.errorCode then
            log("When fetch from etcd: " .. upstreamList.message)
            goto continue
        end

        for n, s in pairs(upstreamList.node.nodes) do
            local name = basename(s.key)
            _M.data[name] = {version=tonumber(upstreamList.etcdIndex), peers={}}

            local upstreamInfo, err = fetch(url .. name .. "?recursive=true")
            if err then
                log("When fetch from etcd: " .. err)
                goto continue
            end

            if upstreamList.errorCode then
                log("When fetch from etcd: " .. upstreamList.message)
                goto continue
            end

            log("full fetching: " .. json.encode(upstreamInfo))
            if upstreamInfo.node.dir and upstreamInfo.node.nodes then
                for i, j in pairs(upstreamInfo.node.nodes) do
                    local peer, _, err = newPeer(j.key, j.value)
                    if not err then
                        _M.data[name].peers[#_M.data[name].peers+1] = peer
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
            goto continue
        elseif err ~= nil then
            log("Error when watching etcd: ", err)
            goto continue
        end

        if change.errorCode == 401 then
            nextIndex = nil
            goto continue
        end

        -- log("recv a change: " .. json.encode(change))

        local action = change.action
        if change.node.dir then
            local target = change.node.key:match(_M.conf.etcd_path .. '(.*)/?')
            if action == "delete" then
                _M.data[target] = nil
            elseif action == "set" or action == "update" then
                local new_svc = target:match('([^/]*).*')
                if not _M.data[new_svc] then
                    _M.data[new_svc] = {version=tonumber(change.etcdIndex), peers={}}
                end
            end
        else
            local peer, name, err = newPeer(change.node.key, change.node.value)
            if not err then
                if action == "delete" or action == "expire" then
                    table.remove(_M.data[name].peers, indexOf(_M.data[name].peers, peer))
                    if 0 == #_M.data[name].peers then
                        _M.data[name] = nil
                    end
                    log("DELETE [".. name .. "]: " .. peer.host .. ":" .. peer.port)
                elseif action == "set" or action == "update" then
                    if not _M.data[name] then
                        _M.data[name] = {version=tonumber(change.etcdIndex), peers={peer}}
                    else
                        local index = indexOf(_M.data[name].peers, peer)
                        if index == nil then
                            log("ADD [" .. name .. "]: " .. peer.host ..":".. peer.port)
                            peer.start_at = ngx_time()
                            table.insert(_M.data[name].peers, peer)
                        else
                            log("MODIFY [" .. name .. "]: " .. peer.host ..":".. peer.port .. " " .. change.node.value)
                            _M.data[name].peers[index] = peer
                        end
                        _M.data[name].version = tonumber(change.node.modifiedIndex)
                    end
                end
            else
                log(err)
            end
        end
        _M.data._version = tonumber(change.node.modifiedIndex)
        nextIndex = _M.data._version + 1
        save()
    end

    ::continue::
    -- Start the update cycle.
    local ok, err = ngx_timer_at(0, watch, nextIndex)
    if not ok then
        log("Error start watch: ", err)
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
            log("Error start watch: " .. err)
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
                    log("Error start watch: " .. err)
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
        log("Error start watch: " .. err)
    end

end

return _M
