local _M = {}
local json = require "cjson"
local ngx_time = ngx.time

_M.storage = nil
_M.upstream = nil
_M.interval = nil
_M.ok = false

local function log(c)
    pcall(ngx_log, ngx_ERR, c)
end

-- from log
_M.code_list = { 200, 202, 204, 301, 302, 304, 400, 401, 403, 404, 405, 408, 409, 413, 499, 500, 502, 503, 504 }

local function splitstr(str)
    local t = {}
    for i in str:gmatch("[^ ,]") do
        t[#t+1] = i
    end
    return t
end

local function put(name, peer, rt, code)
    local ttl = _M.interval * 3
    local dict = _M.storage
    local time_point = math.ceil(ngx_time() / _M.interval + 1) * _M.interval

    local key = table.concat({name, time_point, peer, code}, "|")
    local key_rt = table.concat({name, time_point, peer, "rt"}, "|")

    -- count total requests
    local newval, err = dict:incr(key, 1)
    if not newval and err == "not found" then
        local ok, err = dict:safe_add(key, 0, ttl)
        if not ok then
            log("logger: " .. err)
            return
        end
        dict:incr(key, 1)
    end

    -- sum of response_time
    local cost = tonumber(rt) or tonumber(ngx.var.request_time) or 0
    local s = dict:get(key_rt) or 0
    s = s + cost

    local ok, err = dict:safe_set(key_rt, s, ttl)
    if not ok then
        log("logger: " .. err)
        return
    end

    return
end

local function get(name, peer)
    local dict = _M.storage
    local time_point = math.ceil(ngx_time() / _M.interval + 1) * _M.interval

    local peer_stat = {peer=peer, rtsum=0, stat={}}

    for _, code in pairs(_M.code_list) do
        local key = table.concat({name, time_point, peer, code}, "|")
        local c = dict:get(key)
        if c then
            local s = {code=code, count=c}
            table.insert(peer_stat.stat, s)
        end
    end

    local rt = dict:get(table.concat({name, time_point, peer, "rt"}, "|")) or 0
    peer_stat.rtsum = rt

    return peer_stat
end

function _M.init(shm, interval, upstream_storage)
    if not shm then
        log("logger configuration error")
        _M.ok = false
        return
    end

    if not interval then
        _M.interval = 5
        log("logger configuration missing interval, default 5s")
    else
        _M.interval = tonumber(interval)
    end

    if upstream_storage then
        _M.upstream = upstream_storage
    end

    _M.storage = shm
    _M.ok = true
    return
end

function _M.calc()
    if not _M.ok then
        return
    end

    if not ngx.var.host or ngx.var.host == "_" then
        return
    end

    local upstream = ngx.var.upstream_addr
    if not upstream then
        return
    end

    local name = ngx.var.backname
    if not name or name == "" then
        return
    end

    local status   = ngx.var.upstream_status
    local resptime = ngx.var.upstream_response_time

    local T = tonumber(resptime)
    if T then
        put(name, upstream, T, status)
    else
        status_all    = splitstr(status)
        resptime_all  = splitstr(resptime)
        upstreams_all = splitstr(upstream)
        for i=1,#upstreams_all do
            put(name, upstreams_all[i], resptime_all[i], status_all[i])
        end
    end

    return
end

local function getPeerList(name)
    if not name or not _M.upstream then
        return
    end

    local data = _M.upstream:get(name .. "|peers")
    local ok, peers = pcall(json.decode, data)

    if not ok or type(peers) ~= "table" then
        return
    end

    local result = {}
    for i=1,#peers do
        local peer = table.concat({peers[i].host, peers[i].port}, ":")
        table.insert(result, peer)
    end

    return result
end

function _M.report(name, peer)
    if not name then
        return
    end

    local dict = _M.storage
    local report = {name=name, statistics={}}

    if peer then
        local peer_stat = get(name, peer)
        table.insert(report.statistics, peer_stat)
    else
        local peer_list = getPeerList(name)
        if not peer_list or #peer_list == 0 then
            return
        end

        for _, peer in pairs(peer_list) do
            local peer_stat = get(name, peer)
            table.insert(report.statistics, peer_stat)
        end
    end

    return json.encode(report)
end

return _M
