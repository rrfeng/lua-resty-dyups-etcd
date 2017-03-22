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
    local ttl = _M.max_keep_time
    local dict = _M.storage
    local time_point = ngx_time()

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

    -- total counter of nginx
    local counter_key = "total|" .. time_point
    local new, err = dict:incr(counter_key, 1)
    if not new and err == "not found" then
        local ok, err = dict:safe_add(counter_key, 0, ttl)
        if not ok then
            log("logger: " .. err)
            return
        end
        dict:incr(counter_key, 1)
    end

    return
end

local function get(name, peer, t_start, t_end)
    local dict = _M.storage
    local peer_stat = {peer=peer, rtsum=0, stat={}}

    local rtsum = 0
    for _, code in pairs(_M.code_list) do
        local count = 0
        for ts = t_start,t_end do
            local key = table.concat({name, ts, peer, code}, "|")
            local c = dict:get(key) or 0
            count = count + c

            local rt_key = table.concat({name, ts, peer, "rt"}, "|")
            local rt = dict:get(rt_key) or 0
            rtsum = rtsum + rt
        end

        if count > 0 then
            local s = {code=code, count=count}
            table.insert(peer_stat.stat, s)
        end
    end

    peer_stat.rtsum = rtsum

    return peer_stat
end

function _M.init(shm, max_keep_time, upstream_storage)
    if not shm then
        log("logger configuration error")
        _M.ok = false
        return
    end

    if not max_keep_time then
        _M.max_keep_time = 60
        log("logger configuration missing max_keep_time, default 60s")
    else
        _M.max_keep_time = tonumber(max_keep_time)
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
        name = "_"
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

function _M.report(name, peer, offset)
    if not name then
        return
    end

    offset = tonumber(offset)
    if not offset then
        offset = 60
    elseif offset > _M.max_keep_time then
        offset = _M.max_keep_time
    end

    local dict = _M.storage
    local t_end = ngx_time() - 1
    local t_start = t_end - offset + 1
    local report = {name=name, ts_start=t_start, ts_end=t_end, statistics={}}

    if peer then
        local peer_stat = get(name, peer, t_start, t_end)
        table.insert(report.statistics, peer_stat)
    else
        local peer_list = getPeerList(name)
        if not peer_list or #peer_list == 0 then
            return
        end

        for _, peer in pairs(peer_list) do
            local peer_stat = get(name, peer, t_start, t_end)
            table.insert(report.statistics, peer_stat)
        end
    end

    return json.encode(report)
end

function _M.tps(offset)
    local dict = _M.storage

    offset = tonumber(offset)
    if not offset then
        offset = 60
    elseif offset > _M.max_keep_time then
        offset = _M.max_keep_time
    end

    local t_end = ngx_time() - 1
    local t_start = t_end - offset + 1

    local total = 0
    for ts = t_start,t_end do
        local count = dict:get("total|" .. ts) or 0
        total = total + count
    end
    return total / offset
end

return _M
