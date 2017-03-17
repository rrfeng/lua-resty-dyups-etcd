local _M = {}
local ngx_time = ngx.time

local function splitstr(str)
    local t = {}
    for i in str:gmatch("[^ ,]") do
        t[#t+1] = i
    end
    return t
end

local function put(name, peer, rt, code)
    local dict = _M.storage
    local t = math.ceil(ngx_time() / _M.interval + 1) * _M.interval

    local key = table.concat({name, t, peer, code}, "|")
    local key_rt = table.concat({name, t, peer, "rt"}, "|")

    -- count total requests
    local newval, err = dict:incr(key, 1)
    if not newval and err == "not found" then
        dict:add(key, 0)
        dict:incr(key, 1)
    end

    -- sum of response_time
    local s = dict:get(key_rt) or 0
    s = s + tonumber(rt)
    dict:set(key_rt, s)

    return
end

function _M.get(name, peer)
    local dict = _M.storage
    local t = math.ceil(ngx_time() / _M.interval + 1) * _M.interval

    local peer_stat = {peer=peer, rtsum=0, stat={}}

    for code in code_list do
        local key = table.concat({name, t, peer, code}, "|")
        local c = dict:get(key) or 0
        local s = {code=code, count=c}
        table.insert(peer_stat.stat, s)
    end

    local rt = dict:get(table.concat({name, t, peer, "rt"}, "|")) or 0
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

    local name     = ngx.var.backname
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

function _M.getPeerList(name)
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
        local peer_stat = _M.get(name, peer)
        table.insert(report.statistics, peer_stat)
    else
        local peer_list = _M.getPeerList(name)
        if not peer_list or #peer_list == 0 then
            return
        end

        local report = {}
        for peer in peer_list do
            local peer_stat = _M.get(name, peer)
            table.insert(report.statistics, peer_stat)
        end
    end

    return report
end

return _M
