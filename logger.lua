local _M = {}

local function splitstr(str)
    local t = {}
    for i in str:gmatch("[^ ,]") do
        t[#t+1] = i
    end
    return t
end

local function put(name, peer, time, httpcode)
    local dict = _M.storage
    local status
    if not httpcode then
        status = "fail"
    else
        local code = tonumber(httpcode)
        if code < 500 then
            status = "success"
        elseif code == 502 then
            status = "fatal"
        else
            status = "fail"
        end
    end

    local key = name .. "|" .. peer

    -- we store the peer list in a key:
    -- there is a bug here, may store a peer twice
    if dict:add(key, true) then
        local peer_list = dict:get(name .. "_list")
        if not peer_list then
            dict:set(name .. "_list", peer)
        else
            dict:set(name .. "_list", peer_list .. "|" .. peer)
        end
    end

    -- count total requests
    local newval, err = dict:incr(key .. "|total", 1)
    if not newval and err == "not found" then
        dict:add(key .. "|total", 0)
        dict:incr(key .. "|total", 1)
    end

    if status == "success" then
        -- sum of response_time, only success
        local key = name .. "|" .. peer
        local s = dict:get(key .. "|rtsum") or 0
        s = s + tonumber(time)
        dict:set(key .. "|rtsum", s)
    else
        local newval, err = dict:incr(key .. "|" .. status, 1)
        if not newval and err == "not found" then
            dict:add(key .. "|" .. status, 0)
            dict:incr(key .. "|" .. status, 1)
        end
    end

    return
end

function _M.init(shm)
    _M.storage = shm
    return
end

function _M.calc()
    if not ngx.var.host or ngx.var.host == "_" then
        return
    end

    if _M.storage == nil then
        ngx.log(ngx.ERR, "no storage configured ")
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

function _M.report(name)
    local dict = _M.storage
    local peer_list = dict:get(name .. "_list")
    if peer_list == nil then
        return nil
    end

    local statistics = {}
    for peer in peer_list:gmatch('[^|]+') do
        local fail    = dict:get(name .. "|" .. peer .. "|fail")  or 0
        local fatal   = dict:get(name .. "|" .. peer .. "|fatal") or 0
        local total   = dict:get(name .. "|" .. peer .. "|total") or 0
        local rtsum   = dict:get(name .. "|" .. peer .. "|rtsum") or 0
        table.insert(statistics, {peer=peer, total=total, fatal=fatal, fail=fail, rtsum=rtsum})
    end
    return statistics
end

return _M
