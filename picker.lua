local _M = {}
local json = require "cjson"

local ngx_timer_at = ngx.timer.at
local ngx_log = ngx.log
local ngx_ERR = ngx.ERR
local ngx_time = ngx.time

_M.ready = false
_M.black_hole = {ip="127.0.0.1", port=2222, weight=0}
_M.data = {}

local function log(c)
    ngx_log(ngx_ERR, c)
end

local function indexOf(t, e)
    for i=1,#t do
        if t[i].host == e.host and t[i].port == e.port then
            return i
        end
    end
    return nil
end

function _M.init(shm)
    _M.storage = shm
end

local function slowStart(premature, name, peer, t)
    if premature then return end

    if t < 1 then t = 1 end

    local peers = _M.data[name].peers
    -- we must confirm the index every time
    -- if a peer disappear, index will change
    local i =  indexOf(peers, peer)
    if not i then
        return
    end

    local times = peers[i].slow_start

    if t > times then
        return
    end

    if t == times then
        peers[i].cfg_weight = peer.weight
        return
    end

    peers[i].cfg_weight = peer.weight * t / times

    local ok, err = ngx_timer_at(1, slowStart, name, peer, t+1)
    if not ok then
        log("Error start slowStart: " .. err)
        peers[i].cfg_weight = peer.weight
    end
end

local function update(name)
    -- if the etcd version is same, no update
    local ver = _M.storage:get(name .. "|version")
    if _M.data[name] and _M.data[name].version == ver then
        return nil
    end

    local ver  = _M.storage:get(name .. "|version")
    local data = _M.storage:get(name .. "|peers")

    if not ver and not data then
        _M.data[name] = nil
    end

    local ok, value = pcall(json.decode, data)

    if not ok or type(value) ~= "table" then
        return "data format error"
    end

    if not _M.data[name] then
        _M.data[name] = {}
    end

    _M.data[name].peers = value
    _M.data[name].version = ver

    -- Check if there is a new peer that needs slow start.
    local peers = _M.data[name].peers
    if #peers <= 1 then
        return
    end

    local now = ngx_time()
    for i=1,#peers do
        if peers[i].slow_start > 0 then
            if peers[i].start_at and now - peers[i].start_at < 5 then
                local ok, err = ngx_timer_at(0, slowStart, name, peers[i], 1)
                if not ok then
                    log("Error start slowStart: " .. err)
                end
            end
        end
    end
    return nil
end

local function ischeckdown(name, host, port)
    return _M.storage:get("checkdown:" .. name .. ":" .. host .. ":" .. port)
end

function _M.rr(name)
    -- before pick check update
    update(name)

    if not _M.data[name] then
        return nil
    end

    -- start to pick a peer
    local peers = _M.data[name].peers
    local total = 0
    local pick = nil

    for i=1,#peers do

        -- If no weight set, the default is 1.
        if peers[i].cfg_weight == nil then
            peers[i].cfg_weight = peers[i].weight or 1
        end

        if peers[i].run_weight == nil then
            peers[i].run_weight = 0
        end

        if peers[i].cfg_weight == 0 then
            goto continue
        end

        if peers[i].status == "down" then
            goto continue
        end

        if peers[i].checkdown == true then
            if ischeckdown(name, peers[i].host, peers[i].port) then
                goto continue
            else
                peers[i].checkdown = false
            end
        end

        peers[i].run_weight = peers[i].run_weight + peers[i].cfg_weight
        total = total + peers[i].cfg_weight

        if pick == nil or pick.run_weight < peers[i].run_weight then
            pick = peers[i]
        end

        ::continue::
    end

    -- if all peers cfg_weight is 0, then reset.
    if not pick and total == 0 then
        for i=1,#peers do
            peers[i].cfg_weight = peers[i].weight or 1
        end
        pick = peers[1]
    end

    if pick then
        pick.run_weight = pick.run_weight - total
    end

    -- for health check
    pick.checkdown = ischeckdown(name, pick.host, pick.port)

    return pick
end

function _M.show(name)
    if not name then
        return "{}"
    end

    return json.encode(_M.data[name])
end

function _M.cutdown(name, peer, percent)
    local i = indexOf(_M.data[name], peer)
    if not i then
        return 'peer not exists'
    end
    _M.data[name].peers[i].cfg_weight = _M.data[name].peers[i].cfg_weight * percent
    return nil
end

function _M.recover(name, peer)
    local i = indexOf(_M.data[name], peer)
    if not i then
        return 'peer not exists'
    end
    _M.data[name].peers[i].cfg_weight = _M.data[name].peers[i].weight
    return nil
end

function _M.setBlackHole(name, percent)
    if percent >= 1 or percent < 0 then
        return "not permitted"
    end

    local peers = _M.data[name].peers
    local index = indexOf(peers, _M.black_hole)

    if percent == 0 then
        if index then
            table.remove(_M.data[name].peers, index)
            return nil
        else
            return nil
        end
    else
        local total_weight = 0
        for i=1,#peers do
            total_weight = total_weight + peers[i].cfg_weight
        end
        local black_weight = total_weight / percent - total_weight

        if index then
            _M.data[name].peers[index].cfg_weight = black_weight
            return nil
        else
            local black_peer = _M.black_hole
            black_peer.cfg_weight = black_weight
            table.insert(_M.data[name].peers, black_peer)
            return nil
        end
    end
end

return _M
