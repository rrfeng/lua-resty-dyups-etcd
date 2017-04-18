local _M = {}

local log = ngx.log
local ERR = ngx.ERR
local WARN = ngx.WARN
local INFO = ngx.INFO
local new_timer = ngx.timer.at
local ngx_worker_id = ngx.worker.id
local ngx_worker_exiting = ngx.worker.exiting

local json = require "cjson"
local logger = require "lreu.logger"

local function info(...)
    log(INFO, "judger: ", ...)
end

local function warn(...)
    log(WARN, "judger: ", ...)
end

local function errlog(...)
    log(ERR, "judger: ", ...)
end

local function splitstr(str)
    local t = {}
    for i in str:gmatch("[^|]+") do
        t[#t+1] = i
    end
    return t
end

local function getUpstreamList()
    local dict = _M.storage
    local allname = dict:get("_allname")

    if not allname then
        warn("get nil upstream list")
        return nil
    end

    return splitstr(allname)
end

local function getUpstreamPeers(name)
    local dict = _M.storage
    local data = dict:get(name .. "|peers")
    local ok, value = pcall(json.decode, data)
    if not ok or type(value) ~= "table" then
        errlog("get upstream peers error: malformed data")
        return {}
    end
    return value
end

local function getPeerList()
    local peers = {}
    local us = getUpstreamList()
    for i = 1,#us do
        local ps = getUpstreamPeers(us[i])
        for j = 1,#ps do
            ps[i].name = us[i]
            peers[#peers+1] = ps[i]
        end
    end
    return peers
end

local function peerFail(name, peer)
    local dict = _M.storage
    local key = "checkdown:" .. name .. ":" .. peer
    local ok, err = dict:set(key, true, _M.recover_after)
    if not ok then
        errlog("set peer fail error!", name, peer, err)
    else
        info("set peer fail.", name, peer)
    end
    return
end

-----------------
--- Log Check ---
-----------------
local function genReport(name)
    local report = {}
    local st = logger.report(name, nil, 5)
    if not st then
        return report
    end

    for i = 1,#st.statistics do
        local peer = st.statistics[i]
        local total, errors = 0, 0
        for j = 1,#st.statistics[i].stat do
            total = total + st.statistics[i].stat[j].count
            if st.statistics[i].stat[j].code >= 400 then
                errors = errors + st.statistics[i].stat[j].count
            end
        end

        -- if a peer never process a request, may down.
        if total > 0 then
            err_rate = errors / total
            report[peer] = err_rate
        end
    end
    return report
end

local function checkLog(report)
    local fp = {}

    -- if only 1 peer in the upstream, do not fail it.
    if #report <= 1 then
        return {}
    end

    -- a simple rule, fail_rate > 50 and not all peer fail
    local total, avg
    for peer, rate in pairs(report) do
        if rate > 0.5 then
            fp[#fp+1] = peer
        end
    end

    if 2*#fp > #report then
        return {}
    end

    return fp
end

local function logcheck(premature, name)
    if premature or ngx_worker_exiting() then
        return
    end

    local report = genReport(name)
    local failed_peers = checkLog(report)

    if #failed_peers >= 1 then
        info("peer fail:", json.encode(report))
        for i = 1,#failed_peers do
            peerFail(name, failed_peers[i])
        end
    end
 
    local ok, err = new_timer(_M.check_interval, logcheck, name)
    if not ok then
        errlog("start time error: ", name, err)
    end
end

--------------------
--- Health Check ---
--------------------
local function checkHttp(url)
end

local function healthcheck(premature, name)
    if premature or ngx_worker_exiting() then
        return
    end

    local peers = getUpstreamPeers(name)
    for i = 1,#peers do
    end
end

function _M.init(cfg)
    if ngx_worker_id() ~= 0 then
        return
    end

    -- use same storage with syncer
    -- 1. get all upstream name
    -- 2. put failed peers for picker
    if not cfg or not cfg.storage then
        errlog("configuration errors, no storage provide.")
        return
    else
        _M.storage = cfg.storage
    end

    if cfg.check_interval and cfg.check_interval >= 1 then
        _M.check_interval = cfg.check_interval
    else
        _M.check_interval = 5
    end

    -- default recover time is 60 seconds
    -- min recover time is 2*check_interval
    _M.recover_after = cfg.recover_after or 60
    if cfg.recover_after <= 2*_M.check_interval then
        _M.recover_after = 2*_M.check_interval
    end

    local us = getUpstreamList()

    -- always run healthcheck
--    for _, name in pairs(us) do
--        local ok, err = new_timer(_M.check_interval, healthcheck, name)
--        if ok then
--            info("started: ", name, 
--                 ",with interval: ", _M.check_interval, 
--                 ",recover_after: ", _M.recover_after
--                )
--        else
--            errlog("start time error: ", name, err)
--        end
--    end

    -- only run logcheck when logger enabled.
    if logger.enable then
        for _, name in pairs(us) do
            local ok, err = new_timer(_M.check_interval, logcheck, name)
            if ok then
                info("started: ", name, 
                     ",with interval: ", _M.check_interval, 
                     ",recover_after: ", _M.recover_after
                    )
            else
                errlog("start time error: ", name, err)
            end
        end
    end
end

return _M
