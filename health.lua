local _M = {}

local log = ngx.log
local ERR = ngx.ERR
local WARN = ngx.WARN
local INFO = ngx.INFO

local new_timer = ngx.timer.at
local ngx_worker_id = ngx.worker.id
local ngx_worker_exiting = ngx.worker.exiting

local http = require "http"
local json = require "cjson"
local logger = require "lreu.logger"

_M.ok_status   = {}
_M.logcheck    = {}
_M.healthcheck = {}

local _default_ok_status = {200, 201, 204, 301, 302, 401, 403}

local function info(...)
    log(INFO, "healthcheck: ", ...)
end

local function warn(...)
    log(WARN, "healthcheck: ", ...)
end

local function errlog(...)
    log(ERR, "healthcheck: ", ...)
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
    local ready = dict:get("ready")
    if not ready then
        return nil
    end

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
        errlog("parse upstream peers error: ", name, " ", data)
        return {}
    end
    return value
end

local function peerFail(name, peer, from)
    local dict = _M.storage
    local key = "checkdown:" .. name .. ":" .. peer

    if from == "healthcheck" then
        local count_key = "count_" .. key
        local newval, err = dict:incr(count_key, 1, 0)
        warn("peer check fail: ", newval, " ", name, " ", peer)
        if err then
            errlog("incr errors fail: ", err)
        else
            if newval < _M.healthcheck.max_fails then
                return
            end

            local ok, err = dict:set(key, true)
            errlog("set peer down: ", newval, " ", name, " ", peer)
            if err then
                errlog("cannot set peer fail: ", err)
            end
        end
    elseif from == "logcheck" then
        local ok, err = dict:set(key, true, _M.logcheck.recover, 1)
        errlog("set peer down by logcheck: ", name, " ", peer)
        if not ok then
            errlog("set peer fail error!", name, peer, err)
        end
    end
end

local function peerOk(name, peer)
    local dict = _M.storage
    local key = "checkdown:" .. name .. ":" .. peer
    local fails = dict:get("count_" .. key)
    info("peer ok: ", name, " ", peer)
    dict:delete("count_" .. key)

    local value, flags = dict:get(key)
    if value and not flags then
        errlog("peer recover: ", fails, " ", name, " ", peer)
        return dict:delete(key)
    end
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
        local peer = st.statistics[i].peer
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

local function logchecker(premature, name)
    if premature or ngx_worker_exiting() then
        return
    end

    local report = genReport(name)
    local failed_peers = checkLog(report)

    if #failed_peers >= 1 then
        warn("peer fail:", json.encode(report))
        for i = 1,#failed_peers do
            peerFail(name, failed_peers[i], "logcheck")
        end
    end
 
    local ok, err = new_timer(_M.logcheck.interval, logchecker, name)
    if not ok then
        errlog("start timer error: ", name, err)
    end
end

local function spawn_logchecker(premature)
    if premature or ngx_worker_exiting() then
        return
    end

    local us = getUpstreamList()
    if not us then
        new_timer(1, spawn_logchecker)
        return
    end

    for _, name in pairs(us) do
        local ok, err = new_timer(_M.logcheck.interval, logchecker, name)
        if ok then
            info("started logchecker: ", name, 
                 ", interval: ", _M.logcheck.interval, 
                 ", recover: ", _M.logcheck.recover
                )
        else
            errlog("start logchecker error: ", name, err)
        end
    end
end

--------------------
--- Health Check ---
--------------------
local function checkPeer(premature, name, fat_peer)
    if premature or ngx_worker_exiting() then
        return
    end

    local client = http:new()
    client:set_timeout(500)
    client:connect(fat_peer.host, fat_peer.port)

    local peer = fat_peer.host .. ":" .. fat_peer.port
    local res, err = client:request({path = peer.check_url, method = "GET", headers = { ["User-Agent"] = "nginx healthcheck" } })
    if not res then
        errlog("check fail: ", err, " ", name, " ", peer)
        peerFail(name, peer, "healthcheck")
    elseif _M.ok_status[res.status] then
        peerOk(name, peer)
    end
    client:close()
end

local function healthchecker(premature, name)
    if premature or ngx_worker_exiting() then
        return
    end

    local fat_peers = getUpstreamPeers(name)

    for i = 1,#fat_peers do
        local ok, err = new_timer(0, checkPeer, name, fat_peers[i])
        if not ok then
            errlog("start timer error: ", name, err)
        end
    end

    local ok, err = new_timer(_M.healthcheck.interval, healthchecker, name)
    if not ok then
        errlog("start timer error: ", name, err)
    end
end

local function spawn_healthchecker(premature)
    if premature or ngx_worker_exiting() then
        return
    end

    local us = getUpstreamList()
    if not us then
        new_timer(1, spawn_healthchecker)
        return
    end

    for _, name in pairs(us) do
        local ok, err = new_timer(_M.healthcheck.interval, healthchecker, name)
        if ok then
            info("started healthchecker: ", name, 
                 ", interval: ", _M.healthcheck.interval, 
                 ", max_fails: ", _M.healthcheck.max_fails
                )
        else
            errlog("start healthchecker error: ", name, err)
        end
    end
end

-- cfg = {
--   storage = ngx.shared.dict.SYNCER_STORAGE,
--   healthcheck = {
--      enable    = true,
--      interval  = 5,
--      max_fails = 3,
--      ok_status = {200, 204, 301, 302}
--   },
--   logcheck = {
--      enable   = false,
--      interval = 5,
--      recover  = 60
--   }
-- }
--

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

    if cfg.healthcheck and cfg.healthcheck.enable then
        if not cfg.healthcheck.interval or cfg.healthcheck.interval < 1 then
            _M.healthcheck.interval = 1
        else
            _M.healthcheck.interval = cfg.healthcheck.interval
        end

        if not cfg.healthcheck.max_fails or cfg.healthcheck.max_fails < 1 then
            _M.healthcheck.max_fails = 1
        else
            _M.healthcheck.max_fails = cfg.healthcheck.max_fails
        end

        if type(cfg.healthcheck.ok_status) ~= "table" then
            for _, status_code in pairs(_default_ok_status) do
                _M.ok_status[status_code] = true
            end
        else
            for _, status_code in pairs(cfg.healthcheck.ok_status) do
                _M.ok_status[status_code] = true
            end
        end

        -- start healthchecker
        new_timer(0, spawn_healthchecker)
    end

    if cfg.logcheck and cfg.logcheck.enable and logger.enable then
        if not cfg.logcheck.interval or cfg.logcheck.interval < 5 then
            _M.logcheck.interval = 5
        else
            _M.logcheck.interval = cfg.logcheck.interval
        end

        if not cfg.logcheck.recover or cfg.logcheck.recover < 2*_M.logcheck.interval then
            _M.logcheck.recover = 2*_M.logcheck.interval
        else
            _M.logcheck.recover = cfg.logcheck.recover
        end

        -- start logchecker
        new_timer(0, spawn_logchecker)
            end
end

return _M
