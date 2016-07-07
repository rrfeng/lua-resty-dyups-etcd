local _M = {}
local http = require "http"
local json = require "cjson"

local ngx_timer_at = ngx.timer.at
local ngx_log = ngx.log
local ngx_ERR = ngx.ERR
local ngx_sleep = ngx.sleep
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

local function indexof(t, e)
    for k, v in pairs(t) do
        if v.host == e.host and v.port == e.port then
            return k
        end
    end
    return nil
end

local function basename(s)
    local x, y = s:match("(.*)/([^/]*)/?")
    return y, x
end

local function split_addr(s)
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

local function get_lock()
    local dict = _M.conf.dict
    local key = "lock"
    -- only the worker who get the lock can update the dump file.
    local ok, err = dict:add(key, true)
    if not ok then
        if err == "exists" then
            return nil
        end
        log("failed to add key \"", key, "\": ", err)
        return nil
    end
    return true
end

local function release_lock()
    local dict = _M.conf.dict
    local key = "lock"
    local ok, err = dict:delete(key)
    return true
end

local function dump_tofile(force)
    local cur_v = _M.data.version
    local saved = false
    local dict = _M.conf.dict
    while not saved do
        local pre_v = dict:get("version")
        if not force then
            if pre_v then
                if tonumber(pre_v) >= tonumber(cur_v) then
                    return true
                end
            end
        end

        local l = get_lock()
        if l then
            local f_path = _M.conf.dump_file .. _M.conf.etcd_path:gsub("/", "_")
            local file, err = io.open(f_path, 'w')
            if file == nil then
                log("Can't open file: " .. f_path .. err)
                release_lock()
                return false
            end

            local data = json.encode(_M.data)
            file:write(data)
            file:flush()
            file:close()

            dict:set("version", cur_v)
            saved = true
            release_lock()
        else
            ngx_sleep(0.2)
        end
    end
end

local function watch(premature, conf, index)
    if premature then
        return
    end

    if ngx_worker_exiting() then
        return
    end

    local c = http:new()
    c:set_timeout(120000)
    c:connect(conf.etcd_host, conf.etcd_port)

    local nextIndex
    local url = "/v2/keys" .. conf.etcd_path

    -- First time to init all the upstreams.
    if index == nil then
        local s_url = url .. "?recursive=true"
        local res, err = c:request({ path = s_url, method = "GET" })
        if not err then
            local body, err = res:read_body()
            if not err then
                local all = json.decode(body)
                if not all.errorCode and all.node.nodes then
                    for n, s in pairs(all.node.nodes) do
                        local name = basename(s.key)
                        _M.data[name] = { count=0, servers={}}
                        local s_url = url .. name .. "?recursive=true"
                        local res, err = c:request({path = s_url, method = "GET"})
                        if not err then
                            local body, err = res:read_body()
                            if not err then
                                local svc = json.decode(body)
                                if not svc.errorCode and svc.node.nodes then
                                    for i, j in pairs(svc.node.nodes) do
                                        local b = basename(j.key)
                                        local h, p, err = split_addr(b)
                                        if not err then
                                            _M.data[name].servers[#_M.data[name].servers+1] = {host=h, port=p}
                                        end
                                    end
                                end
                            end
                            _M.data.version = res.headers["x-etcd-index"]
                        end
                    end
                end
                _M.ready = true
                if _M.data.version then
                    nextIndex = _M.data.version + 1
                end
                dump_tofile(true)
            end
        end

    -- Watch the change and update the data.
    else
        local s_url = url .. "?wait=true&recursive=true&waitIndex=" .. index
        local res, err = c:request({ path = s_url, method = "GET" })
        if not err then
            local body, err = res:read_body()
            if not err then
                -- log("DEBUG: recieve change: "..body)
                local change = json.decode(body)

                if not change.errorCode then
                    local action = change.action
                    if change.node.dir then
                        local target = change.node.key:match(_M.conf.etcd_path .. '(.*)/?')
                        if action == "delete" then
                            _M.data[target] = nil
                        elseif action == "set" or action == "update" then
                            local new_svc = target:match('([^/]*).*')
                            if not _M.data[new_svc] then
                                _M.data[new_svc] = {count=0, servers={}}
                            end
                        end
                    else
                        local bkd, ret = basename(change.node.key)
                        local h, p, err = split_addr(bkd)
                        if not err then
                            local bs = {host=h, port=p}
                            local svc = basename(ret)

                            if action == "delete" or action == "expire" then
                                table.remove(_M.data[svc].servers, indexof(_M.data[svc].servers, bs))
                                log("DELETE [".. svc .. "]: " .. bs.host .. ":" .. bs.port)
                            elseif action == "set" or action == "update" then
                                if not _M.data[svc] then
                                    _M.data[svc] = {count=0, servers={bs}}
                                elseif not indexof(_M.data[svc].servers, bs) then
                                    log("ADD [" .. svc .. "]: " .. bs.host ..":".. bs.port)
                                    table.insert(_M.data[svc].servers, bs)
                                end
                            end
                        else
                            log(err)
                        end
                    end
                    _M.data.version = change.node.modifiedIndex
                    nextIndex = _M.data.version + 1
                elseif change.errorCode == 401 then
                    nextIndex = nil
                end
            elseif err == "timeout" then
                nextIndex = res.headers["x-etcd-index"] + 1
            end
            dump_tofile(false)
        end
    end
    c:close()

    -- Start the update cycle.
    local ok, err = ngx_timer_at(0, watch, conf, nextIndex)
    return
end

function _M.init(conf)
    -- Load the upstreams from file
    if not _M.ready then
        _M.conf = conf
        local f_path = _M.conf.dump_file .. _M.conf.etcd_path:gsub("/", "_")
        local file, err = io.open(f_path, "r")
        if file == nil then
            log(err)
            local ok, err = ngx_timer_at(0, watch, conf, nextIndex)
            return
        else
            local d = file:read("*a")
            local data = json.decode(d)
            if err then
                log(err)
                local ok, err = ngx_timer_at(0, watch, conf, nextIndex)
                return
            else
                _M.data = copyTab(data)
                file:close()
                _M.ready = true
                if _M.data.version then
                    nextIndex = _M.data.version + 1
                end
            end
        end
    end

    -- Start the etcd watcher
    local ok, err = ngx_timer_at(0, watch, conf, nextIndex)

end

-- Round robin
function _M.round_robin_server(name)

    if not _M.ready or not _M.data[name] then
        return nil, "upstream not ready."
    end

    local c = _M.conf.dict
    local c_key = name .. "_count"
    local count, err = c:incr(c_key, 1)
    if err == "not found" then
        count = 1
        ok = c:set(c_key, 1)
        if not ok then
            _M.data[name].count = _M.data[name].count + 1
            count = _M.data[name].count
        end
    end
    local pick = count % #_M.data[name].servers
    return _M.data[name].servers[pick + 1]
end

function _M.all_servers(name)
    if _M.data[name] then
        return _M.data[name].servers
    else
        return nil
    end
end

return _M
