local _M = {}
local http = require "resty.http"
local json = require "cjson"

_M.ready = false
_M.data = {}

local function log(c)
  ngx.log(ngx.ERR, c)
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
  return host, port
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
      local file, err = io.open(_M.conf.dump_file, 'w')
      if file == nil then
        log("Can't open file: " .. _M.conf.dump_file .. err)
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
      ngx.sleep(0.2)
    end
  end
end

local function watch(premature, conf, index)
  local c = http:new()
  c:set_timeout(60000)
  c:connect(conf.etcd_host, conf.etcd_port)

  local nextIndex
  local url = "/v2/keys" .. conf.etcd_path

  -- First time to init all the upstreams.
  if index == nil then
    local s_url = url .. "?recursive=true"
    local res, err = c:request({ path = s_url, method = "GET" })
    local body, err = res:read_body()
    if not err then
      local all = json.decode(body)
      if not all.errorCode then
        for n, s in pairs(all.node.nodes) do
          local name = basename(s.key)
          _M.data[name] = { count=0, servers={}}
          local s_url = url .. name .. "?recursive=true"
          local res, err = c:request({path = s_url, method = "GET"})
          local body, err = res:read_body()
          if not err then
            local svc = json.decode(body)
            if not svc.errorCode and svc.node.nodes then
              for i, j in pairs(svc.node.nodes) do
                local b = basename(j.key)
                local h, p = split_addr(b)
                _M.data[name].servers[#_M.data[name].servers+1] = {host=h, port=p}
              end
            end
          end
          _M.data.version = res.headers["x-etcd-index"]
        end
      end
      _M.ready = true
      nextIndex = _M.data.version + 1
      dump_tofile(true)
    end

  -- Watch the change and update the data.
  else
    local s_url = url .. "?wait=true&recursive=true&waitIndex=" .. index
    local res, err = c:request({ path = s_url, method = "GET" })
    local body, err = res:read_body()

    if not err then
      log("watch.2: end, changed"..body)
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
          local h, p = split_addr(bkd)
          local bs = {host=h, port=p}
          local svc = basename(ret)

          if action == "delete" or action == "expire" then
            table.remove(_M.data[svc].servers, indexof(_M.data[svc].servers, bs))
            log("DELETE: "..bs.host..":"..bs.port)
          elseif action == "set" or action == "update" then
            if not _M.data[svc] then
              _M.data[svc] = {count=0, servers={bs}}
            elseif not indexof(_M.data[svc].servers, bs) then
              log("ADD".. bs.host .. bs.port)
              table.insert(_M.data[svc].servers, bs)
            end
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

  -- Start the update cycle.
  log("watch start: "..nextIndex)
  local ok, err = ngx.timer.at(1, watch, conf, nextIndex)
  return
end

function _M.init(conf)
  -- Load the upstreams from file
  if not _M.ready then
    _M.conf = conf
    local file, err = io.open(conf.dump_file, "r")
    if file == nil then
      log(err)
      local ok, err = ngx.timer.at(0, watch, conf, nextIndex)
      return
    else
      local data = json.decode(file:read("*a"))
      _M.data = copyTab(data)
      file:close()
      _M.ready = true
    end
    nextIndex = _M.data.version + 1
  end

  -- Start the etcd watcher
  log("watch start: "..nextIndex)
  local ok, err = ngx.timer.at(0, watch, conf, nextIndex)

end

-- Round robin
function _M.round_robin_server(name)

  if not _M.ready or not _M.data[name] then
    return nil, "upstream not ready."
  end

  _M.data[name].count = _M.data[name].count + 1
  local pick = _M.data[name].count % #_M.data[name].servers
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
