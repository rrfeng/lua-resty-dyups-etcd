# lua-resty-upstream-etcd
```
!!!This module is under heavy development, do not use in  production environment.!!!

A lua module for OpenResty, can dynamically update the upstreams from etcd.
```

## DEPENDENCE
- openresty-1.9.11.1 and higher
- balancer_by_lua
- ngx.worker.id()
- lua-resty-http
- cjson

## SYNOPSIS
- syncer: fetch from etcd and watch etcd changes, save upstreams in shared.dict.syncer
- picker: sync upstreams from shared.dict.syncer(which syncer writes), run balancer alg to select upstream. REQUIRE: syncer
- logger: record the upstream response status and time cost in shared.dict.logger, and generate reports. OPTIONAL REQUIRE: syncer
- judger: TODO
- health: TODO

## USAGE

### Prepare data in etcd:
```
etcdctl set /v1/testing/services/my_test_service/10.1.1.1:8080 '{"weight": 3, "slow_start": 30, "checkurl": "/health"}'
etcdctl set /v1/testing/services/my_test_service/10.1.1.2:8080 '{"weight": 4, "status": "down"}'
etcdctl set /v1/testing/services/my_test_service/10.1.1.3:8080 '{"weight": 5}'

The default weight is 1, if not set in ETCD, or json parse error and so on.
```

### Init the module:
```
lua_socket_log_errors off; # recommend
lua_shared_dict lreu-upstream 1m; # for storeage of upstreams
init_worker_by_lua_block {
    local syncer = require "lreu.syncer"
    syncer.init({
        etcd_host = "127.0.0.1",
        etcd_port = 2379,
        etcd_path = "/v1/testing/services/",
        storage = ngx.shared.lreu-upstream
    })

    -- init the picker with the shared storage(read only)
    local picker = require "lreu.picker"
    picker.init(ngx.shared.lreu-upstream)
}
```
### Get a server in upstream:
```
upstream test {
    server 127.0.0.1:2222; # fake server

    balancer_by_lua_block {
        local balancer = require "ngx.balancer"
        local u = require "lreu.picker"
        local s, err = u.rr("my_test_service")
        if not s then
            ngx.log(ngx.ERR, err)
            return ngx.exit(500)
        end
        local ok, err = balancer.set_current_peer(s.host, s.port)
        if not ok then
            ngx.log(ngx.ERR, "failed to set current peer: " .. err)
            return ngx.exit(500)
        end
    }
}
```

## Functions
### picker.rr(service_name)
### picker.show(service_name)

## Todo
- Etcd cluster support.
- Add more load-balance-alg.
- ~~Upstream peers weight support.~~
- Upstream health check support.

## License
```
I have not thought about it yet.
```
