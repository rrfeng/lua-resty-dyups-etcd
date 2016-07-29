# lua-resty-upstream-etcd
```
!!!This module is under heavy development, do not use in  production environment.!!!

A lua module for OpenResty, can dynamically update the upstreams from etcd.
```

## DEPENDENCE
- openresty-1.9.7.3 + (balancer_by_lua*)
- lua-resty-http
- cjson

## USAGE

### Prepare data in etcd:
```
etcdctl set /v1/testing/services/my_test_service/10.1.1.1:8080 '{"weight": 3}'
etcdctl set /v1/testing/services/my_test_service/10.1.1.2:8080 '{"weight": 4}'
etcdctl set /v1/testing/services/my_test_service/10.1.1.3:8080 '{"weight": 5}'

Value should be a json, now round_robin_with_weight() support the weight for load balancing, works like nginx round-robin.
The default weight is 1, if not set in ETCD, or json parse error and so on.
```

### Init the module:
```
lua_socket_log_errors off; # recommend
lua_shared_dict dyups 10k; # for global lock and version
init_worker_by_lua_block {
    local u = require "dyups"
    u.init({
        etcd_host = "127.0.0.1",
        etcd_port = 2379,
        etcd_path = "/v1/testing/services/",
-- The real path of the dump file will be: /tmp/nginx-upstreams_v1_testing_services_
        dump_file = "/tmp/nginx-upstreams",
        dict = ngx.shared.dyups
    })
}
```
### Get a server in upstream:
```
upstream test {
    server 127.0.0.1:2222; # fake server

    balancer_by_lua_block {
        local balancer = require "ngx.balancer"
        local u = require "dyups"
        local s, err = u.round_robin_server("my_test_service")
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
### dyups.round_robin_server(service_name)
```
Get a backend server from the server list in a upstream, and using round-robin algorithm.
return a table: 
{
  host = "127.0.0.1",
  port = 1234
}
```
### dyups.all_servers(service_name)
```
Get all backend servers in a upstream.
return a table:
{
  {host= "127.0.0.1", port = 1234},
  {host= "127.0.0.2", port = 1234},
  {host= "127.0.0.3", port = 1234}
}

So you can realize your own balance algorithms.
```

## dyups.round_robin_with_weight(service_name)
```
Like round_robin_server, you should use this function instead of round_robin_serverZ().
This support peers weight.
```

## Todo
--- Etcd cluster support.
--- Add more load-balance-alg.
--- ~~Upstream peers weight support.~~
--- Upstream health check support.

## License
```
I have not thought about it yet.
```
