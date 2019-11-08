# lua-resty-upstream-etcd

A lua module for OpenResty, can dynamically update the upstreams from etcd and kubernetes.

- Upstream realtime change from etcd or kubernetes apiserver, without reload.
- Weighted round robin load balancing (not for kubernetes because of no `weight` settings of pod).
- Healthcheck
- Request statistics

## REQUIREMENTS

- openresty-1.9.11.1 and newer
- balancer_by_lua
- ngx.worker.id
- lua-resty-http
- cjson

## USAGE

### Prepare Etcd:
```
etcdctl set /v1/testing/services/${service_name}/${host}:${port} ${value}
```

The `service_name` is upstream's name, should be string.
The `host` is upstream's ip address, domain name not supported.
The `port` is upstream's port.
The `value` should be a json, if json parse error, will use the default value below:
```
{
    "weight": 100,
    "check_url": "/health",
    "slow_start": 30,
    "status": "up"
}
```

- weight: upstream peer weight as number, int or float.
- check_url: for healthcheck, if enabled, checker will do http request to `http://${host}:${port}${check_url}`.
- slow_start: number of seconds duration that the upstream peer's weight slowly increased from `0` to `${weight}`
- status: indicates the peer is up or down, must be `up` if you want the peer work, any other value means peer down.

### Init the module:
```
lua_socket_log_errors off;   # recommend
lua_package_path "/path/of/your/environment...";

lua_shared_dict lreu_shm 1m;     # for global storage
lua_shared_dict lreu_shm_k8s 1m; # for global storage

init_worker_by_lua_block {
    local syncer = require "lreu.syncer"
    syncer.init({
        etcd_host = "127.0.0.1",
        etcd_port = 2379,
        etcd_path = "/v1/testing/services/",
        storage = ngx.shared.lreu_shm
    })

    -- if you want to use k8s
    local syncer_k8s = require "lreu.syncer_k8s"
    sycner_k8s.init({
        apiserver_host = "127.0.0.1",
        apiserver_port = "6443",
        namespace = "default",
        token = "the token",
        storage = ngx.shared.lreu_shm_k8s
    })

    -- init the picker before using it in balancer_by_lua
    local picker = require "lreu.picker"

    -- you can use both etcd and k8s
    picker.init(ngx.shared.lreu_shm, ngx.shared.lreu_shm_k8s, true, ngx.shared.hc)

    -- if you want to use health check
    local health = require "lreu.health"
    health.init({
        storage = ngx.shared.lreu_upstream,
        healthcheck = {
            enable = true,
            timeout = 3,
            interval = 5,
            max_fails = 3,
            ok_status = {200, 204, 301, 302, 401, 403, 404}
        },
        logcheck = {
            enable = true,
            interval = 5,
            recover = 60
        }
    })
}
```

### Use picker in upstream:
```
upstream test {
    server 127.0.0.1:2222; # fake server

    balancer_by_lua_block {
        local balancer = require "ngx.balancer"
        local picker = require "lreu.picker"
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

### Functions
#### picker.init(first_shm, second_shm?, merge?, hcshm?)
`first_shm` and `second_shm` should be the storage of syncer. One or two.
If the `second_shm` provided, both used. but the `merge` controls the behavior:
- `merge`==`true`: all peers in both shm merged together for load balancing.
- `merge`==`false`: picker try to find peers in the `first_shm`, if found, use it; if no peers in `first_shm`, picker try the `second_shm`.

`hcshm` is the storeage of `health.lua` checker, if not set, will use the `first_shm`.

## Todo

- More docs to be added.
- Etcd cluster support.
- Add more load-balance-alg.