# Consul Ksonnet 

A set of extensible configs for running Consul on Kubernetes.

Usage:
- Make sure you have the [ksonnet v0.8.0](https://github.com/ksonnet/ksonnet).

```
$ brew install https://raw.githubusercontent.com/ksonnet/homebrew-tap/82ef24cb7b454d1857db40e38671426c18cd8820/ks.rb
$ brew pin ks
$ ks version
ksonnet version: v0.8.0
jsonnet version: v0.9.5
client-go version: v1.6.8-beta.0+$Format:%h$
```

- In your config repo, if you don't have a ksonnet application, make a new one (will copy credentials from current context):

```
$ ks init <application name>
$ cd <application name>
```

- Vendor this package using [jsonnet-bundler](https://github.com/jsonnet-bundler/jsonnet-bundler)

```
$ go get github.com/jsonnet-bundler/jsonnet-bundler/cmd/jb
$ jb install https://github.com/grafana/jsonnet-libs/consul
```

- Assuming you want to run in the default namespace ('environment' in ksonnet parlance), add the following to the file `environments/default/main.jsonnet`:

```
local consul = import "consul/consul.libsonnet";

consul + {
  _config+:: {
    consul_replicas: 1,
  }
}
```

- Apply your config:

```
$ ks apply default
```
# Customising and Extending.

The choice of Ksonnet for configuring these jobs was intention; it allows users
to easily override setting in these configurations to suit their needs, without having
to fork or modify this library.  For instance, to override the resource requests
and limits for the Consul container, you would:

```
local consul = import "consul/consul.libsonnet";

consul {
  consul_container+::
     $.util.resourcesRequests("1", "2Gi") +
     $.util.resourcesLimits("2", "4Gi"),
}
```

We sometimes specify config options in a `_config` dict; there are two situations
under which we do this:

- When you must provide a value for the parameter (such as `namesapce`).
- When the parameter get referenced in multiple places, and overriding it using
  the technique above would be cumbersome and error prone (such as with `cluster_dns_suffix`).

We use these two guidelines for when to put parameters in `_config` as otherwise
the config field would just become the same as the jobs it declares - and lets
not forget, this whole thing is config - so its completely acceptable to override
pretty much any of it.
