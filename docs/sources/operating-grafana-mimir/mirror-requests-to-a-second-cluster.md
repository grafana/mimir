---
title: "Mirror requests to a second cluster"
description: ""
weight: 10
---

# Mirror requests to a second cluster

Requests mirroring can be used when you need to setup a testing Grafana Mimir cluster receiving the same series ingested by a primary one without having control over Prometheus remote write config (if you do, then configuring two remote write entries in Prometheus would be the preferred option).

## Mirroring with Envoy proxy

[Envoy proxy](https://www.envoyproxy.io/) can be used to mirror HTTP requests to a secondary upstream cluster. From a network path perspective, you should run Envoy in front of both clusters' distributors, letting Envoy to proxy requests to the primary Grafana Mimir cluster and mirror them to a secondary cluster in background. The performances and availability of the secondary cluster have no impact on the requests to the primary one. The response to the client will always be the one from the primary one. In this sense, the requests from Envoy to the secondary cluster are "fire and forget".

### Example Envoy config

The following Envoy configuration shows an example with two Grafana Mimir clusters. Envoy will listen on port `9900` and will proxies all requests to `mimir-primary:8080`, mirroring it to `mimir-secondary:8080` too.

<!-- prettier-ignore-start -->
[embedmd]:# (../../configurations/requests-mirroring-envoy.yaml)
```yaml
admin:
  # No access logs.
  access_log_path: /dev/null
  address:
    socket_address: { address: 0.0.0.0, port_value: 9901 }

static_resources:
  listeners:
    - name: mimir_listener
      address:
        socket_address: { address: 0.0.0.0, port_value: 9900 }
      filter_chains:
        - filters:
            - name: envoy.http_connection_manager
              config:
                stat_prefix: mimir_ingress
                route_config:
                  name: all_routes
                  virtual_hosts:
                    - name: all_hosts
                      domains: ["*"]
                      routes:
                        - match: { prefix: "/" }
                          route:
                            cluster: mimir_primary

                            # Specifies the upstream timeout. This spans between the point at which the entire downstream
                            # request has been processed and when the upstream response has been completely processed.
                            timeout: 15s

                            # Specifies the cluster that requests will be mirrored to. The performances and availability of
                            # the secondary cluster have no impact on the requests to the primary one. The response to the
                            # client will always be the one from the primary one. The requests from Envoy to the secondary
                            # cluster are "fire and forget".
                            request_mirror_policies:
                              - cluster: mimir_secondary
                http_filters:
                  - name: envoy.router
  clusters:
    - name: mimir_primary
      type: STRICT_DNS
      connect_timeout: 1s
      hosts: [{ socket_address: { address: mimir-primary, port_value: 8080 }}]
      dns_refresh_rate: 5s
    - name: mimir_secondary
      type: STRICT_DNS
      connect_timeout: 1s
      hosts: [{ socket_address: { address: mimir-secondary, port_value: 8080 }}]
      dns_refresh_rate: 5s
```
<!-- prettier-ignore-end -->
