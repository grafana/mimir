---
title: "Mirror requests to a second cluster"
description: ""
weight: 10
---

# Mirror requests to a second cluster

Requests mirroring can be used when you need to setup a testing Grafana Mimir cluster receiving the same series ingested by a primary one and you don't have control over Prometheus remote write config. If you have control over it, then configuring two remote write entries in Prometheus would be the preferred option. Refer to the [Prometheus remote write reference][reference-prom-rw] for help.

[reference-prom-rw]: https://prometheus.io/docs/prometheus/latest/configuration/configuration/#remote_write

## Mirroring with Envoy proxy

[Envoy proxy](https://www.envoyproxy.io/) can be used to mirror HTTP requests to a secondary upstream cluster. From a network path perspective, you should run Envoy in front of both clusters' [distributors](../../architecture/distributor). This lets Envoy proxy requests to the primary Grafana Mimir cluster and mirror them to a secondary cluster in the background. The performance and availability of the secondary cluster have no impact on the requests to the primary one. The response to the client will always be the one from the primary one. In this sense, the requests from Envoy to the secondary cluster are "fire and forget".

The diagram below shows the simplified network structure.

<!-- Diagram source at https://docs.google.com/presentation/d/1bHp8_zcoWCYoNU2AhO2lSagQyuIrghkCncViSqn14cU/edit -->

![Mirroring with Envoy Proxy - network diagram](../../images/mirroring-envoy.png)

### Example Envoy configuration

The following Envoy configuration shows an example with two Grafana Mimir clusters. Envoy will listen on port `9900` and will proxy all requests to `mimir-primary:8080`, while mirroring them to `mimir-secondary:8080`.

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

                            # Specifies the cluster that requests will be mirrored to. The performance
                            # and availability of the secondary cluster have no impact on the requests to the primary
                            # one. The response to the client will always be the one from the primary one. In this sense,
                            # the requests from Envoy to the secondary cluster are "fire and forget".
                            request_mirror_policies:
                              - cluster: mimir_secondary
                http_filters:
                  - name: envoy.router
  clusters:
    - name: mimir_primary
      type: STRICT_DNS
      connect_timeout: 1s
      # Replace mimir-primary with the address and port the distributor of your primary mimir cluster
      hosts: [{ socket_address: { address: mimir-primary, port_value: 8080 }}]
      dns_refresh_rate: 5s
    - name: mimir_secondary
      type: STRICT_DNS
      connect_timeout: 1s
      # Replace mimir-secondary with the address and port the distributor of your secondary mimir cluster
      hosts: [{ socket_address: { address: mimir-secondary, port_value: 8080 }}]
      dns_refresh_rate: 5s
```
<!-- prettier-ignore-end -->
