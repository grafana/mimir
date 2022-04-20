---
title: "Configuring Grafana Mimir to evaluate rules using query-frontend"
menuTitle: "Configuring to evaluate rules using query-frontend"
description: "Learn how to configure Grafana Mimir ruler to use query-frontend for rule evaluation."
weight: 95
---

# Configuring to evaluate rules using query-frontend

Grafana Mimir allows you to configure the ruler to delegate evaluation of rule expressions to the query-frontend.
In this way, it is possible to leverage query optimization capabilities, such as [query sharding]({{< relref "../architecture/query-sharding/index.md" >}})), to drastically reduce evaluation times.

## Configuration

To enable query-frontend rule evaluation, set the `-ruler.query-frontend.address` CLI flag or its respective YAML configuration parameter for the ruler.

Communication between ruler and query-frontend is established over gRPC, so if needed, we can make use of client-side load balancing by prefixing the address value with the `dns://` literal.

On top of that, there's a set of extra parameters `-ruler.query-frontend.tls-*` (and its respective YAML configuration options) that allow us to secure the connection when necessary.

#### Example configuration

The following YAML configuration snippet shows how to configure Grafana Mimir to offload expression evaluation to the query-frontend.

```yaml
ruler:
  query_frontend:
    # Address of the query-frontend service that will perform expression evaluations
    # (prefix it with dns:// to make use of client-side load balancing).
    address: dns://query-frontend:9095

    # Optional TLS parameters for securing connection.
    tls_enabled: true
    tls_cert_path: "/path/to/server.crt"
    tls_key_path: "/path/to/server.key"
    tls_ca_path: "/path/to/root.crt"
    tls_server_name: "server-name"
    tls_insecure_skip_verify: false
  ...
```
