---
description: Learn how to configure Grafana Mimir for resource utilization based ingester read path limiting.
menuTitle: Resource utilization based ingester read path limiting
title: Configure resource utilization based ingester read path limiting
weight: 120
---

# Configure resource utilization based ingester read path limiting

Grafana Mimir allows you to configure limits for CPU and/or memory utilization, which cause the ingester to reject incoming read requests while either of the limits is reached.
The idea is to prevent expensive queries from interrupting the write path, ensuring a minimum of headroom for the latter.
The ingester's model of memory utilization corresponds to the Go heap size, which it tracks, along with a sliding window average of the process' CPU utilization.

The process' CPU utilization and Go memory heap size are sampled every second, and a sliding window average is taken of
the CPU utilization. If either is greater than or equal to the corresponding configured limit, ingester read requests
get rejected with HTTP status code 503 (Service Unavailable), until utilization levels are below respective limits again.

Whenever the ingester rejects a read request due to utilization based limiting, it increments the
`cortex_ingester_utilization_limited_read_requests_total` counter metric.
CPU and memory utilization are also tracked, via the `cortex_ingester_utilization_limiter_current_cpu_load` and
`cortex_ingester_utilization_limiter_current_memory_usage_bytes` gauge metrics respectively.

To configure resource utilization based ingester read path limiting, you may use the following flags:

- `-ingester.read-path-cpu-utilization-limit`: CPU utilization limit, as CPU cores
- `-ingester.read-path-memory-utilization-limit`: Memory limit, in bytes

Alternatively, you may configure the ingester via YAML, as in the following snippet:

```yaml
ingester:
  # Configure ingester to reject read requests when average CPU utilization is >= 0.8 cores
  read_path_cpu_utilization_limit: 0.8
  # Configure ingester to reject read requests when memory utilization is >= 16 GB
  read_path_memory_utilization_limit: 20132659200
```
