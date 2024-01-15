---
aliases:
  - ../operators-guide/configure/configure-resource-utilization-based-ingester-read-path-limiting/
description: Learn how to configure Grafana Mimir for resource utilization based ingester read path limiting.
menuTitle: Resource utilization based ingester read path limiting
title: Configure resource utilization based ingester read path limiting
weight: 120
---

# Configure resource utilization based ingester read path limiting

As an **experimental** feature, Grafana Mimir allows you to configure limits for CPU and/or memory utilization which can cause the ingester to reject incoming read requests in order to protect the write path. The ingester's model of memory utilization
corresponds to the Go heap size, which it tracks, along with a sliding window average of the process' CPU utilization.

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
