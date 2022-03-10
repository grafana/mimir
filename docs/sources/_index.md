---
title: "Grafana Mimir technical documentation"
weight: 1
---

# Grafana Mimir Documentation

![Grafana Mimir](./images/mimir-logo.png)

Grafana Mimir provides horizontally scalable, highly available, multi-tenant, long-term storage for [Prometheus](https://prometheus.io).

- **Horizontally scalable:** Grafana Mimir can run across multiple machines in a cluster, exceeding the throughput and storage of a single machine. This enables you to send the metrics from multiple Prometheus servers to a single Grafana Mimir cluster and run globally aggregated queries across all data in a single place.
- **Highly available:** When run in a cluster, Grafana Mimir replicates data between machines.
  This makes Grafana Mimir resilient to machine failure, which ensures that there is no data missing in your graphs.
- **Multi-tenant:** Grafana Mimir can isolate data and queries from multiple independent
  Prometheus sources in a single cluster, allowing untrusted parties to share the same cluster.
- **Long-term storage:** Grafana Mimir supports S3, GCS, Swift, and Microsoft Azure for long-term storage of metric data. This enables you to durably store data for longer than the lifetime of a single machine, and use this data for long-term capacity planning.

> **Note:** You can use [Grafana Cloud](https://grafana.com/products/cloud/features/#cloud-metrics) to avoid installing, maintaining, and scaling your own instance of Grafana Mimir. The free forever plan includes 50GB of free logs. [Create an account to get started](https://grafana.com/auth/sign-up/create-user?pg=docs-mimir&plcmt=in-text).

