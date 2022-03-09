---
title: "Grafana Mimir technical documentation"
weight: 1
---

Grafana Mimir provides horizontally scalable, highly available, multi-tenant, long-term storage for [Prometheus](https://prometheus.io).

- **Horizontally scalable:** Grafana Mimir can run across multiple machines in a cluster, exceeding the throughput and storage of a single machine. This enables you to send the metrics from multiple Prometheus servers to a single Grafana Mimir cluster and run globally aggregated queries across all data in a single place.
- **Highly available:** When run in a cluster, Grafana Mimir replicates data between machines.
  This makes Grafana Mimir resilient to machine failure, which ensures that there is no data missing in your graphs.
- **Multi-tenant:** Grafana Mimir can isolate data and queries from multiple independent
  Prometheus sources in a single cluster, allowing untrusted parties to share the same cluster.
- **Long-term storage:** Grafana Mimir supports S3, GCS, Swift, and Microsoft Azure for long-term storage of metric data. This enables you to durably store data for longer than the lifetime of a single machine, and use this data for long-term capacity planning.

## Documentation

If you’re new to Grafana Mimir, read [Getting started with Grafana Mimir]({{< relref "./getting-started/_index.md" >}}).

Before deploying Grafana Mimir, read:

1. [Grafana Mimir architecture]({{< relref "architecture.md" >}})
1. [Getting started with Grafana Mimir]({{< relref "getting-started/_index.md" >}})
1. [Configuring Grafana Mimir]({{< relref "configuring/_index.md" >}})

## Hosted Grafana Mimir (Prometheus as a service)

Grafana Mimir is used in [Grafana Cloud](https://grafana.com/cloud), and is primarily used as a [remote write](https://prometheus.io/docs/operating/configuration/#remote_write) destination for Prometheus via a Prometheus-compatible query API.

### Grafana Cloud

As the creators of [Grafana](https://grafana.com/oss/grafana/), [Grafana Loki](https://grafana.com/oss/loki/), and [Grafana Tempo](https://grafana.com/oss/tempo/), Grafana Labs can offer you the most holistic Observability-as-a-Service stack out there.
