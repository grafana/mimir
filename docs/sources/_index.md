---
title: "Mimir technical documentation"
linkTitle: "Documentation"
weight: 1
menu:
  main:
    weight: 1
---

Mimir provides horizontally scalable, highly available, multi-tenant, long-term storage for [Prometheus](https://prometheus.io).

- **Horizontally scalable:** Mimir can run across multiple machines in a cluster, exceeding the throughput and storage of a single machine. This enables you to send the metrics from multiple Prometheus servers to a single Mimir cluster and run "globally aggregated" queries across all data in a single place.
- **Highly available:** When run in a cluster, Mimir can replicate data between machines. This allows you to survive machine failure without gaps in your graphs.
- **Multi-tenant:** Mimir can isolate data and queries from multiple different independent
  Prometheus sources in a single cluster, allowing untrusted parties to share the same cluster.
- **Long term storage:** Mimir supports S3, GCS, Swift and Microsoft Azure for long term storage of metric data. This allows you to durably store data for longer than the lifetime of any single machine, and use this data for long term capacity planning.

## Documentation

If you’re new to Mimir, read the [Getting started guide](getting-started/_index.md).

Before deploying Mimir with a permanent storage backend, read:

1. [An overview of Mimir’s architecture](architecture.md)
1. [Getting started with Mimir](getting-started/_index.md)
1. [Configuring Mimir](configuration/_index.md)

There are also individual [guides](guides/_index.md) to many tasks.
Before deploying, review the important [security advice](guides/security.md).

## Contributing

To contribute to Mimir, see the [contributor guidelines](contributing/).

## Hosted Mimir (Prometheus as a service)

Mimir is used in [Grafana Cloud](https://grafana.com/cloud), and is primarily used as a [remote write](https://prometheus.io/docs/operating/configuration/#remote_write) destination for Prometheus via a Prometheus-compatible query API.

### Grafana Cloud

As the creators of [Grafana](https://grafana.com/oss/grafana/), [Loki](https://grafana.com/oss/loki/), and [Tempo](https://grafana.com/oss/tempo/), Grafana Labs can offer you the most wholistic Observability-as-a-Service stack out there.
