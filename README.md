# Grafana Mimir

<p align="center"><img src="images/logo.png" alt="Grafana Mimir logo"></p>

Grafana Mimir provides horizontally scalable, highly available, multi-tenant, long-term storage for [Prometheus](https://prometheus.io).

- **Horizontally scalable:** Grafana Mimir can run across multiple machines in a cluster, exceeding the throughput and storage of a single machine. This enables you to send the metrics from multiple Prometheus servers to a single Mimir cluster and run "globally aggregated" queries across all data in a single place.
- **Highly available:** When run in a cluster, Grafana Mimir can replicate data between machines. This allows you to survive machine failure without gaps in your graphs.
- **Multi-tenant:** Grafana Mimir can isolate data and queries from multiple different independent
  Prometheus sources in a single cluster, allowing untrusted parties to share the same cluster.
- **Long term storage:** Grafana Mimir supports S3, GCS, Swift and Microsoft Azure for long term storage of metric data. This allows you to durably store data for longer than the lifetime of any single machine, and use this data for long term capacity planning.

## Documentation

If you’re new to Grafana Mimir, read the [Getting started guide](docs/sources/getting-started/_index.md).

Before deploying Grafana Mimir with a permanent storage backend, read:

1. [An overview of Grafana Mimir’s architecture](docs/sources/architecture.md)
1. [Getting started with Grafana Mimir](docs/sources/getting-started/_index.md)
1. [Configuring Grafana Mimir](docs/sources/configuration/_index.md)

## Contributing

To contribute to Grafana Mimir, see [Contributing to Grafana Mimir](./CONTRIBUTING.md).

## Hosted Grafana Mimir (Prometheus as a service)

Grafana Mimir is used in [Grafana Cloud](https://grafana.com/cloud), and is primarily used as a [remote write](https://prometheus.io/docs/operating/configuration/#remote_write) destination for Prometheus via a Prometheus-compatible query API.

### Grafana Cloud

As the creators of [Grafana](https://grafana.com/oss/grafana/), [Loki](https://grafana.com/oss/loki/), and [Tempo](https://grafana.com/oss/tempo/), Grafana Labs offers you the most comprehensive Observability-as-a-Service stack available.
