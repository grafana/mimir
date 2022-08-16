---
description: Learn about Grafana Mimir anonymous usage statistics reporting
menuTitle: About anonymous usage statistics reporting
title: About Grafana Mimir anonymous usage statistics reporting
weight: 30
---

# About Grafana Mimir anonymous usage statistics reporting

Grafana Mimir includes a system to anonymously report non-sensitive and non-personal identifiable information about the running Mimir cluster to a remote statistics server.
Mimir maintainers use these anonymous information to learn more about how the opensource community runs Mimir and what the Mimir team should focus on when working on the next features and documentation improvements.

The anonymous usage statistics reporting is **disabled by default**.
If possible, we ask you to enable the usage statistics.

## The statistics server

When the usage statistics reporting is enabled, the information is collected by a server run by Grafana Labs and exposed at `https://stats.grafana.org`.

## Which information is collected

When the usage statistics reporting is enabled, Grafana Mimir collects the following information:

- Information about the **Mimir cluster and version**
  - Unique randomly-generated Mimir cluster identifier (e.g. `3749b5e2-b727-4107-95ae-172abac27496`).
  - Timestamp when the anonymous usage statistics reporting was enabled the first time and the cluster identifier was created.
  - The Mimir version (e.g. `2.3.0`).
  - The Mimir branch, revision and golang version used to build the binary.
- Information about the **environment** where Mimir is running
  - The operating system (e.g. `linux`) and architecture (e.g. `amd64`) where Mimir is running.
  - The Mimir memory utilization and number of goroutines.
  - The number of logical CPU cores available to the Mimir process.
- Information about the Mimir **configuration**
  - The `-target` parameter value (e.g. `all` when running Mimir in monolithic mode).
  - The `-blocks-storage.backend` value (e.g. `s3`).
  - The `-ingester.ring.replication-factor` value (e.g. `3`).
- Information about the Mimir **cluster scale**
  - Ingester:
    - The number of inmemory series.
    - The number of tenants having inmemory series.
    - The number of samples and exemplars ingested.
  - Querier:
    - The number of requests to queriers, split by API endpoint type (no information is tracked about the actual request or query). The following endpoints are tracked:
      - Remote read.
      - Instant query.
      - Range query.
      - Exemplars query.
      - Labels query.
      - Series query.
      - Metadata query.
      - Cardinality analysis query.

> **Note**: Mimir maintainers commit to keep the list of tracked information updated over time and report any change both in the CHANGELOG and release notes.

## How to enable the anonymous usage statistics reporting

If you would like to participate in usage statistics reporting, the feature can be enabled setting the CLI flag `-usage-stats.enabled=true` or the following YAML configuration:

```yaml
usage_stats:
  enabled: true
```
