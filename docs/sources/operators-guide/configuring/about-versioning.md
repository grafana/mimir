---
title: "About Grafana Mimir versioning"
menuTitle: "About versioning"
description: "Learn about guarantees for this Grafana Mimir major release."
weight: 50
---

# About Grafana Mimir versioning

This topic describes our guarantees for this Grafana Mimir major release.

## Flags, configuration, and minor version upgrades

Upgrading Grafana Mimir from one minor version to the next minor version should work, but we don't want to bump the major version every time we remove a configuration parameter.
We will keep deprecated flags and YAML configuration parameters in place for two minor releases.
You can use the `deprecated_flags_inuse_total` metric to generate an alert that helps you determine if you're using a deprecated flag.

These guarantees don't apply to [experimental features](#experimental-features).

## Reading old data

The Grafana Mimir maintainers commit to ensuring that future versions can read data written by versions within the last two years.
In practice, we expect to be able to read data written more than two years ago, but a minimum of two years is our guarantee.

## API Compatibility

Grafana Mimir strives to be 100% compatible with the Prometheus HTTP API which is by default served by endpoints with the /prometheus HTTP path prefix `/prometheus/*`.

We consider any deviation from this 100% API compatibility to be a bug, except for the following scenarios:

- Additional API endpoints for creating, removing, modifying alerts, and recording rules.
- Additional APIs that push metrics (under `/prometheus/api/push`).
- Additional API endpoints for management of Grafana Mimir, such as the ring. These APIs are not included in any compatibility guarantees.

## Experimental features

Grafana Mimir is an actively developed project and we encourage the introduction of new features and capabilities.
Not everything in each release of Grafana Mimir is considered production-ready.
We mark as "Experimental" all features and flags that we don't consider production-ready.

We do not guarantee backwards compatibility for experimental features and flags.
Experimental configuration and flags are subject to change.

The following features are currently experimental:

- Ruler: Tenant federation
- Distributor: Metrics relabeling
- Purger: Tenant deletion API
- Exemplar storage
  - `-ingester.max-global-exemplars-per-user`
  - `-ingester.exemplars-update-period`
  - API endpoint `/api/v1/query_exemplars`
- Hash ring
  - Disabling ring heartbeat timeouts
    - `-distributor.ring.heartbeat-timeout=0`
    - `-ingester.ring.heartbeat-timeout=0`
    - `-ruler.ring.heartbeat-timeout=0`
    - `-alertmanager.sharding-ring.heartbeat-timeout=0`
    - `-compactor.ring.heartbeat-timeout=0`
    - `-store-gateway.sharding-ring.heartbeat-timeout=0`
  - Disabling ring heartbeats
    - `-distributor.ring.heartbeat-period=0`
    - `-ingester.ring.heartbeat-period=0`
    - `-ruler.ring.heartbeat-period=0`
    - `-alertmanager.sharding-ring.heartbeat-period=0`
    - `-compactor.ring.heartbeat-period=0`
    - `-store-gateway.sharding-ring.heartbeat-period=0`
  - Exclude ingesters running in specific zones (`-ingester.ring.excluded-zones`)
- Ingester
  - Add variance to chunks end time to spread writing across time (`-blocks-storage.tsdb.head-chunks-end-time-variance`)
  - Using queue and asynchronous chunks disk mapper (`-blocks-storage.tsdb.head-chunks-write-queue-size`)
  - Snapshotting of in-memory TSDB data on disk when shutting down (`-blocks-storage.tsdb.memory-snapshot-on-shutdown`)
- Query-frontend
  - `-query-frontend.querier-forget-delay`
- Query-scheduler
  - `-query-scheduler.querier-forget-delay`
- Store-gateway
  - `-blocks-storage.bucket-store.index-header-thread-pool-size`

## Deprecated features

The following features are currently deprecated:

- Ingester:
  - `-blocks-storage.tsdb.isolation-enabled` CLI flag and `isolation_enabled` YAML config parameter. This will be removed in version 2.3.0.
  - `active_series_custom_trackers` YAML config parameter in the ingester block. The configuration has been moved to limit config, the ingester config will be removed in version 2.3.0.
- Ruler:
  - `/api/v1/rules/**` configuration endpoints. These will be removed in version 2.2.0. Use their `<prometheus-http-prefix>/config/v1/rules/**` equivalents instead.
  - `<prometheus-http-prefix>/rules/**` configuration endpoints. These will be removed in version 2.2.0. Use their `<prometheus-http-prefix>/config/v1/rules/**` equivalents instead.
