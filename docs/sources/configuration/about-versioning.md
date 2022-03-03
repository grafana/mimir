---
title: "About versioning"
description: ""
weight: 10
---

# About versioning

Mimir provides the following guarantees:

## Flags, Config and minor version upgrades

Upgrading Grafana Mimir from one minor version to the next should just work. That being said, we don't want to bump the major version every time we remove a configuration parameter. Please see [Parameter Lifecycle]({{< relref "./reference-configuration-parameters/#parameter-lifecycle" >}}) for how we handle removal of configuration paramaeters.

## Reading old data

The Grafana Mimir maintainers commit to ensuring future versions can read data written by versions up to two years old. In practice we expect to be able to read more, but this is our guarantee.

## API Compatibility

Grafana Mimir strives to be 100% API compatible with Prometheus (under `/prometheus/*`); any deviation from this is considered a bug, except:

- Additional API endpoints for creating, removing and modifying alerts and recording rules.
- Additional API around pushing metrics (under `/prometheus/api/push`).
- Additional API endpoints for management of Grafana Mimir itself, such as the ring. These APIs are not part of the any compatibility guarantees.

## Experimental features

Grafana Mimir is an actively developed project and we want to encourage the introduction of new features and capabilities. As such, not everything in each release of Grafana Mimir is considered "production-ready". Features not considered "production-ready" will be marked "experimental" in the documentation. The flags used to enable and/or configure these features will also be marked "experimental", as described in [Parameter Categories]({{< relref "./reference-configuration-parameters/#parameter-categories" >}}).

There are no backwards compatibility guarantees on anything marked experimental. All configuration parameters relating to experimental features are subject to change.

Currently experimental features are:

- Ruler: tenant federation.
- Distributor: metrics relabeling.
- Purger: tenant deletion API.
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

## Deprecated features

Currently deprecated features are:

- Ruler:
  - `/api/v1/rules/**` configuration endpoints. These will be removed in version 2.2.0. Use their `<prometheus-http-prefix>/config/v1/rules/**` equivalents instead.
  - `<prometheus-http-prefix>/rules/**` configuration endpoints. These will be removed in version 2.2.0. Use their `<prometheus-http-prefix>/config/v1/rules/**` equivalents instead.
