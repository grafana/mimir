---
aliases:
  - ../configuring/about-versioning/
description: Learn about guarantees for this Grafana Mimir major release.
menuTitle: About versioning
title: About Grafana Mimir versioning
weight: 50
---

# About Grafana Mimir versioning

This topic describes our guarantees for this Grafana Mimir major release.

## Flags, configuration, and minor version upgrades

Upgrading Grafana Mimir from one minor version to the next minor version should work, but we don't want to bump the major version every time we remove a configuration parameter.
We will keep [deprecated features](#deprecated-features) in place for two minor releases.
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
- [Delete series API](https://prometheus.io/docs/prometheus/latest/querying/api/#delete-series).

## Experimental features

Grafana Mimir is an actively developed project and we encourage the introduction of new features and capabilities.
Not everything in each release of Grafana Mimir is considered production-ready.
We mark as "Experimental" all features and flags that we don't consider production-ready.

We do not guarantee backwards compatibility for experimental features and flags.
Experimental configuration and flags are subject to change.

The following features are currently experimental:

- Ruler
  - Tenant federation
  - Disable alerting and recording rules evaluation on a per-tenant basis
    - `-ruler.recording-rules-evaluation-enabled`
    - `-ruler.alerting-rules-evaluation-enabled`
  - Aligning of evaluation timestamp on interval (`align_evaluation_time_on_interval`)
  - Ruler storage cache
    - `-ruler-storage.cache.*`
- Distributor
  - Metrics relabeling
  - OTLP ingestion path
- Hash ring
  - Disabling ring heartbeat timeouts
    - `-distributor.ring.heartbeat-timeout=0`
    - `-ingester.ring.heartbeat-timeout=0`
    - `-ruler.ring.heartbeat-timeout=0`
    - `-alertmanager.sharding-ring.heartbeat-timeout=0`
    - `-compactor.ring.heartbeat-timeout=0`
    - `-store-gateway.sharding-ring.heartbeat-timeout=0`
    - `-overrides-exporter.ring.heartbeat-timeout=0`
  - Disabling ring heartbeats
    - `-distributor.ring.heartbeat-period=0`
    - `-ingester.ring.heartbeat-period=0`
    - `-ruler.ring.heartbeat-period=0`
    - `-alertmanager.sharding-ring.heartbeat-period=0`
    - `-compactor.ring.heartbeat-period=0`
    - `-store-gateway.sharding-ring.heartbeat-period=0`
    - `-overrides-exporter.ring.heartbeat-period=0`
  - Exclude ingesters running in specific zones (`-ingester.ring.excluded-zones`)
- Ingester
  - Add variance to chunks end time to spread writing across time (`-blocks-storage.tsdb.head-chunks-end-time-variance`)
  - Snapshotting of in-memory TSDB data on disk when shutting down (`-blocks-storage.tsdb.memory-snapshot-on-shutdown`)
  - Out-of-order samples ingestion (`-ingester.out-of-order-time-window`)
  - Shipper labeling out-of-order blocks before upload to cloud storage (`-ingester.out-of-order-blocks-external-label-enabled`)
  - Postings for matchers cache configuration:
    - `-blocks-storage.tsdb.head-postings-for-matchers-cache-ttl`
    - `-blocks-storage.tsdb.head-postings-for-matchers-cache-size`
    - `-blocks-storage.tsdb.head-postings-for-matchers-cache-force`
    - `-blocks-storage.tsdb.block-postings-for-matchers-cache-ttl`
    - `-blocks-storage.tsdb.block-postings-for-matchers-cache-size`
    - `-blocks-storage.tsdb.block-postings-for-matchers-cache-force`
  - CPU/memory utilization based read request limiting:
    - `-ingester.read-path-cpu-utilization-limit`
    - `-ingester.read-path-memory-utilization-limit"`
  - Force TSDB Head compaction to reduce in-memory series:
    - `-blocks-storage.tsdb.forced-head-compaction-min-in-memory-series`
    - `-blocks-storage.tsdb.forced-head-compaction-min-estimated-series-reduction-percentage`
- Querier
  - Use of Redis cache backend (`-blocks-storage.bucket-store.metadata-cache.backend=redis`)
  - Streaming chunks from ingester to querier (`-querier.prefer-streaming-chunks`, `-querier.streaming-chunks-per-ingester-buffer-size`)
- Query-frontend
  - `-query-frontend.querier-forget-delay`
  - Instant query splitting (`-query-frontend.split-instant-queries-by-interval`)
  - Lower TTL for cache entries overlapping the out-of-order samples ingestion window (re-using `-ingester.out-of-order-allowance` from ingesters)
  - Cardinality-based query sharding (`-query-frontend.query-sharding-target-series-per-shard`)
  - Use of Redis cache backend (`-query-frontend.results-cache.backend=redis`)
  - Query expression size limit (`-query-frontend.max-query-expression-size-bytes`)
  - Cardinality query result caching (`-query-frontend.results-cache-ttl-for-cardinality-query`)
- Query-scheduler
  - `-query-scheduler.querier-forget-delay`
- Store-gateway
  - `-blocks-storage.bucket-store.chunks-cache.fine-grained-chunks-caching-enabled`
  - `-blocks-storage.bucket-store.fine-grained-chunks-caching-ranges-per-series`
  - Use of Redis cache backend (`-blocks-storage.bucket-store.chunks-cache.backend=redis`, `-blocks-storage.bucket-store.index-cache.backend=redis`, `-blocks-storage.bucket-store.metadata-cache.backend=redis`)
  - `-blocks-storage.bucket-store.series-selection-strategy`
- Read-write deployment mode
- `/api/v1/user_limits` API endpoint
- Metric separation by an additionally configured group label
  - `-validation.separate-metrics-group-label`
  - `-max-separate-metrics-groups-per-user`
- Overrides-exporter
  - Peer discovery / tenant sharding for overrides exporters (`-overrides-exporter.ring.enabled`)
- Per-tenant Results cache TTL (`-query-frontend.results-cache-ttl`, `-query-frontend.results-cache-ttl-for-out-of-order-time-window`)
- Fetching TLS secrets from Vault for various clients (`-vault.enabled`)
- Timeseries Unmarshal caching optimization in distributor (`-timeseries-unmarshal-caching-optimization-enabled`)
- Reusing buffers for marshalling write requests in distributors (`-distributor.write-requests-buffer-pooling-enabled`)

## Deprecated features

Deprecated features are usable up until the release that indicates their removal.
For details about what _deprecated_ means, see [Parameter lifecycle]({{< relref "../references/configuration-parameters#parameter-lifecycle" >}}).

The following features are currently deprecated and will be **removed in Mimir 2.10**:

- Ingester
  - `-blocks-storage.tsdb.max-tsdb-opening-concurrency-on-startup`

The following features or configuration parameters are currently deprecated and will be **removed in Mimir 2.11**:

- Store-gateway
  - `-blocks-storage.bucket-store.chunk-pool-min-bucket-size-bytes`
  - `-blocks-storage.bucket-store.chunk-pool-max-bucket-size-bytes`
  - `-blocks-storage.bucket-store.max-chunk-pool-bytes`
- Querier, ruler, store-gateway
  - `-blocks-storage.bucket-store.bucket-index.enabled`
- Querier
  - `-querier.iterators` and `-querier.batch-iterators` (Mimir 2.11 onwards will always use `-querier.batch-iterators=true`)
