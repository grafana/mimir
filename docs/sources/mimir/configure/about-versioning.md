---
aliases:
  - ../configuring/about-versioning/
description: Learn about guarantees for this Grafana Mimir major release.
menuTitle: Versioning
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

- Alertmanager
  - Enable a set of experimental API endpoints to help support the migration of the Grafana Alertmanager to the Mimir Alertmanager.
    - `-alertmanager.grafana-alertmanager-compatibility-enabled`
  - Enable support for any UTF-8 character as part of Alertmanager configuration/API matchers and labels.
    - `-alertmanager.utf8-strict-mode-enabled`
- Compactor
  - Enable cleanup of remaining files in the tenant bucket when there are no blocks remaining in the bucket index.
    - `-compactor.no-blocks-file-cleanup-enabled`
- Ruler
  - Tenant federation
  - Disable alerting and recording rules evaluation on a per-tenant basis
    - `-ruler.recording-rules-evaluation-enabled`
    - `-ruler.alerting-rules-evaluation-enabled`
  - Aligning of evaluation timestamp on interval (`align_evaluation_time_on_interval`)
- Distributor
  - Metrics relabeling
    - `-distributor.metric-relabeling-enabled`
  - Using status code 529 instead of 429 upon rate limit exhaustion.
    - `distributor.service-overload-status-code-on-rate-limit-enabled`
  - Set Retry-After header in recoverable error responses
    - `-distributor.retry-after-header.enabled`
    - `-distributor.retry-after-header.base-seconds`
    - `-distributor.retry-after-header.max-backoff-exponent`
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
- Ingester
  - Add variance to chunks end time to spread writing across time (`-blocks-storage.tsdb.head-chunks-end-time-variance`)
  - Snapshotting of in-memory TSDB data on disk when shutting down (`-blocks-storage.tsdb.memory-snapshot-on-shutdown`)
  - Out-of-order samples ingestion (`-ingester.out-of-order-time-window`)
  - Shipper labeling out-of-order blocks before upload to cloud storage (`-ingester.out-of-order-blocks-external-label-enabled`)
  - Postings for matchers cache configuration:
    - `-blocks-storage.tsdb.head-postings-for-matchers-cache-ttl`
    - `-blocks-storage.tsdb.head-postings-for-matchers-cache-size` (deprecated)
    - `-blocks-storage.tsdb.head-postings-for-matchers-cache-max-bytes`
    - `-blocks-storage.tsdb.head-postings-for-matchers-cache-force`
    - `-blocks-storage.tsdb.block-postings-for-matchers-cache-ttl`
    - `-blocks-storage.tsdb.block-postings-for-matchers-cache-size` (deprecated)
    - `-blocks-storage.tsdb.block-postings-for-matchers-cache-max-bytes`
    - `-blocks-storage.tsdb.block-postings-for-matchers-cache-force`
  - CPU/memory utilization based read request limiting:
    - `-ingester.read-path-cpu-utilization-limit`
    - `-ingester.read-path-memory-utilization-limit"`
  - Early TSDB Head compaction to reduce in-memory series:
    - `-blocks-storage.tsdb.early-head-compaction-min-in-memory-series`
    - `-blocks-storage.tsdb.early-head-compaction-min-estimated-series-reduction-percentage`
  - Timely head compaction (`-blocks-storage.tsdb.timely-head-compaction-enabled`)
  - Count owned series and use them to enforce series limits:
    - `-ingester.track-ingester-owned-series`
    - `-ingester.use-ingester-owned-series-for-limits`
    - `-ingester.owned-series-update-interval`
- Ingester client
  - Per-ingester circuit breaking based on requests timing out or hitting per-instance limits
    - `-ingester.client.circuit-breaker.enabled`
    - `-ingester.client.circuit-breaker.failure-threshold`
    - `-ingester.client.circuit-breaker.failure-execution-threshold`
    - `-ingester.client.circuit-breaker.period`
    - `-ingester.client.circuit-breaker.cooldown-period`
- Querier
  - Use of Redis cache backend (`-blocks-storage.bucket-store.metadata-cache.backend=redis`)
  - Streaming chunks from ingester to querier (`-querier.prefer-streaming-chunks-from-ingesters`)
  - Streaming chunks from store-gateway to querier (`-querier.prefer-streaming-chunks-from-store-gateways`, `-querier.streaming-chunks-per-store-gateway-buffer-size`)
  - Ingester query request minimisation (`-querier.minimize-ingester-requests`)
  - Limiting queries based on the estimated number of chunks that will be used (`-querier.max-estimated-fetched-chunks-per-query-multiplier`)
  - Max concurrency for tenant federated queries (`-tenant-federation.max-concurrent`)
  - Maximum response size for active series queries (`-querier.active-series-results-max-size-bytes`)
  - Enable PromQL experimental functions (`-querier.promql-experimental-functions-enabled`)
  - Allow streaming of `/active_series` responses to the frontend (`-querier.response-streaming-enabled`)
- Query-frontend
  - `-query-frontend.querier-forget-delay`
  - Instant query splitting (`-query-frontend.split-instant-queries-by-interval`)
  - Lower TTL for cache entries overlapping the out-of-order samples ingestion window (re-using `-ingester.out-of-order-allowance` from ingesters)
  - Use of Redis cache backend (`-query-frontend.results-cache.backend=redis`)
  - Query blocking on a per-tenant basis (configured with the limit `blocked_queries`)
  - Max number of tenants that may be queried at once (`-tenant-federation.max-tenants`)
  - Sharding of active series queries (`-query-frontend.shard-active-series-queries`)
  - Server-side write timeout for responses to active series requests (`-query-frontend.active-series-write-timeout`)
- Query-scheduler
  - `-query-scheduler.querier-forget-delay`
- Store-gateway
  - Use of Redis cache backend (`-blocks-storage.bucket-store.chunks-cache.backend=redis`, `-blocks-storage.bucket-store.index-cache.backend=redis`, `-blocks-storage.bucket-store.metadata-cache.backend=redis`)
  - `-blocks-storage.bucket-store.series-selection-strategy`
  - Eagerly loading some blocks on startup even when lazy loading is enabled `-blocks-storage.bucket-store.index-header.eager-loading-startup-enabled`
- Read-write deployment mode
- API endpoints:
  - `/api/v1/user_limits`
  - `/api/v1/cardinality/active_series`
- Metric separation by an additionally configured group label
  - `-validation.separate-metrics-group-label`
  - `-max-separate-metrics-groups-per-user`
- Vault
  - Fetching TLS secrets from Vault for various clients (`-vault.enabled`)
  - Vault client authentication token lifetime watcher. Ensures the client token is always valid by renewing the token lease or re-authenticating. Includes the metrics:
    - `cortex_vault_token_lease_renewal_active`
    - `cortex_vault_token_lease_renewal_success_total`
    - `cortex_vault_auth_success_total`
- Logger
  - Rate limited logger support
    - `log.rate-limit-enabled`
    - `log.rate-limit-logs-per-second`
    - `log.rate-limit-logs-burst-size`
- Memcached client
  - Customise write and read buffer size
    - `-<prefix>.memcached.write-buffer-size-bytes`
    - `-<prefix>.memcached.read-buffer-size-bytes`
- Timeseries Unmarshal caching optimization in distributor (`-timeseries-unmarshal-caching-optimization-enabled`)
- Reusing buffers for marshalling write requests in distributors (`-distributor.write-requests-buffer-pooling-enabled`)
- Logging of requests that did not send any HTTP request: `-server.http-log-closed-connections-without-response-enabled`.
- Ingester: track "owned series" and use owned series instead of in-memory series for tenant limits.
  - `-ingester.use-ingester-owned-series-for-limits`
  - `-ingester.track-ingester-owned-series`
  - `-ingester.owned-series-update-interval`

## Deprecated features

Deprecated features are usable up until the release that indicates their removal.
For details about what _deprecated_ means, see [Parameter lifecycle]({{< relref "./configuration-parameters#parameter-lifecycle" >}}).

The following features or configuration parameters are currently deprecated and will be **removed in Mimir 2.13**:

- Logging
  - `-log.buffered`

The following features or configuration parameters are currently deprecated and will be **removed in Mimir 2.14**:

- Distributor
  - the metric `cortex_distributor_sample_delay_seconds`
- Ingester
  - `-ingester.return-only-grpc-errors`
- Ingester client
  - `-ingester.client.report-grpc-codes-in-instrumentation-label-enabled`
