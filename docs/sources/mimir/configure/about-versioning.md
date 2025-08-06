---
aliases:
  - ../configuring/about-versioning/
description: Learn about guarantees for this Grafana Mimir major release.
menuTitle: Versioning
title: About Grafana Mimir versioning
weight: 35
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

- Cost attribution
  - Configure labels for cost attribution
    - `-validation.cost-attribution-labels-structured`
  - Configure cost attribution limits, such as cardinality:
    - `-validation.max-cost-attribution-cardinality`
  - Configure cooldown periods and eviction intervals for cost attribution
    - `-validation.cost-attribution-cooldown`
    - `-cost-attribution.eviction-interval`
  - Configure the metrics endpoint dedicated to cost attribution
    - `-cost-attribution.registry-path`
  - Configure the cost attribution cleanup process run interval
    - `-cost-attribution.cleanup-interval`
- Alertmanager
  - Enable a set of experimental API endpoints to help support the migration of the Grafana Alertmanager to the Mimir Alertmanager.
    - `-alertmanager.grafana-alertmanager-compatibility-enabled`
  - Enable support for any UTF-8 character as part of Alertmanager configuration/API matchers and labels.
    - `-alertmanager.utf8-strict-mode-enabled`
- Compactor
  - Enable cleanup of remaining files in the tenant bucket when there are no blocks remaining in the bucket index.
    - `-compactor.no-blocks-file-cleanup-enabled`
  - In-memory cache for parsed meta.json files:
    - `-compactor.in-memory-tenant-meta-cache-size`
  - Limit blocks processed in each compaction cycle. Blocks uploaded prior to the maximum lookback aren't processed.
    - `-compactor.max-lookback`
  - Enable the compactor to upload sparse index headers to object storage during compaction cycles.
    - `-compactor.upload-sparse-index-headers`
- Ruler
  - Aligning of evaluation timestamp on interval (`align_evaluation_time_on_interval`)
  - Allow defining limits on the maximum number of rules allowed in a rule group by namespace and the maximum number of rule groups by namespace. If set, this supersedes the `-ruler.max-rules-per-rule-group` and `-ruler.max-rule-groups-per-tenant` limits.
  - `-ruler.max-rules-per-rule-group-by-namespace`
  - `-ruler.max-rule-groups-per-tenant-by-namespace`
  - Allow protecting rule groups from modification by namespace. Rule groups can always be read, and you can use the `X-Mimir-Ruler-Override-Namespace-Protection` header with namespace names as values to override protection from modification.
  - `-ruler.protected-namespaces`
  - Allow control over independent rules to be evaluated concurrently as long as they exceed a certain threshold on their rule group last duration runtime against their interval. We have both a limit on the number of rules that can be executed per ruler and per tenant:
  - `-ruler.max-independent-rule-evaluation-concurrency`
  - `-ruler.max-independent-rule-evaluation-concurrency-per-tenant`
  - `-ruler.independent-rule-evaluation-concurrency-min-duration-percentage`
  - `-ruler.rule-evaluation-write-enabled`
  - Allow control over rule sync intervals.
    - `ruler.outbound-sync-queue-poll-interval`
    - `ruler.inbound-sync-queue-poll-interval`
  - `-ruler.min-rule-evaluation-interval`
  - Configure metric and label name validation scheme
    - `-validation.name-validation-scheme`
- Distributor
  - Influx ingestion
    - `/api/v1/push/influx/write` endpoint
    - `-distributor.influx-endpoint-enabled`
    - `-distributor.max-influx-request-size`
  - Metrics relabeling
    - `-distributor.metric-relabeling-enabled`
  - Using status code 529 instead of 429 upon rate limit exhaustion.
    - `-distributor.service-overload-status-code-on-rate-limit-enabled`
  - Limit exemplars per series per request
    - `-distributor.max-exemplars-per-series-per-request`
  - Limit OTLP write request byte size
    - `-distributor.max-otlp-request-size`
  - Enforce a maximum pool buffer size for write requests
    - `-distributor.max-request-pool-buffer-size`
  - Enable conversion of OTel start timestamps to Prometheus zero samples to mark series start
    - `-distributor.otel-created-timestamp-zero-ingestion-enabled`
  - Promote a certain set of OTel resource attributes to labels
    - `-distributor.otel-promote-resource-attributes`
  - Add experimental `memberlist` key-value store for ha_tracker. Note that this feature is `experimental`, as the upper limits of propagation times have not yet been validated. Additionally, cleanup operations have not yet been implemented for the memberlist entries.
    - `-distributor.ha-tracker.kvstore.store`
  - Allow keeping OpenTelemetry `service.instance.id`, `service.name` and `service.namespace` resource attributes in `target_info` on top of converting them to the `instance` and `job` labels.
    - `-distributor.otel-keep-identifying-resource-attributes`
  - Enable conversion of OTel explicit bucket histograms into native histograms with custom buckets.
    - `-distributor.otel-convert-histograms-to-nhcb`
  - Enable promotion of OTel scope metadata to metric labels
    - `-distributor.otel-promote-scope-metadata`
  - Enable native ingestion of delta OTLP metrics. This means storing the raw delta sample values without converting them to cumulative values and having the metric type set to "Unknown". Delta support is in an early stage of development. The ingestion and querying process is likely to change over time. You can find considerations around querying and gotchas in the [corresponding Prometheus documentation](https://prometheus.io/docs/prometheus/3.4/feature_flags/#otlp-native-delta-support).
    - `distributor.otel-native-delta-ingestion`
  - Configure metric and label name validation scheme
    - `-validation.name-validation-scheme`
  - Configure metric and label name translation strategy in OTLP endpoint
    - `-distributor.otel-translation-strategy`
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
  - Per-ingester circuit breaking based on requests timing out or hitting per-instance limits
    - `-ingester.push-circuit-breaker.circuit-breaker.enabled`
    - `-ingester.push-circuit-breaker.failure-threshold-percentage`
    - `-ingester.push-circuit-breaker.failure-execution-threshold`
    - `-ingester.push-circuit-breaker.thresholding-period`
    - `-ingester.push-circuit-breaker.cooldown-period`
    - `-ingester.push-circuit-breaker.initial-delay`
    - `-ingester.push-circuit-breaker.request-timeout`
    - `-ingester.read-circuit-breaker.circuit-breaker.enabled`
    - `-ingester.read-circuit-breaker.failure-threshold-percentage`
    - `-ingester.read-circuit-breaker.failure-execution-threshold`
    - `-ingester.read-circuit-breaker.thresholding-period`
    - `-ingester.read-circuit-breaker.cooldown-period`
    - `-ingester.read-circuit-breaker.initial-delay`
    - `-ingester.read-circuit-breaker.request-timeout`
  - Reactive concurrency limiters
    - `-ingester.push-reactive-limiter.enabled`
    - `-ingester.push-reactive-limiter.short-window-min-duration`
    - `-ingester.push-reactive-limiter.short-window-max-duration`
    - `-ingester.push-reactive-limiter.short-window-min-samples`
    - `-ingester.push-reactive-limiter.long-window`
    - `-ingester.push-reactive-limiter.sample-quantile`
    - `-ingester.push-reactive-limiter.min-inflight-limit`
    - `-ingester.push-reactive-limiter.max-inflight-limit`
    - `-ingester.push-reactivereactive-limiter.initial-inflight-limit`
    - `-ingester.push-reactive-limiter.max-limit-factor`
    - `-ingester.push-reactive-limiter.correlation-window`
    - `-ingester.push-reactive-limiter.initial-rejection-factor`
    - `-ingester.push-reactive-limiter.max-rejection-factor`
    - `-ingester.read-reactive-limiter.enabled`
    - `-ingester.read-reactive-limiter.short-window-min-duration`
    - `-ingester.read-reactive-limiter.short-window-max-duration`
    - `-ingester.read-reactive-limiter.short-window-min-samples`
    - `-ingester.read-reactive-limiter.long-window`
    - `-ingester.read-reactive-limiter.sample-quantile`
    - `-ingester.read-reactive-limiter.min-inflight-limit`
    - `-ingester.read-reactive-limiter.max-inflight-limit`
    - `-ingester.read-reactive-limiter.initial-inflight-limit`
    - `-ingester.read-reactive-limiter.max-limit-factor`
    - `-ingester.read-reactive-limiter.correlation-window`
    - `-ingester.read-reactive-limiter.initial-rejection-factor`
    - `-ingester.read-reactive-limiter.max-rejection-factor`
    - `-ingester.rejection-prioritizer.calibration-interval`
- Querier
  - Limiting queries based on the estimated number of chunks that will be used (`-querier.max-estimated-fetched-chunks-per-query-multiplier`)
  - Max concurrency for tenant federated queries (`-tenant-federation.max-concurrent`)
  - Maximum response size for active series queries (`-querier.active-series-results-max-size-bytes`)
  - Allow streaming of `/active_series` responses to the frontend (`-querier.response-streaming-enabled`)
  - [Mimir query engine](https://grafana.com/docs/mimir/<MIMIR_VERSION>/references/architecture/mimir-query-engine) (`-querier.query-engine` and `-querier.enable-query-engine-fallback`, and all flags beginning with `-querier.mimir-query-engine`)
  - Maximum estimated memory consumption per query limit (`-querier.max-estimated-memory-consumption-per-query`)
  - Ignore deletion marks while querying delay (`-blocks-storage.bucket-store.ignore-deletion-marks-while-querying-delay`)
- Query-frontend
  - `-query-frontend.querier-forget-delay`
  - Lower TTL for cache entries overlapping the out-of-order samples ingestion window (re-using `-ingester.out-of-order-window` from ingesters)
  - Query blocking on a per-tenant basis (configured with the limit `blocked_queries`)
  - Sharding of active series queries (`-query-frontend.shard-active-series-queries`)
  - Server-side write timeout for responses to active series requests (`-query-frontend.active-series-write-timeout`)
  - Blocking HTTP requests on a per-tenant basis (configured with the `blocked_requests` limit)
  - Spinning off (as actual range queries) subqueries from instant queries (`-query-frontend.subquery-spin-off-enabled` and the `subquery_spin_off_enabled` per-tenant limit)
  - Enable PromQL experimental functions per-tenant (`-query-frontend.enabled-promql-experimental-functions` and the `enabled_promql_experimental_functions` per-tenant limit)
  - Support for cluster validation via `-query-frontend.client-cluster-validation.label` or `-common.client-cluster-validation.label`.
    Requests with invalid cluster validation labels are tracked via the `cortex_client_invalid_cluster_validation_label_requests_total` metric.
  - Support for duration expressions in PromQL, which are simple arithmetics on numbers in offset and range specification.
  - Support for configuring the maximum series limit for cardinality API requests on a per-tenant basis via `cardinality_analysis_max_results`.
  - [Mimir query engine](https://grafana.com/docs/mimir/<MIMIR_VERSION>/references/architecture/mimir-query-engine) (`-query-frontend.query-engine` and `-query-frontend.enable-query-engine-fallback`)
  - Labels query optimizer (`-query-frontend.labels-query-optimizer-enabled`)
- Query-scheduler
  - `-query-scheduler.querier-forget-delay`
- Store-gateway
  - Eagerly loading some blocks on startup even when lazy loading is enabled `-blocks-storage.bucket-store.index-header.eager-loading-startup-enabled`
  - Allow more than the default of 3 store-gateways to own recent blocks `-store-gateway.dynamic-replication`
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
- Timeseries Unmarshal caching optimization in distributor (`-timeseries-unmarshal-caching-optimization-enabled`)
- Reusing buffers for marshalling write requests in distributors (`-distributor.write-requests-buffer-pooling-enabled`)
- Logging of requests that did not send any HTTP request: `-server.http-log-closed-connections-without-response-enabled`.
- Ingester: track "owned series" and use owned series instead of in-memory series for tenant limits.
  - `-ingester.use-ingester-owned-series-for-limits`
  - `-ingester.track-ingester-owned-series`
  - `-ingester.owned-series-update-interval`
- Server
  - [PROXY protocol](https://www.haproxy.org/download/2.3/doc/proxy-protocol.txt) support
    - `-server.proxy-protocol-enabled`
  - Cross-cluster validation support for gRPC and HTTP communication
    - `-server.cluster-validation.label`
    - `-server.cluster-validation.grpc.enabled`
    - `-server.cluster-validation.grpc.soft-validation`
    - `-server.cluster-validation.http.enabled`
    - `-server.cluster-validation.http.soft-validation`
    - `-server.cluster-validation.http.excluded-paths`
    - Requests with invalid cluster validation labels are tracked via the `cortex_server_invalid_cluster_validation_label_requests_total` metric.
- gRPC clients
  - Cross-cluster validation support for gRPC communication:
    - Assuming that a gRPC client configuration can be reached via `-<grpc-client-config-path>`, cluster validation label is configured via: `-<grpc-client-config-path>.cluster-validation.label`.
    - The cluster validation label of all gRPC clients can be configured via `-common.client-cluster-validation.label`.
    - Requests with invalid cluster validation labels are tracked via the `cortex_client_invalid_cluster_validation_label_requests_total` metric.
- Kafka-based ingest storage
  - `-ingest-storage.*`
  - `-ingester.partition-ring.*`

## Deprecated features

Deprecated features are usable up until the release that indicates their removal.
For details about what _deprecated_ means, see [Parameter lifecycle](../configuration-parameters/#parameter-lifecycle).

The following features or configuration parameters are currently deprecated and will be **removed in a future release (to be announced)**:

- Distributor's HA tracker timeouts can now be configured on a per-tenant basis. To support this, their configuration has moved from [`distributor.ha_tracker`](../configuration-parameters/#distributor) to [`limits`](../configuration-parameters/#limits) (deprecated since Mimir 2.17)
- Tracing configuration through Jaeger `JAEGER_*` environment variables and Jaeger tracing exposition protocol (deprecated since Mimir 2.17)
  - Use OpenTelemetry configuration instead, as Jaeger supports OTLP ingestion natively
- Rule group configuration file
  - `evaluation_delay` field: use `query_offset` instead
- The `-ingester.stream-chunks-when-using-blocks` CLI flag, and `ingester_stream_chunks_when_using_blocks` runtime configuration option
- The `-store-gateway.sharding-ring.auto-forget-enabled` is deprecated and will be removed in a future release. Set the `-store-gateway.sharding-ring.auto-forget-unhealthy-periods` flag to 0 to disable the auto-forget feature. Deprecated since Mimir 2.17.
