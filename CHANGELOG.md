# Changelog

## Grafana Mimir - main / unreleased

### Grafana Mimir

* [CHANGE] Ingester: Added user label to ingester metric `cortex_ingester_tsdb_out_of_order_samples_appended_total`. On multitenant clusters this helps us find the rate of appended out-of-order samples for a specific tenant. #2493
* [CHANGE] Compactor: delete source and output blocks from local disk on compaction failed, to reduce likelihood that subsequent compactions fail because of no space left on disk. #2261
* [CHANGE] Ruler: Remove unused CLI flags `-ruler.search-pending-for` and `-ruler.flush-period` (and their respective YAML config options). #2288
* [CHANGE] Successful gRPC requests are no longer logged (only affects internal API calls). #2309
* [CHANGE] Add new `-*.consul.cas-retry-delay` flags. They have a default value of `1s`, while previously there was no delay between retries. #2309
* [CHANGE] Store-gateway: Remove the experimental ability to run requests in a dedicated OS thread pool and associated CLI flag `-store-gateway.thread-pool-size`. #2423
* [CHANGE] Memberlist: disabled TCP-based ping fallback, because Mimir already uses a custom transport based on TCP. #2456
* [CHANGE] Change default value for `-distributor.ha-tracker.max-clusters` to `100` to provide a DoS protection. #2465
* [CHANGE] Experimental block upload API exposed by compactor has changed: Previous `/api/v1/upload/block/{block}` endpoint for starting block upload is now `/api/v1/upload/block/{block}/start`, and previous endpoint `/api/v1/upload/block/{block}?uploadComplete=true` for finishing block upload is now `/api/v1/upload/block/{block}/finish`. New API endpoint has been added: `/api/v1/upload/block/{block}/check`. #2486 #2548
* [CHANGE] Compactor: changed `-compactor.max-compaction-time` default from `0s` (disabled) to `1h`. When compacting blocks for a tenant, the compactor will move to compact blocks of another tenant or re-plan blocks to compact at least every 1h. #2514
* [CHANGE] Distributor: removed previously deprecated `extend_writes` (see #1856) YAML key and `-distributor.extend-writes` CLI flag from the distributor config. #2551
* [CHANGE] Ingester: removed previously deprecated `active_series_custom_trackers` (see #1188) YAML key from the ingester config. #2552
* [FEATURE] Compactor: Adds the ability to delete partial blocks after a configurable delay. This option can be configured per tenant. #2285
  - `-compactor.partial-block-deletion-delay`, as a duration string, allows you to set the delay since a partial block has been modified before marking it for deletion. A value of `0`, the default, disables this feature.
  - The metric `cortex_compactor_blocks_marked_for_deletion_total` has a new value for the `reason` label `reason="partial"`, when a block deletion marker is triggered by the partial block deletion delay.
* [FEATURE] Querier: enabled support for queries with negative offsets, which are not cached in the query results cache. #2429
* [FEATURE] EXPERIMENTAL: OpenTelemetry Metrics ingestion path on `/otlp/v1/metrics`. #695 #2436
* [FEATURE] Querier: Added support for tenant federation to metric metadata endpoint. #2467
* [ENHANCEMENT] Distributor: Decreased distributor tests execution time. #2562
* [ENHANCEMENT] Alertmanager: Allow the HTTP `proxy_url` configuration option in the receiver's configuration. #2317
* [ENHANCEMENT] ring: optimize shuffle-shard computation when lookback is used, and all instances have registered timestamp within the lookback window. In that case we can immediately return origial ring, because we would select all instances anyway. #2309
* [ENHANCEMENT] Memberlist: added experimental memberlist cluster label support via `-memberlist.cluster-label` and `-memberlist.cluster-label-verification-disabled` CLI flags (and their respective YAML config options). #2354
* [ENHANCEMENT] Object storage can now be configured for all components using the `common` YAML config option key (or `-common.storage.*` CLI flags). #2330
* [ENHANCEMENT] Go: updated to go 1.18.4. #2400
* [ENHANCEMENT] Store-gateway, listblocks: list of blocks now includes stats from `meta.json` file: number of series, samples and chunks. #2425
* [ENHANCEMENT] Added more buckets to `cortex_ingester_client_request_duration_seconds` histogram metric, to correctly track requests taking longer than 1s (up until 16s). #2445
* [ENHANCEMENT] Azure client: Improve memory usage for large object storage downloads. #2408
* [ENHANCEMENT] Distributor: Add `-distributor.instance-limits.max-inflight-push-requests-bytes`. This limit protects the distributor against multiple large requests that together may cause an OOM, but are only a few, so do not trigger the `max-inflight-push-requests` limit. #2413
* [ENHANCEMENT] Distributor: Drop exemplars in distributor for tenants where exemplars are disabled. #2504
* [BUGFIX] Compactor: log the actual error on compaction failed. #2261
* [BUGFIX] Alertmanager: restore state from storage even when running a single replica. #2293
* [BUGFIX] Ruler: do not block "List Prometheus rules" API endpoint while syncing rules. #2289
* [BUGFIX] Ruler: return proper `*status.Status` error when running in remote operational mode. #2417
* [BUGFIX] Alertmanager: ensure the configured `-alertmanager.web.external-url` is either a path starting with `/`, or a full URL including the scheme and hostname. #2381 #2542
* [BUGFIX] Memberlist: fix problem with loss of some packets, typically ring updates when instances were removed from the ring during shutdown. #2418
* [BUGFIX] Ingester: fix misfiring `MimirIngesterHasUnshippedBlocks` and stale `cortex_ingester_oldest_unshipped_block_timestamp_seconds` when some block uploads fail. #2435
* [BUGFIX] Query-frontend: fix incorrect mapping of http status codes 429 to 500 when request queue is full. #2447
* [BUGFIX] Memberlist: Fix problem with ring being empty right after startup. Memberlist KV store now tries to "fast-join" the cluster to avoid serving empty KV store. #2505
* [BUGFIX] Compactor: Fix bug when using `-compactor.partial-block-deletion-delay`: compactor didn't correctly check for modification time of all block files. #2559
* [BUGFIX] Query-frontend: fix wrong query sharding results for queries with boolean result like `1 < bool 0`. #2558
* [BUGFIX] Fixed error messages related to per-instance limits incorrectly reporting they can be set on a per-tenant basis. #2610

### Mixin

* [CHANGE] Dashboards: "Slow Queries" dashboard no longer works with versions older than Grafana 9.0. #2223
* [CHANGE] Alerts: use RSS memory instead of working set memory in the `MimirAllocatingTooMuchMemory` alert for ingesters. #2480
* [ENHANCEMENT] Dashboards: added missed rule evaluations to the "Evaluations per second" panel in the "Mimir / Ruler" dashboard. #2314
* [ENHANCEMENT] Dashboards: add k8s resource requests to CPU and memory panels. #2346
* [ENHANCEMENT] Dashboards: add RSS memory utilization panel for ingesters, store-gateways and compactors. #2479
* [BUGFIX] Dashboards: fixed unit of latency panels in the "Mimir / Ruler" dashboard. #2312
* [BUGFIX] Dashboards: fixed "Intervals per query" panel in the "Mimir / Queries" dashboard. #2308
* [BUGFIX] Dashboards: Make "Slow Queries" dashboard works with Grafana 9.0. #2223
* [BUGFIX] Dashboards: add missing API routes to Ruler dashboard. #2412

### Jsonnet

* [CHANGE] query-scheduler is enabled by default. We advise to deploy the query-scheduler to improve the scalability of the query-frontend. #2431
* [CHANGE] Replaced anti-affinity rules with pod topology spread constraints for distributor, query-frontend, querier and ruler.
  - The following configuration options have been removed:
    - `distributor_allow_multiple_replicas_on_same_node`
    - `query_frontend_allow_multiple_replicas_on_same_node`
    - `querier_allow_multiple_replicas_on_same_node`
    - `ruler_allow_multiple_replicas_on_same_node`
  - The following configuration options have been added:
    - `distributor_topology_spread_max_skew`
    - `query_frontend_topology_spread_max_skew`
    - `querier_topology_spread_max_skew`
    - `ruler_topology_spread_max_skew`
* [FEATURE] Memberlist: added support for experimental memberlist cluster label, through the jsonnet configuration options `memberlist_cluster_label` and `memberlist_cluster_label_verification_disabled`. #2349
* [FEATURE] Added ruler-querier autoscaling support. It requires [KEDA](https://keda.sh) installed in the Kubernetes cluster. Ruler-querier autoscaler can be enabled and configure through the following options in the jsonnet config: #2545
  * `autoscaling_ruler_querier_enabled`: `true` to enable autoscaling.
  * `autoscaling_ruler_querier_min_replicas`: minimum number of ruler-querier replicas.
  * `autoscaling_ruler_querier_max_replicas`: maximum number of ruler-querier replicas.
  * `autoscaling_prometheus_url`: Prometheus base URL from which to scrape Mimir metrics (e.g. `http://prometheus.default:9090/prometheus`).
* [ENHANCEMENT] Memberlist now uses DNS service-discovery by default. #2549

### Mimirtool

* [ENHANCEMENT] Added `mimirtool backfill` command to upload Prometheus blocks using API available in the compactor. #1822
* [ENHANCEMENT] mimirtool bucket-validation: Verify existing objects can be overwritten by subsequent uploads. #2491
* [BUGFIX] mimirtool analyze: Fix dashboard JSON unmarshalling errors by using custom parsing. #2386

### Mimir Continuous Test

### Documentation

* [ENHANCEMENT] Referenced `mimirtool` commands in the HTTP API documentation. #2516
* [ENHANCEMENT] Improved DNS service discovery documentation. #2513

## 2.2.0

### Grafana Mimir

* [CHANGE] Increased default configuration for `-server.grpc-max-recv-msg-size-bytes` and `-server.grpc-max-send-msg-size-bytes` from 4MB to 100MB. #1884
* [CHANGE] Default values have changed for the following settings. This improves query performance for recent data (within 12h) by only reading from ingesters: #1909 #1921
    - `-blocks-storage.bucket-store.ignore-blocks-within` now defaults to `10h` (previously `0`)
    - `-querier.query-store-after` now defaults to `12h` (previously `0`)
* [CHANGE] Alertmanager: removed support for migrating local files from Cortex 1.8 or earlier. Related to original Cortex PR https://github.com/cortexproject/cortex/pull/3910. #2253
* [CHANGE] The following settings are now classified as advanced because the defaults should work for most users and tuning them requires in-depth knowledge of how the read path works: #1929
    - `-querier.query-ingesters-within`
    - `-querier.query-store-after`
* [CHANGE] Config flag category overrides can be set dynamically at runtime. #1934
* [CHANGE] Ingester: deprecated `-ingester.ring.join-after`. Mimir now behaves as this setting is always set to 0s. This configuration option will be removed in Mimir 2.4.0. #1965
* [CHANGE] Blocks uploaded by ingester no longer contain `__org_id__` label. Compactor now ignores this label and will compact blocks with and without this label together. `mimirconvert` tool will remove the label from blocks as "unknown" label. #1972
* [CHANGE] Querier: deprecated `-querier.shuffle-sharding-ingesters-lookback-period`, instead adding `-querier.shuffle-sharding-ingesters-enabled` to enable or disable shuffle sharding on the read path. The value of `-querier.query-ingesters-within` is now used internally for shuffle sharding lookback. #2110
* [CHANGE] Memberlist: `-memberlist.abort-if-join-fails` now defaults to false. Previously it defaulted to true. #2168
* [CHANGE] Ruler: `/api/v1/rules*` and `/prometheus/rules*` configuration endpoints are removed. Use `/prometheus/config/v1/rules*`. #2182
* [CHANGE] Ingester: `-ingester.exemplars-update-period` has been renamed to `-ingester.tsdb-config-update-period`. You can use it to update multiple, per-tenant TSDB configurations. #2187
* [FEATURE] Ingester: (Experimental) Add the ability to ingest out-of-order samples up to an allowed limit. If you enable this feature, it requires additional memory and disk space. This feature also enables a write-behind log, which might lead to longer ingester-start replays. When this feature is disabled, there is no overhead on memory, disk space, or startup times. #2187
  * `-ingester.out-of-order-time-window`, as duration string, allows you to set how back in time a sample can be. The default is `0s`, where `s` is seconds.
  * `cortex_ingester_tsdb_out_of_order_samples_appended_total` metric tracks the total number of out-of-order samples ingested by the ingester.
  * `cortex_discarded_samples_total` has a new label `reason="sample-too-old"`, when the `-ingester.out-of-order-time-window` flag is greater than zero. The label tracks the number of samples that were discarded for being too old; they were out of order, but beyond the time window allowed. The labels `reason="sample-out-of-order"` and `reason="sample-out-of-bounds"` are not used when out-of-order ingestion is enabled.
* [ENHANCEMENT] Distributor: Added limit to prevent tenants from sending excessive number of requests: #1843
  * The following CLI flags (and their respective YAML config options) have been added:
    * `-distributor.request-rate-limit`
    * `-distributor.request-burst-limit`
  * The following metric is exposed to tell how many requests have been rejected:
    * `cortex_discarded_requests_total`
* [ENHANCEMENT] Store-gateway: Add the experimental ability to run requests in a dedicated OS thread pool. This feature can be configured using `-store-gateway.thread-pool-size` and is disabled by default. Replaces the ability to run index header operations in a dedicated thread pool. #1660 #1812
* [ENHANCEMENT] Improved error messages to make them easier to understand; each now have a unique, global identifier that you can use to look up in the runbooks for more information. #1907 #1919 #1888 #1939 #1984 #2009 #2056 #2066 #2104 #2150 #2234
* [ENHANCEMENT] Memberlist KV: incoming messages are now processed on per-key goroutine. This may reduce loss of "maintanance" packets in busy memberlist installations, but use more CPU. New `memberlist_client_received_broadcasts_dropped_total` counter tracks number of dropped per-key messages. #1912
* [ENHANCEMENT] Blocks Storage, Alertmanager, Ruler: add support a prefix to the bucket store (`*_storage.storage_prefix`). This enables using the same bucket for the three components. #1686 #1951
* [ENHANCEMENT] Upgrade Docker base images to `alpine:3.16.0`. #2028
* [ENHANCEMENT] Store-gateway: Add experimental configuration option for the store-gateway to attempt to pre-populate the file system cache when memory-mapping index-header files. Enabled with `-blocks-storage.bucket-store.index-header.map-populate-enabled=true`. Note this flag only has an effect when running on Linux. #2019 #2054
* [ENHANCEMENT] Chunk Mapper: reduce memory usage of async chunk mapper. #2043
* [ENHANCEMENT] Ingester: reduce sleep time when reading WAL. #2098
* [ENHANCEMENT] Compactor: Run sanity check on blocks storage configuration at startup. #2144
* [ENHANCEMENT] Compactor: Add HTTP API for uploading TSDB blocks. Enabled with `-compactor.block-upload-enabled`. #1694 #2126
* [ENHANCEMENT] Ingester: Enable querying overlapping blocks by default. #2187
* [ENHANCEMENT] Distributor: Auto-forget unhealthy distributors after ten failed ring heartbeats. #2154
* [ENHANCEMENT] Distributor: Add new metric `cortex_distributor_forward_errors_total` for error codes resulting from forwarding requests. #2077
* [ENHANCEMENT] `/ready` endpoint now returns and logs detailed services information. #2055
* [ENHANCEMENT] Memcached client: Reduce number of connections required to fetch cached keys from memcached. #1920
* [ENHANCEMENT] Improved error message returned when `-querier.query-store-after` validation fails. #1914
* [BUGFIX] Fix regexp parsing panic for regexp label matchers with start/end quantifiers. #1883
* [BUGFIX] Ingester: fixed deceiving error log "failed to update cached shipped blocks after shipper initialisation", occurring for each new tenant in the ingester. #1893
* [BUGFIX] Ring: fix bug where instances may appear unhealthy in the hash ring web UI even though they are not. #1933
* [BUGFIX] API: gzip is now enforced when identity encoding is explicitly rejected. #1864
* [BUGFIX] Fix panic at startup when Mimir is running in monolithic mode and query sharding is enabled. #2036
* [BUGFIX] Ruler: report `cortex_ruler_queries_failed_total` metric for any remote query error except 4xx when remote operational mode is enabled. #2053 #2143
* [BUGFIX] Ingester: fix slow rollout when using `-ingester.ring.unregister-on-shutdown=false` with long `-ingester.ring.heartbeat-period`. #2085
* [BUGFIX] Ruler: add timeout for remote rule evaluation queries to prevent rule group evaluations getting stuck indefinitely. The duration is configurable with `-querier.timeout` (default `2m`). #2090 #2222
* [BUGFIX] Limits: Active series custom tracker configuration has been named back from `active_series_custom_trackers_config` to `active_series_custom_trackers`. For backwards compatibility both version is going to be supported for until Mimir v2.4. When both fields are specified, `active_series_custom_trackers_config` takes precedence over `active_series_custom_trackers`. #2101
* [BUGFIX] Ingester: fixed the order of labels applied when incrementing the `cortex_discarded_metadata_total` metric. #2096
* [BUGFIX] Ingester: fixed bug where retrieving metadata for a metric with multiple metadata entries would return multiple copies of a single metadata entry rather than all available entries. #2096
* [BUGFIX] Distributor: canceled requests are no longer accounted as internal errors. #2157
* [BUGFIX] Memberlist: Fix typo in memberlist admin UI. #2202
* [BUGFIX] Ruler: fixed typo in error message when ruler failed to decode a rule group. #2151
* [BUGFIX] Active series custom tracker configuration is now displayed properly on `/runtime_config` page. #2065
* [BUGFIX] Query-frontend: `vector` and `time` functions were sharded, which made expressions like `vector(1) > 0 and vector(1)` fail. #2355

### Mixin

* [CHANGE] Split `mimir_queries` rules group into `mimir_queries` and `mimir_ingester_queries` to keep number of rules per group within the default per-tenant limit. #1885
* [CHANGE] Dashboards: Expose full image tag in "Mimir / Rollout progress" dashboard's "Pod per version panel." #1932
* [CHANGE] Dashboards: Disabled gateway panels by default, because most users don't have a gateway exposing the metrics expected by Mimir dashboards. You can re-enable it setting `gateway_enabled: true` in the mixin config and recompiling the mixin running `make build-mixin`. #1955
* [CHANGE] Alerts: adapt `MimirFrontendQueriesStuck` and `MimirSchedulerQueriesStuck` to consider ruler query path components. #1949
* [CHANGE] Alerts: Change `MimirRulerTooManyFailedQueries` severity to `critical`. #2165
* [ENHANCEMENT] Dashboards: Add config option `datasource_regex` to customise the regular expression used to select valid datasources for Mimir dashboards. #1802
* [ENHANCEMENT] Dashboards: Added "Mimir / Remote ruler reads" and "Mimir / Remote ruler reads resources" dashboards. #1911 #1937
* [ENHANCEMENT] Dashboards: Make networking panels work for pods created by the mimir-distributed helm chart. #1927
* [ENHANCEMENT] Alerts: Add `MimirStoreGatewayNoSyncedTenants` alert that fires when there is a store-gateway owning no tenants. #1882
* [ENHANCEMENT] Rules: Make `recording_rules_range_interval` configurable for cases where Mimir metrics are scraped less often that every 30 seconds. #2118
* [ENHANCEMENT] Added minimum Grafana version to mixin dashboards. #1943
* [BUGFIX] Fix `container_memory_usage_bytes:sum` recording rule. #1865
* [BUGFIX] Fix `MimirGossipMembersMismatch` alerts if Mimir alertmanager is activated. #1870
* [BUGFIX] Fix `MimirRulerMissedEvaluations` to show % of missed alerts as a value between 0 and 100 instead of 0 and 1. #1895
* [BUGFIX] Fix `MimirCompactorHasNotUploadedBlocks` alert false positive when Mimir is deployed in monolithic mode. #1902
* [BUGFIX] Fix `MimirGossipMembersMismatch` to make it less sensitive during rollouts and fire one alert per installation, not per job. #1926
* [BUGFIX] Do not trigger `MimirAllocatingTooMuchMemory` alerts if no container limits are supplied. #1905
* [BUGFIX] Dashboards: Remove empty "Chunks per query" panel from `Mimir / Queries` dashboard. #1928
* [BUGFIX] Dashboards: Use Grafana's `$__rate_interval` for rate queries in dashboards to support scrape intervals of >15s. #2011
* [BUGFIX] Alerts: Make each version of `MimirCompactorHasNotUploadedBlocks` distinct to avoid rule evaluation failures due to duplicate series being generated. #2197
* [BUGFIX] Fix `MimirGossipMembersMismatch` alert when using remote ruler evaluation. #2159

### Jsonnet

* [CHANGE] Remove use of `-querier.query-store-after`, `-querier.shuffle-sharding-ingesters-lookback-period`, `-blocks-storage.bucket-store.ignore-blocks-within`, and `-blocks-storage.tsdb.close-idle-tsdb-timeout` CLI flags since the values now match defaults. #1915 #1921
* [CHANGE] Change default value for `-blocks-storage.bucket-store.chunks-cache.memcached.timeout` to `450ms` to increase use of cached data. #2035
* [CHANGE] The `memberlist_ring_enabled` configuration now applies to Alertmanager. #2102 #2103 #2107
* [CHANGE] Default value for `memberlist_ring_enabled` is now true. It means that all hash rings use Memberlist as default KV store instead of Consul (previous default). #2161
* [CHANGE] Configure `-ingester.max-global-metadata-per-user` to correspond to 20% of the configured max number of series per tenant. #2250
* [CHANGE] Configure `-ingester.max-global-metadata-per-metric` to be 10. #2250
* [CHANGE] Change `_config.multi_zone_ingester_max_unavailable` to 25. #2251
* [FEATURE] Added querier autoscaling support. It requires [KEDA](https://keda.sh) installed in the Kubernetes cluster and query-scheduler enabled in the Mimir cluster. Querier autoscaler can be enabled and configure through the following options in the jsonnet config: #2013 #2023
  * `autoscaling_querier_enabled`: `true` to enable autoscaling.
  * `autoscaling_querier_min_replicas`: minimum number of querier replicas.
  * `autoscaling_querier_max_replicas`: maximum number of querier replicas.
  * `autoscaling_prometheus_url`: Prometheus base URL from which to scrape Mimir metrics (e.g. `http://prometheus.default:9090/prometheus`).
* [FEATURE] Jsonnet: Add support for ruler remote evaluation mode (`ruler_remote_evaluation_enabled`), which deploys and uses a dedicated query path for rule evaluation. This enables the benefits of the query-frontend for rule evaluation, such as query sharding. #2073
* [ENHANCEMENT] Added `compactor` service, that can be used to route requests directly to compactor (e.g. admin UI). #2063
* [ENHANCEMENT] Added a `consul_enabled` configuration option to provide the ability to disable consul. It is automatically set to false when `memberlist_ring_enabled` is true and `multikv_migration_enabled` (used for migration from Consul to memberlist) is not set. #2093 #2152
* [BUGFIX] Querier: Fix disabling shuffle sharding on the read path whilst keeping it enabled on write path. #2164

### Mimirtool

* [CHANGE] mimirtool rules: `--use-legacy-routes` now toggles between using `/prometheus/config/v1/rules` (default) and `/api/v1/rules` (legacy) endpoints. #2182
* [FEATURE] Added bearer token support for when Mimir is behind a gateway authenticating by bearer token. #2146
* [BUGFIX] mimirtool analyze: Fix dashboard JSON unmarshalling errors (#1840). #1973
* [BUGFIX] Make mimirtool build for Windows work again. #2273

### Mimir Continuous Test

* [ENHANCEMENT] Added the `-tests.smoke-test` flag to run the `mimir-continuous-test` suite once and immediately exit. #2047 #2094

### Documentation

* [ENHANCEMENT] Published Grafana Mimir runbooks as part of documentation. #1970
* [ENHANCEMENT] Improved ruler's "remote operational mode" documentation. #1906
* [ENHANCEMENT] Recommend fast disks for ingesters and store-gateways in production tips. #1903
* [ENHANCEMENT] Explain the runtime override of active series matchers. #1868
* [ENHANCEMENT] Clarify "Set rule group" API specification. #1869
* [ENHANCEMENT] Published Mimir jsonnet documentation. #2024
* [ENHANCEMENT] Documented required scrape interval for using alerting and recording rules from Mimir jsonnet. #2147
* [ENHANCEMENT] Runbooks: Mention memberlist as possible source of problems for various alerts. #2158
* [ENHANCEMENT] Added step-by-step article about migrating from Consul to Memberlist KV store using jsonnet without downtime. #2166
* [ENHANCEMENT] Documented `/memberlist` admin page. #2166
* [ENHANCEMENT] Documented how to configure Grafana Mimir's ruler with Jsonnet. #2127
* [ENHANCEMENT] Documented how to configure queriers’ autoscaling with Jsonnet. #2128
* [ENHANCEMENT] Updated mixin building instructions in "Installing Grafana Mimir dashboards and alerts" article. #2015 #2163
* [ENHANCEMENT] Fix location of "Monitoring Grafana Mimir" article in the documentation hierarchy. #2130
* [ENHANCEMENT] Runbook for `MimirRequestLatency` was expanded with more practical advice. #1967
* [BUGFIX] Fixed ruler configuration used in the getting started guide. #2052
* [BUGFIX] Fixed Mimir Alertmanager datasource in Grafana used by "Play with Grafana Mimir" tutorial. #2115
* [BUGFIX] Fixed typos in "Scaling out Grafana Mimir" article. #2170
* [BUGFIX] Added missing ring endpoint exposed by Ingesters. #1918

## 2.1.0

### Grafana Mimir

* [CHANGE] Compactor: No longer upload debug meta files to object storage. #1257
* [CHANGE] Default values have changed for the following settings: #1547
    - `-alertmanager.alertmanager-client.grpc-max-recv-msg-size` now defaults to 100 MiB (previously was not configurable and set to 16 MiB)
    - `-alertmanager.alertmanager-client.grpc-max-send-msg-size` now defaults to 100 MiB (previously was not configurable and set to 4 MiB)
    - `-alertmanager.max-recv-msg-size` now defaults to 100 MiB (previously was 16 MiB)
* [CHANGE] Ingester: Add `user` label to metrics `cortex_ingester_ingested_samples_total` and `cortex_ingester_ingested_samples_failures_total`. #1533
* [CHANGE] Ingester: Changed `-blocks-storage.tsdb.isolation-enabled` default from `true` to `false`. The config option has also been deprecated and will be removed in 2 minor version. #1655
* [CHANGE] Query-frontend: results cache keys are now versioned, this will cause cache to be re-filled when rolling out this version. #1631
* [CHANGE] Store-gateway: enabled attributes in-memory cache by default. New default configuration is `-blocks-storage.bucket-store.chunks-cache.attributes-in-memory-max-items=50000`. #1727
* [CHANGE] Compactor: Removed the metric `cortex_compactor_garbage_collected_blocks_total` since it duplicates `cortex_compactor_blocks_marked_for_deletion_total`. #1728
* [CHANGE] All: Logs that used the`org_id` label now use `user` label. #1634 #1758
* [CHANGE] Alertmanager: the following metrics are not exported for a given `user` and `integration` when the metric value is zero: #1783
  * `cortex_alertmanager_notifications_total`
  * `cortex_alertmanager_notifications_failed_total`
  * `cortex_alertmanager_notification_requests_total`
  * `cortex_alertmanager_notification_requests_failed_total`
  * `cortex_alertmanager_notification_rate_limited_total`
* [CHANGE] Removed the following metrics exposed by the Mimir hash rings: #1791
  * `cortex_member_ring_tokens_owned`
  * `cortex_member_ring_tokens_to_own`
  * `cortex_ring_tokens_owned`
  * `cortex_ring_member_ownership_percent`
* [CHANGE] Querier / Ruler: removed the following metrics tracking number of query requests send to each ingester. You can use `cortex_request_duration_seconds_count{route=~"/cortex.Ingester/(QueryStream|QueryExemplars)"}` instead. #1797
  * `cortex_distributor_ingester_queries_total`
  * `cortex_distributor_ingester_query_failures_total`
* [CHANGE] Distributor: removed the following metrics tracking the number of requests from a distributor to ingesters: #1799
  * `cortex_distributor_ingester_appends_total`
  * `cortex_distributor_ingester_append_failures_total`
* [CHANGE] Distributor / Ruler: deprecated `-distributor.extend-writes`. Now Mimir always behaves as if this setting was set to `false`, which we expect to be safe for every Mimir cluster setup. #1856
* [FEATURE] Querier: Added support for [streaming remote read](https://prometheus.io/blog/2019/10/10/remote-read-meets-streaming/). Should be noted that benefits of chunking the response are partial here, since in a typical `query-frontend` setup responses will be buffered until they've been completed. #1735
* [FEATURE] Ruler: Allow setting `evaluation_delay` for each rule group via rules group configuration file. #1474
* [FEATURE] Ruler: Added support for expression remote evaluation. #1536 #1818
  * The following CLI flags (and their respective YAML config options) have been added:
    * `-ruler.query-frontend.address`
    * `-ruler.query-frontend.grpc-client-config.grpc-max-recv-msg-size`
    * `-ruler.query-frontend.grpc-client-config.grpc-max-send-msg-size`
    * `-ruler.query-frontend.grpc-client-config.grpc-compression`
    * `-ruler.query-frontend.grpc-client-config.grpc-client-rate-limit`
    * `-ruler.query-frontend.grpc-client-config.grpc-client-rate-limit-burst`
    * `-ruler.query-frontend.grpc-client-config.backoff-on-ratelimits`
    * `-ruler.query-frontend.grpc-client-config.backoff-min-period`
    * `-ruler.query-frontend.grpc-client-config.backoff-max-period`
    * `-ruler.query-frontend.grpc-client-config.backoff-retries`
    * `-ruler.query-frontend.grpc-client-config.tls-enabled`
    * `-ruler.query-frontend.grpc-client-config.tls-ca-path`
    * `-ruler.query-frontend.grpc-client-config.tls-cert-path`
    * `-ruler.query-frontend.grpc-client-config.tls-key-path`
    * `-ruler.query-frontend.grpc-client-config.tls-server-name`
    * `-ruler.query-frontend.grpc-client-config.tls-insecure-skip-verify`
* [FEATURE] Distributor: Added the ability to forward specifics metrics to alternative remote_write API endpoints. #1052
* [FEATURE] Ingester: Active series custom trackers now supports runtime tenant-specific overrides. The configuration has been moved to limit config, the ingester config has been deprecated.  #1188
* [ENHANCEMENT] Alertmanager API: Concurrency limit for GET requests is now configurable using `-alertmanager.max-concurrent-get-requests-per-tenant`. #1547
* [ENHANCEMENT] Alertmanager: Added the ability to configure additional gRPC client settings for the Alertmanager distributor #1547
  - `-alertmanager.alertmanager-client.backoff-max-period`
  - `-alertmanager.alertmanager-client.backoff-min-period`
  - `-alertmanager.alertmanager-client.backoff-on-ratelimits`
  - `-alertmanager.alertmanager-client.backoff-retries`
  - `-alertmanager.alertmanager-client.grpc-client-rate-limit`
  - `-alertmanager.alertmanager-client.grpc-client-rate-limit-burst`
  - `-alertmanager.alertmanager-client.grpc-compression`
  - `-alertmanager.alertmanager-client.grpc-max-recv-msg-size`
  - `-alertmanager.alertmanager-client.grpc-max-send-msg-size`
* [ENHANCEMENT] Ruler: Add more detailed query information to ruler query stats logging. #1411
* [ENHANCEMENT] Admin: Admin API now has some styling. #1482 #1549 #1821 #1824
* [ENHANCEMENT] Alertmanager: added `insight=true` field to alertmanager dispatch logs. #1379
* [ENHANCEMENT] Store-gateway: Add the experimental ability to run index header operations in a dedicated thread pool. This feature can be configured using `-blocks-storage.bucket-store.index-header-thread-pool-size` and is disabled by default. #1660
* [ENHANCEMENT] Store-gateway: don't drop all blocks if instance finds itself as unhealthy or missing in the ring. #1806 #1823
* [ENHANCEMENT] Querier: wait until inflight queries are completed when shutting down queriers. #1756 #1767
* [BUGFIX] Query-frontend: do not shard queries with a subquery unless the subquery is inside a shardable aggregation function call. #1542
* [BUGFIX] Query-frontend: added `component=query-frontend` label to results cache memcached metrics to fix a panic when Mimir is running in single binary mode and results cache is enabled. #1704
* [BUGFIX] Mimir: services' status content-type is now correctly set to `text/html`. #1575
* [BUGFIX] Multikv: Fix panic when using using runtime config to set primary KV store used by `multi` KV. #1587
* [BUGFIX] Multikv: Fix watching for runtime config changes in `multi` KV store in ruler and querier. #1665
* [BUGFIX] Memcached: allow to use CNAME DNS records for the memcached backend addresses. #1654
* [BUGFIX] Querier: fixed temporary partial query results when shuffle sharding is enabled and hash ring backend storage is flushed / reset. #1829
* [BUGFIX] Alertmanager: prevent more file traversal cases related to template names. #1833
* [BUGFUX] Alertmanager: Allow usage with `-alertmanager-storage.backend=local`. Note that when using this storage type, the Alertmanager is not able persist state remotely, so it not recommended for production use. #1836
* [BUGFIX] Alertmanager: Do not validate alertmanager configuration if it's not running. #1835

### Mixin

* [CHANGE] Dashboards: Remove per-user series legends from Tenants dashboard. #1605
* [CHANGE] Dashboards: Show in-memory series and the per-user series limit on Tenants dashboard. #1613
* [CHANGE] Dashboards: Slow-queries dashboard now uses `user` label from logs instead of `org_id`. #1634
* [CHANGE] Dashboards: changed all Grafana dashboards UIDs to not conflict with Cortex ones, to let people install both while migrating from Cortex to Mimir: #1801 #1808
  * Alertmanager from `a76bee5913c97c918d9e56a3cc88cc28` to `b0d38d318bbddd80476246d4930f9e55`
  * Alertmanager Resources from `68b66aed90ccab448009089544a8d6c6` to `a6883fb22799ac74479c7db872451092`
  * Compactor from `9c408e1d55681ecb8a22c9fab46875cc` to `1b3443aea86db629e6efdb7d05c53823`
  * Compactor Resources from `df9added6f1f4332f95848cca48ebd99` to `09a5c49e9cdb2f2b24c6d184574a07fd`
  * Config from `61bb048ced9817b2d3e07677fb1c6290` to `5d9d0b4724c0f80d68467088ec61e003`
  * Object Store from `d5a3a4489d57c733b5677fb55370a723` to `e1324ee2a434f4158c00a9ee279d3292`
  * Overrides from `b5c95fee2e5e7c4b5930826ff6e89a12` to `1e2c358600ac53f09faea133f811b5bb`
  * Queries from `d9931b1054053c8b972d320774bb8f1d` to `b3abe8d5c040395cc36615cb4334c92d`
  * Reads from `8d6ba60eccc4b6eedfa329b24b1bd339` to `e327503188913dc38ad571c647eef643`
  * Reads Networking from `c0464f0d8bd026f776c9006b05910000` to `54b2a0a4748b3bd1aefa92ce5559a1c2`
  * Reads Resources from `2fd2cda9eea8d8af9fbc0a5960425120` to `cc86fd5aa9301c6528986572ad974db9`
  * Rollout Progress from `7544a3a62b1be6ffd919fc990ab8ba8f` to `7f0b5567d543a1698e695b530eb7f5de`
  * Ruler from `44d12bcb1f95661c6ab6bc946dfc3473` to `631e15d5d85afb2ca8e35d62984eeaa0`
  * Scaling from `88c041017b96856c9176e07cf557bdcf` to `64bbad83507b7289b514725658e10352`
  * Slow queries from `e6f3091e29d2636e3b8393447e925668` to `6089e1ce1e678788f46312a0a1e647e6`
  * Tenants from `35fa247ce651ba189debf33d7ae41611` to `35fa247ce651ba189debf33d7ae41611`
  * Top Tenants from `bc6e12d4fe540e4a1785b9d3ca0ffdd9` to `bc6e12d4fe540e4a1785b9d3ca0ffdd9`
  * Writes from `0156f6d15aa234d452a33a4f13c838e3` to `8280707b8f16e7b87b840fc1cc92d4c5`
  * Writes Networking from `681cd62b680b7154811fe73af55dcfd4` to `978c1cb452585c96697a238eaac7fe2d`
  * Writes Resources from `c0464f0d8bd026f776c9006b0591bb0b` to `bc9160e50b52e89e0e49c840fea3d379`
* [FEATURE] Alerts: added the following alerts on `mimir-continuous-test` tool: #1676
  - `MimirContinuousTestNotRunningOnWrites`
  - `MimirContinuousTestNotRunningOnReads`
  - `MimirContinuousTestFailed`
* [ENHANCEMENT] Added `per_cluster_label` support to allow to change the label name used to differentiate between Kubernetes clusters. #1651
* [ENHANCEMENT] Dashboards: Show QPS and latency of the Alertmanager Distributor. #1696
* [ENHANCEMENT] Playbooks: Add Alertmanager suggestions for `MimirRequestErrors` and `MimirRequestLatency` #1702
* [ENHANCEMENT] Dashboards: Allow custom datasources. #1749
* [ENHANCEMENT] Dashboards: Add config option `gateway_enabled` (defaults to `true`) to disable gateway panels from dashboards. #1761
* [ENHANCEMENT] Dashboards: Extend Top tenants dashboard with queries for tenants with highest sample rate, discard rate, and discard rate growth. #1842
* [ENHANCEMENT] Dashboards: Show ingestion rate limit and rule group limit on Tenants dashboard. #1845
* [ENHANCEMENT] Dashboards: Add "last successful run" panel to compactor dashboard. #1628
* [BUGFIX] Dashboards: Fix "Failed evaluation rate" panel on Tenants dashboard. #1629
* [BUGFIX] Honor the configured `per_instance_label` in all dashboards and alerts. #1697

### Jsonnet

* [FEATURE] Added support for `mimir-continuous-test`. To deploy `mimir-continuous-test` you can use the following configuration: #1675 #1850
  ```jsonnet
  _config+: {
    continuous_test_enabled: true,
    continuous_test_tenant_id: 'type-tenant-id',
    continuous_test_write_endpoint: 'http://type-write-path-hostname',
    continuous_test_read_endpoint: 'http://type-read-path-hostname/prometheus',
  },
  ```
* [ENHANCEMENT] Ingester anti-affinity can now be disabled by using `ingester_allow_multiple_replicas_on_same_node` configuration key. #1581
* [ENHANCEMENT] Added `node_selector` configuration option to select Kubernetes nodes where Mimir should run. #1596
* [ENHANCEMENT] Alertmanager: Added a `PodDisruptionBudget` of `withMaxUnavailable = 1`, to ensure we maintain quorum during rollouts. #1683
* [ENHANCEMENT] Store-gateway anti-affinity can now be enabled/disabled using `store_gateway_allow_multiple_replicas_on_same_node` configuration key. #1730
* [ENHANCEMENT] Added `store_gateway_zone_a_args`, `store_gateway_zone_b_args` and `store_gateway_zone_c_args` configuration options. #1807
* [BUGFIX] Pass primary and secondary multikv stores via CLI flags. Introduced new `multikv_switch_primary_secondary` config option to flip primary and secondary in runtime config.

### Mimirtool

* [BUGFIX] `config convert`: Retain Cortex defaults for `blocks_storage.backend`, `ruler_storage.backend`, `alertmanager_storage.backend`, `auth.type`, `activity_tracker.filepath`, `alertmanager.data_dir`, `blocks_storage.filesystem.dir`, `compactor.data_dir`, `ruler.rule_path`, `ruler_storage.filesystem.dir`, and `graphite.querier.schemas.backend`. #1626 #1762

### Tools

* [FEATURE] Added a `markblocks` tool that creates `no-compact` and `delete` marks for the blocks. #1551
* [FEATURE] Added `mimir-continuous-test` tool to continuously run smoke tests on live Mimir clusters. #1535 #1540 #1653 #1603 #1630 #1691 #1675 #1676 #1692 #1706 #1709 #1775 #1777 #1778 #1795
* [FEATURE] Added `mimir-rules-action` GitHub action, located at `operations/mimir-rules-action/`, used to lint, prepare, verify, diff, and sync rules to a Mimir cluster. #1723

## 2.0.0

### Grafana Mimir

_Changes since Cortex 1.10.0._

* [CHANGE] Remove chunks storage engine. #86 #119 #510 #545 #743 #744 #748 #753 #755 #757 #758 #759 #760 #762 #764 #789 #812 #813
  * The following CLI flags (and their respective YAML config options) have been removed:
    * `-store.engine`
    * `-schema-config-file`
    * `-ingester.checkpoint-duration`
    * `-ingester.checkpoint-enabled`
    * `-ingester.chunk-encoding`
    * `-ingester.chunk-age-jitter`
    * `-ingester.concurrent-flushes`
    * `-ingester.flush-on-shutdown-with-wal-enabled`
    * `-ingester.flush-op-timeout`
    * `-ingester.flush-period`
    * `-ingester.max-chunk-age`
    * `-ingester.max-chunk-idle`
    * `-ingester.max-series-per-query` (and `max_series_per_query` from runtime config)
    * `-ingester.max-stale-chunk-idle`
    * `-ingester.max-transfer-retries`
    * `-ingester.min-chunk-length`
    * `-ingester.recover-from-wal`
    * `-ingester.retain-period`
    * `-ingester.spread-flushes`
    * `-ingester.wal-dir`
    * `-ingester.wal-enabled`
    * `-querier.query-parallelism`
    * `-querier.second-store-engine`
    * `-querier.use-second-store-before-time`
    * `-flusher.wal-dir`
    * `-flusher.concurrent-flushes`
    * `-flusher.flush-op-timeout`
    * All `-table-manager.*` flags
    * All `-deletes.*` flags
    * All `-purger.*` flags
    * All `-metrics.*` flags
    * All `-dynamodb.*` flags
    * All `-s3.*` flags
    * All `-azure.*` flags
    * All `-bigtable.*` flags
    * All `-gcs.*` flags
    * All `-cassandra.*` flags
    * All `-boltdb.*` flags
    * All `-local.*` flags
    * All `-swift.*` flags
    * All `-store.*` flags except `-store.engine`, `-store.max-query-length`, `-store.max-labels-query-length`
    * All `-grpc-store.*` flags
  * The following API endpoints have been removed:
    * `/api/v1/chunks` and `/chunks`
  * The following metrics have been removed:
    * `cortex_ingester_flush_queue_length`
    * `cortex_ingester_queried_chunks`
    * `cortex_ingester_chunks_created_total`
    * `cortex_ingester_wal_replay_duration_seconds`
    * `cortex_ingester_wal_corruptions_total`
    * `cortex_ingester_sent_chunks`
    * `cortex_ingester_received_chunks`
    * `cortex_ingester_flush_series_in_progress`
    * `cortex_ingester_chunk_utilization`
    * `cortex_ingester_chunk_length`
    * `cortex_ingester_chunk_size_bytes`
    * `cortex_ingester_chunk_age_seconds`
    * `cortex_ingester_memory_chunks`
    * `cortex_ingester_flushing_enqueued_series_total`
    * `cortex_ingester_flushing_dequeued_series_total`
    * `cortex_ingester_dropped_chunks_total`
    * `cortex_oldest_unflushed_chunk_timestamp_seconds`
    * `prometheus_local_storage_chunk_ops_total`
    * `prometheus_local_storage_chunkdesc_ops_total`
    * `prometheus_local_storage_memory_chunkdescs`
* [CHANGE] Changed default storage backends from `s3` to `filesystem` #833
  This effects the following flags:
  * `-blocks-storage.backend` now defaults to `filesystem`
  * `-blocks-storage.filesystem.dir` now defaults to `blocks`
  * `-alertmanager-storage.backend` now defaults to `filesystem`
  * `-alertmanager-storage.filesystem.dir` now defaults to `alertmanager`
  * `-ruler-storage.backend` now defaults to `filesystem`
  * `-ruler-storage.filesystem.dir` now defaults to `ruler`
* [CHANGE] Renamed metric `cortex_experimental_features_in_use_total` as `cortex_experimental_features_used_total` and added `feature` label. #32 #658
* [CHANGE] Removed `log_messages_total` metric. #32
* [CHANGE] Some files and directories created by Mimir components on local disk now have stricter permissions, and are only readable by owner, but not group or others. #58
* [CHANGE] Memcached client DNS resolution switched from golang built-in to [`miekg/dns`](https://github.com/miekg/dns). #142
* [CHANGE] The metric `cortex_deprecated_flags_inuse_total` has been renamed to `deprecated_flags_inuse_total` as part of using grafana/dskit functionality. #185
* [CHANGE] API: The `-api.response-compression-enabled` flag has been removed, and GZIP response compression is always enabled except on `/api/v1/push` and `/push` endpoints. #880
* [CHANGE] Update Go version to 1.17.3. #480
* [CHANGE] The `status_code` label on gRPC client metrics has changed from '200' and '500' to '2xx', '5xx', '4xx', 'cancel' or 'error'. #537
* [CHANGE] Removed the deprecated `-<prefix>.fifocache.size` flag. #618
* [CHANGE] Enable index header lazy loading by default. #693
  * `-blocks-storage.bucket-store.index-header-lazy-loading-enabled` default from `false` to `true`
  * `-blocks-storage.bucket-store.index-header-lazy-loading-idle-timeout` default from `20m` to `1h`
* [CHANGE] Shuffle-sharding:
  * `-distributor.sharding-strategy` option has been removed, and shuffle sharding is enabled by default. Default shard size is set to 0, which disables shuffle sharding for the tenant (all ingesters will receive tenants's samples). #888
  * `-ruler.sharding-strategy` option has been removed from ruler. Ruler now uses shuffle-sharding by default, but respects `ruler_tenant_shard_size`, which defaults to 0 (ie. use all rulers for tenant). #889
  * `-store-gateway.sharding-strategy` option has been removed store-gateways. Store-gateway now uses shuffle-sharding by default, but respects `store_gateway_tenant_shard_size` for tenant, and this value defaults to 0. #891
* [CHANGE] Server: `-server.http-listen-port` (yaml: `server.http_listen_port`) now defaults to `8080` (previously `80`). #871
* [CHANGE] Changed the default value of `-blocks-storage.bucket-store.ignore-deletion-marks-delay` from 6h to 1h. #892
* [CHANGE] Changed default settings for memcached clients: #959 #1000
  * The default value for the following config options has changed from `10000` to `25000`:
    * `-blocks-storage.bucket-store.chunks-cache.memcached.max-async-buffer-size`
    * `-blocks-storage.bucket-store.index-cache.memcached.max-async-buffer-size`
    * `-blocks-storage.bucket-store.metadata-cache.memcached.max-async-buffer-size`
    * `-query-frontend.results-cache.memcached.max-async-buffer-size`
  * The default value for the following config options has changed from `0` (unlimited) to `100`:
    * `-blocks-storage.bucket-store.chunks-cache.memcached.max-get-multi-batch-size`
    * `-blocks-storage.bucket-store.index-cache.memcached.max-get-multi-batch-size`
    * `-blocks-storage.bucket-store.metadata-cache.memcached.max-get-multi-batch-size`
    * `-query-frontend.results-cache.memcached.max-get-multi-batch-size`
  * The default value for the following config options has changed from `16` to `100`:
    * `-blocks-storage.bucket-store.chunks-cache.memcached.max-idle-connections`
    * `-blocks-storage.bucket-store.index-cache.memcached.max-idle-connections`
    * `-blocks-storage.bucket-store.metadata-cache.memcached.max-idle-connections`
    * `-query-frontend.results-cache.memcached.max-idle-connections`
  * The default value for the following config options has changed from `100ms` to `200ms`:
    * `-blocks-storage.bucket-store.metadata-cache.memcached.timeout`
    * `-blocks-storage.bucket-store.index-cache.memcached.timeout`
    * `-blocks-storage.bucket-store.chunks-cache.memcached.timeout`
    * `-query-frontend.results-cache.memcached.timeout`
* [CHANGE] Changed the default value of `-blocks-storage.bucket-store.bucket-index.enabled` to `true`. The default configuration must now run the compactor in order to write the bucket index or else queries to long term storage will fail. #924
* [CHANGE] Option `-auth.enabled` has been renamed to `-auth.multitenancy-enabled`. #1130
* [CHANGE] Default tenant ID used with disabled auth (`-auth.multitenancy-enabled=false`) has changed from `fake` to `anonymous`. This tenant ID can now be changed with `-auth.no-auth-tenant` option. #1063
* [CHANGE] The default values for the following local directories have changed: #1072
  * `-alertmanager.storage.path` default value changed to `./data-alertmanager/`
  * `-compactor.data-dir` default value changed to `./data-compactor/`
  * `-ruler.rule-path` default value changed to `./data-ruler/`
* [CHANGE] The default value for gRPC max send message size has been changed from 16MB to 100MB. This affects the following parameters: #1152
  * `-query-frontend.grpc-client-config.grpc-max-send-msg-size`
  * `-ingester.client.grpc-max-send-msg-size`
  * `-querier.frontend-client.grpc-max-send-msg-size`
  * `-query-scheduler.grpc-client-config.grpc-max-send-msg-size`
  * `-ruler.client.grpc-max-send-msg-size`
* [CHANGE] Remove `-http.prefix` flag (and `http_prefix` config file option). #763
* [CHANGE] Remove legacy endpoints. Please use their alternatives listed below. As part of the removal process we are
  introducing two new sets of endpoints for the ruler configuration API: `<prometheus-http-prefix>/rules` and
  `<prometheus-http-prefix>/config/v1/rules/**`. We are also deprecating `<prometheus-http-prefix>/rules` and `/api/v1/rules`;
  and will remove them in Mimir 2.2.0. #763 #1222
  * Query endpoints

    | Legacy                                                  | Alternative                                                |
    | ------------------------------------------------------- | ---------------------------------------------------------- |
    | `/<legacy-http-prefix>/api/v1/query`                    | `<prometheus-http-prefix>/api/v1/query`                    |
    | `/<legacy-http-prefix>/api/v1/query_range`              | `<prometheus-http-prefix>/api/v1/query_range`              |
    | `/<legacy-http-prefix>/api/v1/query_exemplars`          | `<prometheus-http-prefix>/api/v1/query_exemplars`          |
    | `/<legacy-http-prefix>/api/v1/series`                   | `<prometheus-http-prefix>/api/v1/series`                   |
    | `/<legacy-http-prefix>/api/v1/labels`                   | `<prometheus-http-prefix>/api/v1/labels`                   |
    | `/<legacy-http-prefix>/api/v1/label/{name}/values`      | `<prometheus-http-prefix>/api/v1/label/{name}/values`      |
    | `/<legacy-http-prefix>/api/v1/metadata`                 | `<prometheus-http-prefix>/api/v1/metadata`                 |
    | `/<legacy-http-prefix>/api/v1/read`                     | `<prometheus-http-prefix>/api/v1/read`                     |
    | `/<legacy-http-prefix>/api/v1/cardinality/label_names`  | `<prometheus-http-prefix>/api/v1/cardinality/label_names`  |
    | `/<legacy-http-prefix>/api/v1/cardinality/label_values` | `<prometheus-http-prefix>/api/v1/cardinality/label_values` |
    | `/api/prom/user_stats`                                  | `/api/v1/user_stats`                                       |

  * Distributor endpoints

    | Legacy endpoint               | Alternative                   |
    | ----------------------------- | ----------------------------- |
    | `/<legacy-http-prefix>/push`  | `/api/v1/push`                |
    | `/all_user_stats`             | `/distributor/all_user_stats` |
    | `/ha-tracker`                 | `/distributor/ha_tracker`     |

  * Ingester endpoints

    | Legacy          | Alternative           |
    | --------------- | --------------------- |
    | `/ring`         | `/ingester/ring`      |
    | `/shutdown`     | `/ingester/shutdown`  |
    | `/flush`        | `/ingester/flush`     |
    | `/push`         | `/ingester/push`      |

  * Ruler endpoints

    | Legacy                                                | Alternative                                         | Alternative #2 (not available before Mimir 2.0.0)                    |
    | ----------------------------------------------------- | --------------------------------------------------- | ------------------------------------------------------------------- |
    | `/<legacy-http-prefix>/api/v1/rules`                  | `<prometheus-http-prefix>/api/v1/rules`             |                                                                     |
    | `/<legacy-http-prefix>/api/v1/alerts`                 | `<prometheus-http-prefix>/api/v1/alerts`            |                                                                     |
    | `/<legacy-http-prefix>/rules`                         | `/api/v1/rules` (see below)                         |  `<prometheus-http-prefix>/config/v1/rules`                         |
    | `/<legacy-http-prefix>/rules/{namespace}`             | `/api/v1/rules/{namespace}` (see below)             |  `<prometheus-http-prefix>/config/v1/rules/{namespace}`             |
    | `/<legacy-http-prefix>/rules/{namespace}/{groupName}` | `/api/v1/rules/{namespace}/{groupName}` (see below) |  `<prometheus-http-prefix>/config/v1/rules/{namespace}/{groupName}` |
    | `/<legacy-http-prefix>/rules/{namespace}`             | `/api/v1/rules/{namespace}` (see below)             |  `<prometheus-http-prefix>/config/v1/rules/{namespace}`             |
    | `/<legacy-http-prefix>/rules/{namespace}/{groupName}` | `/api/v1/rules/{namespace}/{groupName}` (see below) |  `<prometheus-http-prefix>/config/v1/rules/{namespace}/{groupName}` |
    | `/<legacy-http-prefix>/rules/{namespace}`             | `/api/v1/rules/{namespace}` (see below)             |  `<prometheus-http-prefix>/config/v1/rules/{namespace}`             |
    | `/ruler_ring`                                         | `/ruler/ring`                                       |                                                                     |

    > __Note:__ The `/api/v1/rules/**` endpoints are considered deprecated with Mimir 2.0.0 and will be removed
    in Mimir 2.2.0. After upgrading to 2.0.0 we recommend switching uses to the equivalent
    `/<prometheus-http-prefix>/config/v1/**` endpoints that Mimir 2.0.0 introduces.

  * Alertmanager endpoints

    | Legacy                      | Alternative                        |
    | --------------------------- | ---------------------------------- |
    | `/<legacy-http-prefix>`     | `/alertmanager`                    |
    | `/status`                   | `/multitenant_alertmanager/status` |

* [CHANGE] Ingester: changed `-ingester.stream-chunks-when-using-blocks` default value from `false` to `true`. #717
* [CHANGE] Ingester: default `-ingester.ring.min-ready-duration` reduced from 1m to 15s. #126
* [CHANGE] Ingester: `-ingester.ring.min-ready-duration` now start counting the delay after the ring's health checks have passed instead of when the ring client was started. #126
* [CHANGE] Ingester: allow experimental ingester max-exemplars setting to be changed dynamically #144
  * CLI flag `-blocks-storage.tsdb.max-exemplars` is renamed to `-ingester.max-global-exemplars-per-user`.
  * YAML `max_exemplars` is moved from `tsdb` to `overrides` and renamed to `max_global_exemplars_per_user`.
* [CHANGE] Ingester: active series metrics `cortex_ingester_active_series` and `cortex_ingester_active_series_custom_tracker` are now removed when their value is zero. #672 #690
* [CHANGE] Ingester: changed default value of `-blocks-storage.tsdb.retention-period` from `6h` to `24h`. #966
* [CHANGE] Ingester: changed default value of `-blocks-storage.tsdb.close-idle-tsdb-timeout` from `0` to `13h`. #967
* [CHANGE] Ingester: changed default value of `-ingester.ring.final-sleep` from `30s` to `0s`. #981
* [CHANGE] Ingester: the following low level settings have been removed: #1153
  * `-ingester-client.expected-labels`
  * `-ingester-client.expected-samples-per-series`
  * `-ingester-client.expected-timeseries`
* [CHANGE] Ingester: following command line options related to ingester ring were renamed: #1155
  * `-consul.*` changed to `-ingester.ring.consul.*`
  * `-etcd.*` changed to `-ingester.ring.etcd.*`
  * `-multi.*` changed to `-ingester.ring.multi.*`
  * `-distributor.excluded-zones` changed to `-ingester.ring.excluded-zones`
  * `-distributor.replication-factor` changed to `-ingester.ring.replication-factor`
  * `-distributor.zone-awareness-enabled` changed to `-ingester.ring.zone-awareness-enabled`
  * `-ingester.availability-zone` changed to `-ingester.ring.instance-availability-zone`
  * `-ingester.final-sleep` changed to `-ingester.ring.final-sleep`
  * `-ingester.heartbeat-period` changed to `-ingester.ring.heartbeat-period`
  * `-ingester.join-after` changed to `-ingester.ring.join-after`
  * `-ingester.lifecycler.ID` changed to `-ingester.ring.instance-id`
  * `-ingester.lifecycler.addr` changed to `-ingester.ring.instance-addr`
  * `-ingester.lifecycler.interface` changed to `-ingester.ring.instance-interface-names`
  * `-ingester.lifecycler.port` changed to `-ingester.ring.instance-port`
  * `-ingester.min-ready-duration` changed to `-ingester.ring.min-ready-duration`
  * `-ingester.num-tokens` changed to `-ingester.ring.num-tokens`
  * `-ingester.observe-period` changed to `-ingester.ring.observe-period`
  * `-ingester.readiness-check-ring-health` changed to `-ingester.ring.readiness-check-ring-health`
  * `-ingester.tokens-file-path` changed to `-ingester.ring.tokens-file-path`
  * `-ingester.unregister-on-shutdown` changed to `-ingester.ring.unregister-on-shutdown`
  * `-ring.heartbeat-timeout` changed to `-ingester.ring.heartbeat-timeout`
  * `-ring.prefix` changed to `-ingester.ring.prefix`
  * `-ring.store` changed to `-ingester.ring.store`
* [CHANGE] Ingester: fields in YAML configuration for ingester ring have been changed: #1155
  * `ingester.lifecycler` changed to `ingester.ring`
  * Fields from `ingester.lifecycler.ring` moved to `ingester.ring`
  * `ingester.lifecycler.address` changed to `ingester.ring.instance_addr`
  * `ingester.lifecycler.id` changed to `ingester.ring.instance_id`
  * `ingester.lifecycler.port` changed to `ingester.ring.instance_port`
  * `ingester.lifecycler.availability_zone` changed to `ingester.ring.instance_availability_zone`
  * `ingester.lifecycler.interface_names` changed to `ingester.ring.instance_interface_names`
* [CHANGE] Distributor: removed the `-distributor.shard-by-all-labels` configuration option. It is now assumed to be true. #698
* [CHANGE] Distributor: change default value of `-distributor.instance-limits.max-inflight-push-requests` to `2000`. #964
* [CHANGE] Distributor: change default value of `-distributor.remote-timeout` from `2s` to `20s`. #970
* [CHANGE] Distributor: removed the `-distributor.extra-query-delay` flag (and its respective YAML config option). #1048
* [CHANGE] Query-frontend: Enable query stats by default, they can still be disabled with `-query-frontend.query-stats-enabled=false`. #83
* [CHANGE] Query-frontend: the `cortex_frontend_mapped_asts_total` metric has been renamed to `cortex_frontend_query_sharding_rewrites_attempted_total`. #150
* [CHANGE] Query-frontend: added `sharded` label to `cortex_query_seconds_total` metric. #235
* [CHANGE] Query-frontend: changed the flag name for controlling query sharding total shards from `-querier.total-shards` to `-query-frontend.query-sharding-total-shards`. #230
* [CHANGE] Query-frontend: flag `-querier.parallelise-shardable-queries` has been renamed to `-query-frontend.parallelize-shardable-queries` #284
* [CHANGE] Query-frontend: removed the deprecated (and unused) `-frontend.cache-split-interval`. Use `-query-frontend.split-queries-by-interval` instead. #587
* [CHANGE] Query-frontend: range query response now omits the `data` field when it's empty (error case) like Prometheus does, previously it was `"data":{"resultType":"","result":null}`. #629
* [CHANGE] Query-frontend: instant queries now honor the `-query-frontend.max-retries-per-request` flag. #630
* [CHANGE] Query-frontend: removed in-memory and Redis cache support. Reason is that these caching backends were just supported by query-frontend, while all other Mimir services only support memcached. #796
  * The following CLI flags (and their respective YAML config options) have been removed:
    * `-frontend.cache.enable-fifocache`
    * `-frontend.redis.*`
    * `-frontend.fifocache.*`
  * The following metrics have been removed:
    * `querier_cache_added_total`
    * `querier_cache_added_new_total`
    * `querier_cache_evicted_total`
    * `querier_cache_entries`
    * `querier_cache_gets_total`
    * `querier_cache_misses_total`
    * `querier_cache_stale_gets_total`
    * `querier_cache_memory_bytes`
    * `cortex_rediscache_request_duration_seconds`
* [CHANGE] Query-frontend: migrated memcached backend client to the same one used in other components (memcached config and metrics are now consistent across all Mimir services). #821
  * The following CLI flags (and their respective YAML config options) have been added:
    * `-query-frontend.results-cache.backend` (set it to `memcached` if `-query-frontend.cache-results=true`)
  * The following CLI flags (and their respective YAML config options) have been changed:
    * `-frontend.memcached.hostname` and `-frontend.memcached.service` have been removed: use `-query-frontend.results-cache.memcached.addresses` instead
  * The following CLI flags (and their respective YAML config options) have been renamed:
    * `-frontend.background.write-back-concurrency` renamed to `-query-frontend.results-cache.memcached.max-async-concurrency`
    * `-frontend.background.write-back-buffer` renamed to `-query-frontend.results-cache.memcached.max-async-buffer-size`
    * `-frontend.memcached.batchsize` renamed to `-query-frontend.results-cache.memcached.max-get-multi-batch-size`
    * `-frontend.memcached.parallelism` renamed to `-query-frontend.results-cache.memcached.max-get-multi-concurrency`
    * `-frontend.memcached.timeout` renamed to `-query-frontend.results-cache.memcached.timeout`
    * `-frontend.memcached.max-item-size` renamed to `-query-frontend.results-cache.memcached.max-item-size`
    * `-frontend.memcached.max-idle-conns` renamed to `-query-frontend.results-cache.memcached.max-idle-connections`
    * `-frontend.compression` renamed to `-query-frontend.results-cache.compression`
  * The following CLI flags (and their respective YAML config options) have been removed:
    * `-frontend.memcached.circuit-breaker-consecutive-failures`: feature removed
    * `-frontend.memcached.circuit-breaker-timeout`: feature removed
    * `-frontend.memcached.circuit-breaker-interval`: feature removed
    * `-frontend.memcached.update-interval`: new setting is hardcoded to 30s
    * `-frontend.memcached.consistent-hash`: new setting is always enabled
    * `-frontend.default-validity` and `-frontend.memcached.expiration`: new setting is hardcoded to 7 days
  * The following metrics have been changed:
    * `cortex_cache_dropped_background_writes_total{name}` changed to `thanos_memcached_operation_skipped_total{name, operation, reason}`
    * `cortex_cache_value_size_bytes{name, method}` changed to `thanos_memcached_operation_data_size_bytes{name}`
    * `cortex_cache_request_duration_seconds{name, method, status_code}` changed to `thanos_memcached_operation_duration_seconds{name, operation}`
    * `cortex_cache_fetched_keys{name}` changed to `thanos_cache_memcached_requests_total{name}`
    * `cortex_cache_hits{name}` changed to `thanos_cache_memcached_hits_total{name}`
    * `cortex_memcache_request_duration_seconds{name, method, status_code}` changed to `thanos_memcached_operation_duration_seconds{name, operation}`
    * `cortex_memcache_client_servers{name}` changed to `thanos_memcached_dns_provider_results{name, addr}`
    * `cortex_memcache_client_set_skip_total{name}` changed to `thanos_memcached_operation_skipped_total{name, operation, reason}`
    * `cortex_dns_lookups_total` changed to `thanos_memcached_dns_lookups_total`
    * For all metrics the value of the "name" label has changed from `frontend.memcached` to `frontend-cache`
  * The following metrics have been removed:
    * `cortex_cache_background_queue_length{name}`
* [CHANGE] Query-frontend: merged `query_range` into `frontend` in the YAML config (keeping the same keys) and renamed flags: #825
  * `-querier.max-retries-per-request` renamed to `-query-frontend.max-retries-per-request`
  * `-querier.split-queries-by-interval` renamed to `-query-frontend.split-queries-by-interval`
  * `-querier.align-querier-with-step` renamed to `-query-frontend.align-querier-with-step`
  * `-querier.cache-results` renamed to `-query-frontend.cache-results`
  * `-querier.parallelise-shardable-queries` renamed to `-query-frontend.parallelize-shardable-queries`
* [CHANGE] Query-frontend: the default value of `-query-frontend.split-queries-by-interval` has changed from `0` to `24h`. #1131
* [CHANGE] Query-frontend: `-frontend.` flags were renamed to `-query-frontend.`: #1167
* [CHANGE] Query-frontend / Query-scheduler: classified the `-query-frontend.querier-forget-delay` and `-query-scheduler.querier-forget-delay` flags (and their respective YAML config options) as experimental. #1208
* [CHANGE] Querier / ruler: Change `-querier.max-fetched-chunks-per-query` configuration to limit to maximum number of chunks that can be fetched in a single query. The number of chunks fetched by ingesters AND long-term storare combined should not exceed the value configured on `-querier.max-fetched-chunks-per-query`. [#4260](https://github.com/cortexproject/cortex/pull/4260)
* [CHANGE] Querier / ruler: Option `-querier.ingester-streaming` has been removed. Querier/ruler now always use streaming method to query ingesters. #204
* [CHANGE] Querier: always fetch labels from store and respect start/end times in request; the option `-querier.query-store-for-labels-enabled` has been removed and is now always on. #518 #1132
* [CHANGE] Querier / ruler: removed the `-store.query-chunk-limit` flag (and its respective YAML config option `max_chunks_per_query`). `-querier.max-fetched-chunks-per-query` (and its respective YAML config option `max_fetched_chunks_per_query`) should be used instead. #705
* [CHANGE] Querier/Ruler: `-querier.active-query-tracker-dir` option has been removed. Active query tracking is now done via Activity tracker configured by `-activity-tracker.filepath` and enabled by default. Limit for max number of concurrent queries (`-querier.max-concurrent`) is now respected even if activity tracking is not enabled. #661 #822
* [CHANGE] Querier/ruler/query-frontend: the experimental `-querier.at-modifier-enabled` CLI flag has been removed and the PromQL `@` modifier is always enabled. #941
* [CHANGE] Querier: removed `-querier.worker-match-max-concurrent` and `-querier.worker-parallelism` CLI flags (and their respective YAML config options). Mimir now behaves like if `-querier.worker-match-max-concurrent` is always enabled and you should configure the max concurrency per querier process using `-querier.max-concurrent` instead. #958
* [CHANGE] Querier: changed default value of `-querier.query-ingesters-within` from `0` to `13h`. #967
* [CHANGE] Querier: rename metric `cortex_query_fetched_chunks_bytes_total` to `cortex_query_fetched_chunk_bytes_total` to be consistent with the limit name. #476
* [CHANGE] Ruler: add two new metrics `cortex_ruler_list_rules_seconds` and `cortex_ruler_load_rule_groups_seconds` to the ruler. #906
* [CHANGE] Ruler: endpoints for listing configured rules now return HTTP status code 200 and an empty map when there are no rules instead of an HTTP 404 and plain text error message. The following endpoints are affected: #456
  * `<prometheus-http-prefix>/config/v1/rules`
  * `<prometheus-http-prefix>/config/v1/rules/{namespace}`
  * `<prometheus-http-prefix>/rules` (deprecated)
  * `<prometheus-http-prefix>/rules/{namespace}` (deprecated)
  * `/api/v1/rules` (deprecated)
  * `/api/v1/rules/{namespace}` (deprecated)
* [CHANGE] Ruler: removed `configdb` support from Ruler backend storages. #15 #38 #819
* [CHANGE] Ruler: removed the support for the deprecated storage configuration via `-ruler.storage.*` CLI flags (and their respective YAML config options). Use `-ruler-storage.*` instead. #628
* [CHANGE] Ruler: set new default limits for rule groups: `-ruler.max-rules-per-rule-group` to 20 (previously 0, disabled) and `-ruler.max-rule-groups-per-tenant` to 70 (previously 0, disabled). #847
* [CHANGE] Ruler: removed `-ruler.enable-sharding` option, and changed default value of `-ruler.ring.store` to `memberlist`. #943
* [CHANGE] Ruler: `-ruler.alertmanager-use-v2` has been removed. The ruler will always use the `v2` endpoints. #954 #1100
* [CHANGE] Ruler: `-experimental.ruler.enable-api` flag has been renamed to `-ruler.enable-api` and is now stable. The default value has also changed from `false` to `true`, so both ruler and alertmanager API are enabled by default. #913 #1065
* [CHANGE] Ruler: add support for [DNS service discovery format](./docs/sources/configuration/arguments.md#dns-service-discovery) for `-ruler.alertmanager-url`. `-ruler.alertmanager-discovery` flag has been removed. URLs following the prior SRV format, will be treated as a static target. To continue using service discovery for these URLs prepend `dnssrvnoa+` to them. #993
  * The following metrics for Alertmanager DNS service discovery are replaced:
    * `prometheus_sd_dns_lookups_total` replaced by `cortex_dns_lookups_total{component="ruler"}`
    * `prometheus_sd_dns_lookup_failures_total` replaced by `cortex_dns_failures_total{component="ruler"}`
* [CHANGE] Ruler: deprecate `/api/v1/rules/**` and `<prometheus-http-prefix/rules/**` configuration API endpoints in favour of `/<prometheus-http-prefix>/config/v1/rules/**`. Deprecated endpoints will be removed in Mimir 2.2.0. Main configuration API endpoints are now `/<prometheus-http-prefix>/config/api/v1/rules/**` introduced in Mimir 2.0.0. #1222
* [CHANGE] Store-gateway: index cache now includes tenant in cache keys, this invalidates previous cached entries. #607
* [CHANGE] Store-gateway: increased memcached index caching TTL from 1 day to 7 days. #718
* [CHANGE] Store-gateway: options `-store-gateway.sharding-enabled` and `-querier.store-gateway-addresses` were removed. Default value of `-store-gateway.sharding-ring.store` is now `memberlist` and default value for `-store-gateway.sharding-ring.wait-stability-min-duration` changed from `1m` to `0` (disabled). #976
* [CHANGE] Compactor: compactor will no longer try to compact blocks that are already marked for deletion. Previously compactor would consider blocks marked for deletion within `-compactor.deletion-delay / 2` period as eligible for compaction. [#4328](https://github.com/cortexproject/cortex/pull/4328)
* [CHANGE] Compactor: Removed support for block deletion marks migration. If you're upgrading from Cortex < 1.7.0 to Mimir, you should upgrade the compactor to Cortex >= 1.7.0 first, run it at least once and then upgrade to Mimir. #122
* [CHANGE] Compactor: removed the `cortex_compactor_group_vertical_compactions_total` metric. #278
* [CHANGE] Compactor: no longer waits for initial blocks cleanup to finish before starting compactions. #282
* [CHANGE] Compactor: removed overlapping sources detection. Overlapping sources may exist due to edge cases (timing issues) when horizontally sharding compactor, but are correctly handled by compactor. #494
* [CHANGE] Compactor: compactor now uses deletion marks from `<tenant>/markers` location in the bucket. Marker files are no longer fetched, only listed. #550
* [CHANGE] Compactor: Default value of `-compactor.block-sync-concurrency` has changed from 20 to 8. This flag is now only used to control number of goroutines for downloading and uploading blocks during compaction. #552
* [CHANGE] Compactor is now included in `all` target (single-binary). #866
* [CHANGE] Compactor: Removed `-compactor.sharding-enabled` option. Sharding in compactor is now always enabled. Default value of `-compactor.ring.store` has changed from `consul` to `memberlist`. Default value of `-compactor.ring.wait-stability-min-duration` is now 0, which disables the feature. #956
* [CHANGE] Alertmanager: removed `-alertmanager.configs.auto-webhook-root` #977
* [CHANGE] Alertmanager: removed `configdb` support from Alertmanager backend storages. #15 #38 #819
* [CHANGE] Alertmanager: Don't count user-not-found errors from replicas as failures in the `cortex_alertmanager_state_fetch_replica_state_failed_total` metric. #190
* [CHANGE] Alertmanager: Use distributor for non-API routes. #213
* [CHANGE] Alertmanager: removed `-alertmanager.storage.*` configuration options, with the exception of the CLI flags `-alertmanager.storage.path` and `-alertmanager.storage.retention`. Use `-alertmanager-storage.*` instead. #632
* [CHANGE] Alertmanager: set default value for `-alertmanager.web.external-url=http://localhost:8080/alertmanager` to match the default configuration. #808 #1067
* [CHANGE] Alertmanager: `-experimental.alertmanager.enable-api` flag has been renamed to `-alertmanager.enable-api` and is now stable. #913
* [CHANGE] Alertmanager: now always runs with sharding enabled; other modes of operation are removed. #1044 #1126
  * The following configuration options are removed:
    * `-alertmanager.sharding-enabled`
    * `-alertmanager.cluster.advertise-address`
    * `-alertmanager.cluster.gossip-interval`
    * `-alertmanager.cluster.listen-address`
    * `-alertmanager.cluster.peers`
    * `-alertmanager.cluster.push-pull-interval`
  * The following configuration options are renamed:
    * `-alertmanager.cluster.peer-timeout` to `-alertmanager.peer-timeout`
* [CHANGE] Alertmanager: the default value of `-alertmanager.sharding-ring.store` is now `memberlist`. #1171
* [CHANGE] Ring: changed default value of `-distributor.ring.store` (Distributor ring) and `-ring.store` (Ingester ring) to `memberlist`. #1046
* [CHANGE] Memberlist: the `memberlist_kv_store_value_bytes` metric has been removed due to values no longer being stored in-memory as encoded bytes. [#4345](https://github.com/cortexproject/cortex/pull/4345)
* [CHANGE] Memberlist: forward only changes, not entire original message. [#4419](https://github.com/cortexproject/cortex/pull/4419)
* [CHANGE] Memberlist: don't accept old tombstones as incoming change, and don't forward such messages to other gossip members. [#4420](https://github.com/cortexproject/cortex/pull/4420)
* [CHANGE] Memberlist: changed probe interval from `1s` to `5s` and probe timeout from `500ms` to `2s`. #563
* [CHANGE] Memberlist: the `name` label on metrics `cortex_dns_failures_total`, `cortex_dns_lookups_total` and `cortex_dns_provider_results` was renamed to `component`. #993
* [CHANGE] Limits: removed deprecated limits for rejecting old samples #799
  This removes the following flags:
  * `-validation.reject-old-samples`
  * `-validation.reject-old-samples.max-age`
* [CHANGE] Limits: removed local limit-related flags in favor of global limits. #725
  The distributor ring is now required, and can be configured via the `distributor.ring.*` flags.
  This removes the following flags:
  * `-distributor.ingestion-rate-strategy` -> will now always use the "global" strategy
  * `-ingester.max-series-per-user` -> set `-ingester.max-global-series-per-user` to `N` times the existing value of `-ingester.max-series-per-user` instead
  * `-ingester.max-series-per-metric` -> set `-ingester.max-global-series-per-metric`  to `N` times the existing value of `-ingester.max-series-per-metric` instead
  * `-ingester.max-metadata-per-user` -> set `-ingester.max-global-metadata-per-user` to `N` times the existing value of `-ingester.max-metadata-per-user` instead
  * `-ingester.max-metadata-per-metric` -> set `-ingester.max-global-metadata-per-metric` to `N` times the existing value of `-ingester.max-metadata-per-metric` instead
  * In the above notes, `N` refers to the number of ingester replicas
  Additionally, default values for the following flags have changed:
  * `-ingester.max-global-series-per-user` from `0` to `150000`
  * `-ingester.max-global-series-per-metric` from `0` to `20000`
  * `-distributor.ingestion-rate-limit` from `25000` to `10000`
  * `-distributor.ingestion-burst-size` from `50000` to `200000`
* [CHANGE] Limits: removed limit `enforce_metric_name`, now behave as if set to `true` always. #686
* [CHANGE] Limits: Option `-ingester.max-samples-per-query` and its YAML field `max_samples_per_query` have been removed. It required `-querier.ingester-streaming` option to be set to false, but since `-querier.ingester-streaming` is removed (always defaulting to true), the limit using it was removed as well. #204 #1132
* [CHANGE] Limits: Set the default max number of inflight ingester push requests (`-ingester.instance-limits.max-inflight-push-requests`) to 30000 in order to prevent clusters from being overwhelmed by request volume or temporary slow-downs. #259
* [CHANGE] Overrides exporter: renamed metric `cortex_overrides` to `cortex_limits_overrides`. #173 #407
* [FEATURE] The following features have been moved from experimental to stable: #913 #1002
  * Alertmanager config API
  * Alertmanager receiver firewall
  * Alertmanager sharding
  * Azure blob storage support
  * Blocks storage bucket index
  * Disable the ring health check in the readiness endpoint (`-ingester.readiness-check-ring-health=false`)
  * Distributor: do not extend writes on unhealthy ingesters
  * Do not unregister ingesters from ring on shutdown (`-ingester.unregister-on-shutdown=false`)
  * HA Tracker: cleanup of old replicas from KV Store
  * Instance limits in ingester and distributor
  * OpenStack Swift storage support
  * Query-frontend: query stats tracking
  * Query-scheduler
  * Querier: tenant federation
  * Ruler config API
  * S3 Server Side Encryption (SSE) using KMS
  * TLS configuration for gRPC, HTTP and etcd clients
  * Zone-aware replication
  * `/labels` API using matchers
  * The following querier limits:
    * `-querier.max-fetched-chunks-per-query`
    * `-querier.max-fetched-chunk-bytes-per-query`
    * `-querier.max-fetched-series-per-query`
  * The following alertmanager limits:
    * Notification rate (`-alertmanager.notification-rate-limit` and `-alertmanager.notification-rate-limit-per-integration`)
    * Dispatcher groups (`-alertmanager.max-dispatcher-aggregation-groups`)
    * User config size (`-alertmanager.max-config-size-bytes`)
    * Templates count in user config (`-alertmanager.max-templates-count`)
    * Max template size (`-alertmanager.max-template-size-bytes`)
* [FEATURE] The endpoints `/api/v1/status/buildinfo`, `<prometheus-http-prefix>/api/v1/status/buildinfo`, and `<alertmanager-http-prefix>/api/v1/status/buildinfo` have been added to display build information and enabled features. #1219 #1240
* [FEATURE] PromQL: added `present_over_time` support. #139
* [FEATURE] Added "Activity tracker" feature which can log ongoing activities from previous Mimir run in case of a crash. It is enabled by default and controlled by the `-activity-tracker.filepath` flag. It can be disabled by setting this path to an empty string. Currently, the Store-gateway, Ruler, Querier, Query-frontend and Ingester components use this feature to track queries. #631 #782 #822 #1121
* [FEATURE] Divide configuration parameters into categories "basic", "advanced", and "experimental". Only flags in the basic category are shown when invoking `-help`, whereas `-help-all` will include flags in all categories (basic, advanced, experimental). #840
* [FEATURE] Querier: Added support for tenant federation to exemplar endpoints. #927
* [FEATURE] Ingester: can expose metrics on active series matching custom trackers configured via `-ingester.active-series-custom-trackers` (or its respective YAML config option). When configured, active series for custom trackers are exposed by the `cortex_ingester_active_series_custom_tracker` metric. #42 #672
* [FEATURE] Ingester: Enable snapshotting of in-memory TSDB on disk during shutdown via `-blocks-storage.tsdb.memory-snapshot-on-shutdown` (experimental). #249
* [FEATURE] Ingester: Added `-blocks-storage.tsdb.isolation-enabled` flag, which allows disabling TSDB isolation feature. This is enabled by default (per TSDB default), but disabling can improve performance of write requests. #512
* [FEATURE] Ingester: Added `-blocks-storage.tsdb.head-chunks-write-queue-size` flag, which allows setting the size of the queue used by the TSDB before m-mapping chunks (experimental). #591
  * Added `cortex_ingester_tsdb_mmap_chunk_write_queue_operations_total` metric to track different operations of this queue.
* [FEATURE] Distributor: Added `-api.skip-label-name-validation-header-enabled` option to allow skipping label name validation on the HTTP write path based on `X-Mimir-SkipLabelNameValidation` header being `true` or not. #390
* [FEATURE] Query-frontend: Add `cortex_query_fetched_series_total` and `cortex_query_fetched_chunks_bytes_total` per-user counters to expose the number of series and bytes fetched as part of queries. These metrics can be enabled with the `-frontend.query-stats-enabled` flag (or its respective YAML config option `query_stats_enabled`). [#4343](https://github.com/cortexproject/cortex/pull/4343)
* [FEATURE] Query-frontend: Add `cortex_query_fetched_chunks_total` per-user counter to expose the number of chunks fetched as part of queries. This metric can be enabled with the `-query-frontend.query-stats-enabled` flag (or its respective YAML config option `query_stats_enabled`). #31
* [FEATURE] Query-frontend: Add query sharding for instant and range queries. You can enable querysharding by setting `-query-frontend.parallelize-shardable-queries` to `true`. The following additional config and exported metrics have been added. #79 #80 #100 #124 #140 #148 #150 #151 #153 #154 #155 #156 #157 #158 #159 #160 #163 #169 #172 #196 #205 #225 #226 #227 #228 #230 #235 #240 #239 #246 #244 #319 #330 #371 #385 #400 #458 #586 #630 #660 #707 #1542
  * New config options:
    * `-query-frontend.query-sharding-total-shards`: The amount of shards to use when doing parallelisation via query sharding.
    * `-query-frontend.query-sharding-max-sharded-queries`: The max number of sharded queries that can be run for a given received query. 0 to disable limit.
    * `-blocks-storage.bucket-store.series-hash-cache-max-size-bytes`: Max size - in bytes - of the in-memory series hash cache in the store-gateway.
    * `-blocks-storage.tsdb.series-hash-cache-max-size-bytes`: Max size - in bytes - of the in-memory series hash cache in the ingester.
  * New exported metrics:
    * `cortex_bucket_store_series_hash_cache_requests_total`
    * `cortex_bucket_store_series_hash_cache_hits_total`
    * `cortex_frontend_query_sharding_rewrites_succeeded_total`
    * `cortex_frontend_sharded_queries_per_query`
  * Renamed metrics:
    * `cortex_frontend_mapped_asts_total` to `cortex_frontend_query_sharding_rewrites_attempted_total`
  * Modified metrics:
    * added `sharded` label to `cortex_query_seconds_total`
  * When query sharding is enabled, the following querier config must be set on query-frontend too:
    * `-querier.max-concurrent`
    * `-querier.timeout`
    * `-querier.max-samples`
    * `-querier.at-modifier-enabled`
    * `-querier.default-evaluation-interval`
    * `-querier.active-query-tracker-dir`
    * `-querier.lookback-delta`
  * Sharding can be dynamically controlled per request using the `Sharding-Control: 64` header. (0 to disable)
  * Sharding can be dynamically controlled per tenant using the limit `query_sharding_total_shards`. (0 to disable)
  * Added `sharded_queries` count to the "query stats" log.
  * The number of shards is adjusted to be compatible with number of compactor shards that are used by a split-and-merge compactor. The querier can use this to avoid querying blocks that cannot have series in a given query shard.
* [FEATURE] Query-Frontend: Added `-query-frontend.cache-unaligned-requests` option to cache responses for requests that do not have step-aligned start and end times. This can improve speed of repeated queries, but can also pollute cache with results that are never reused. #432
* [FEATURE] Querier: Added label names cardinality endpoint `<prefix>/api/v1/cardinality/label_names` that is disabled by default. Can be enabled/disabled via the CLI flag `-querier.cardinality-analysis-enabled` or its respective YAML config option. Configurable on a per-tenant basis. #301 #377 #474
* [FEATURE] Querier: Added label values cardinality endpoint `<prefix>/api/v1/cardinality/label_values` that is disabled by default. Can be enabled/disabled via the CLI flag `-querier.cardinality-analysis-enabled` or its respective YAML config option, and configurable on a per-tenant basis. The maximum number of label names allowed to be queried in a single API call can be controlled via `-querier.label-values-max-cardinality-label-names-per-request`. #332 #395 #474
* [FEATURE] Querier: Added `-store.max-labels-query-length` to restrict the range of `/series`, label-names and label-values requests. #507
* [FEATURE] Ruler: Add new `-ruler.query-stats-enabled` which when enabled will report the `cortex_ruler_query_seconds_total` as a per-user metric that tracks the sum of the wall time of executing queries in the ruler in seconds. [#4317](https://github.com/cortexproject/cortex/pull/4317)
* [FEATURE] Ruler: Added federated rule groups. #533
  * Added `-ruler.tenant-federation.enabled` config flag.
  * Added support for `source_tenants` field on rule groups.
* [FEATURE] Store-gateway: Added `/store-gateway/tenants` and `/store-gateway/tenant/{tenant}/blocks` endpoints that provide functionality that was provided by `tools/listblocks`. #911 #973
* [FEATURE] Compactor: compactor now uses new algorithm that we call "split-and-merge". Previous compaction strategy was removed. With the `split-and-merge` compactor source blocks for a given tenant are grouped into `-compactor.split-groups` number of groups. Each group of blocks is then compacted separately, and is split into `-compactor.split-and-merge-shards` shards (configurable on a per-tenant basis). Compaction of each tenant shards can be horizontally scaled. Number of compactors that work on jobs for single tenant can be limited by using `-compactor.compactor-tenant-shard-size` parameter, or per-tenant `compactor_tenant_shard_size` override.  #275 #281 #282 #283 #288 #290 #303 #307 #317 #323 #324 #328 #353 #368 #479 #820
* [FEATURE] Compactor: Added `-compactor.max-compaction-time` to control how long can compaction for a single tenant take. If compactions for a tenant take longer, no new compactions are started in the same compaction cycle. Running compactions are not stopped however, and may take much longer. #523
* [FEATURE] Compactor: When compactor finds blocks with out-of-order chunks, it will mark them for no-compaction. Blocks marked for no-compaction are ignored in future compactions too. Added metric `cortex_compactor_blocks_marked_for_no_compaction_total` to track number of blocks marked for no-compaction. Added `CortexCompactorSkippedBlocksWithOutOfOrderChunks` alert based on new metric. Markers are only checked from `<tenant>/markers` location, but uploaded to the block directory too. #520 #535 #550
* [FEATURE] Compactor: multiple blocks are now downloaded and uploaded at once, which can shorten compaction process. #552
* [ENHANCEMENT] Exemplars are now emitted for all gRPC calls and many operations tracked by histograms. #180
* [ENHANCEMENT] New options `-server.http-listen-network` and `-server.grpc-listen-network` allow binding as 'tcp4' or 'tcp6'. #180
* [ENHANCEMENT] Query federation: improve performance in MergeQueryable by memoizing labels. #312
* [ENHANCEMENT] Add histogram metrics `cortex_distributor_sample_delay_seconds` and `cortex_ingester_tsdb_sample_out_of_order_delta_seconds` #488
* [ENHANCEMENT] Check internal directory access before starting up. #1217
* [ENHANCEMENT] Azure client: expose option to configure MSI URL and user-assigned identity. #584
* [ENHANCEMENT] Added a new metric `mimir_build_info` to coincide with `cortex_build_info`. The metric `cortex_build_info` has not been removed. #1022
* [ENHANCEMENT] Mimir runs a sanity check of storage config at startup and will fail to start if the sanity check doesn't pass. This is done to find potential config issues before starting up. #1180
* [ENHANCEMENT] Validate alertmanager and ruler storage configurations to ensure they don't use same bucket name and region values as those configured for the blocks storage. #1214
* [ENHANCEMENT] Ingester: added option `-ingester.readiness-check-ring-health` to disable the ring health check in the readiness endpoint. When disabled, the health checks are run against only the ingester itself instead of all ingesters in the ring. #48 #126
* [ENHANCEMENT] Ingester: reduce CPU and memory utilization if remote write requests contains a large amount of "out of bounds" samples. #413
* [ENHANCEMENT] Ingester: reduce CPU and memory utilization when querying chunks from ingesters. #430
* [ENHANCEMENT] Ingester: Expose ingester ring page on ingesters. #654
* [ENHANCEMENT] Distributor: added option `-distributor.excluded-zones` to exclude ingesters running in specific zones both on write and read path. #51
* [ENHANCEMENT] Distributor: add tags to tracing span for distributor push with user, cluster and replica. #210
* [ENHANCEMENT] Distributor: performance optimisations. #212 #217 #242
* [ENHANCEMENT] Distributor: reduce latency when HA-Tracking by doing KVStore updates in the background. #271
* [ENHANCEMENT] Distributor: make distributor inflight push requests count include background calls to ingester. #398
* [ENHANCEMENT] Distributor: silently drop exemplars more than 5 minutes older than samples in the same batch. #544
* [ENHANCEMENT] Distributor: reject exemplars with blank label names or values. The `cortex_discarded_exemplars_total` metric will use the `exemplar_labels_blank` reason in this case. #873
* [ENHANCEMENT] Query-frontend: added `cortex_query_frontend_workers_enqueued_requests_total` metric to track the number of requests enqueued in each query-scheduler. #384
* [ENHANCEMENT] Query-frontend: added `cortex_query_frontend_non_step_aligned_queries_total` to track the total number of range queries with start/end not aligned to step. #347 #357 #582
* [ENHANCEMENT] Query-scheduler: exported summary `cortex_query_scheduler_inflight_requests` tracking total number of inflight requests (both enqueued and processing) in percentile buckets. #675
* [ENHANCEMENT] Querier: can use the `LabelNames` call with matchers, if matchers are provided in the `/labels` API call, instead of using the more expensive `MetricsForLabelMatchers` call as before. #3 #1186
* [ENHANCEMENT] Querier / store-gateway: optimized regex matchers. #319 #334 #355
* [ENHANCEMENT] Querier: when fetching data for specific query-shard, we can ignore some blocks based on compactor-shard ID, since sharding of series by query sharding and compactor is the same. Added metrics: #438 #450
  * `cortex_querier_blocks_found_total`
  * `cortex_querier_blocks_queried_total`
  * `cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total`
* [ENHANCEMENT] Querier / ruler: reduce cpu usage, latency and peak memory consumption. #459 #463 #589
* [ENHANCEMENT] Querier: labels requests now obey `-querier.query-ingesters-within`, making them a little more efficient. #518
* [ENHANCEMENT] Querier: retry store-gateway in case of unexpected failure, instead of failing the query. #1003
* [ENHANCEMENT] Querier / ruler: reduce memory used by streaming queries, particularly in ruler. [#4341](https://github.com/cortexproject/cortex/pull/4341)
* [ENHANCEMENT] Ruler: Using shuffle sharding subring on GetRules API. [#4466](https://github.com/cortexproject/cortex/pull/4466)
* [ENHANCEMENT] Ruler: wait for ruler ring client to self-detect during startup. #990
* [ENHANCEMENT] Store-gateway: added `cortex_bucket_store_sent_chunk_size_bytes` metric, tracking the size of chunks sent from store-gateway to querier. #123
* [ENHANCEMENT] Store-gateway: reduced CPU and memory utilization due to exported metrics aggregation for instances with a large number of tenants. #123 #142
* [ENHANCEMENT] Store-gateway: added an in-memory LRU cache for chunks attributes. Can be enabled setting `-blocks-storage.bucket-store.chunks-cache.attributes-in-memory-max-items=X` where `X` is the max number of items to keep in the in-memory cache. The following new metrics are exposed: #279 #415 #437
  * `cortex_cache_memory_requests_total`
  * `cortex_cache_memory_hits_total`
  * `cortex_cache_memory_items_count`
* [ENHANCEMENT] Store-gateway: log index cache requests to tracing spans. #419
* [ENHANCEMENT] Store-gateway: store-gateway can now ignore blocks with minimum time within `-blocks-storage.bucket-store.ignore-blocks-within` duration. Useful when used together with `-querier.query-store-after`. #502
* [ENHANCEMENT] Store-gateway: label values with matchers now doesn't preload or list series, reducing latency and memory consumption. #534
* [ENHANCEMENT] Store-gateway: the results of `LabelNames()`, `LabelValues()` and `Series(skipChunks=true)` calls are now cached in the index cache. #590
* [ENHANCEMENT] Store-gateway: Added `-store-gateway.sharding-ring.unregister-on-shutdown` option that allows store-gateway to stay in the ring even after shutdown. Defaults to `true`, which is the same as current behaviour. #610 #614
* [ENHANCEMENT] Store-gateway: wait for ring tokens stability instead of ring stability to speed up startup and tests. #620
* [ENHANCEMENT] Compactor: add timeout for waiting on compactor to become ACTIVE in the ring. [#4262](https://github.com/cortexproject/cortex/pull/4262)
* [ENHANCEMENT] Compactor: skip already planned compaction jobs if the tenant doesn't belong to the compactor instance anymore. #303
* [ENHANCEMENT] Compactor: Blocks cleaner will ignore users that it no longer "owns" when sharding is enabled, and user ownership has changed since last scan. #325
* [ENHANCEMENT] Compactor: added `-compactor.compaction-jobs-order` support to configure which compaction jobs should run first for a given tenant (in case there are multiple ones). Supported values are: `smallest-range-oldest-blocks-first` (default), `newest-blocks-first`. #364
* [ENHANCEMENT] Compactor: delete blocks marked for deletion faster. #490
* [ENHANCEMENT] Compactor: expose low-level concurrency options for compactor: `-compactor.max-opening-blocks-concurrency`, `-compactor.max-closing-blocks-concurrency`, `-compactor.symbols-flushers-concurrency`. #569 #701
* [ENHANCEMENT] Compactor: expand compactor logs to include total compaction job time, total time for uploads and block counts. #549
* [ENHANCEMENT] Ring: allow experimental configuration of disabling of heartbeat timeouts by setting the relevant configuration value to zero. Applies to the following: [#4342](https://github.com/cortexproject/cortex/pull/4342)
  * `-distributor.ring.heartbeat-timeout`
  * `-ingester.ring.heartbeat-timeout`
  * `-ruler.ring.heartbeat-timeout`
  * `-alertmanager.sharding-ring.heartbeat-timeout`
  * `-compactor.ring.heartbeat-timeout`
  * `-store-gateway.sharding-ring.heartbeat-timeout`
* [ENHANCEMENT] Ring: allow heartbeats to be explicitly disabled by setting the interval to zero. This is considered experimental. This applies to the following configuration options: [#4344](https://github.com/cortexproject/cortex/pull/4344)
  * `-distributor.ring.heartbeat-period`
  * `-ingester.ring.heartbeat-period`
  * `-ruler.ring.heartbeat-period`
  * `-alertmanager.sharding-ring.heartbeat-period`
  * `-compactor.ring.heartbeat-period`
  * `-store-gateway.sharding-ring.heartbeat-period`
* [ENHANCEMENT] Memberlist: optimized receive path for processing ring state updates, to help reduce CPU utilization in large clusters. [#4345](https://github.com/cortexproject/cortex/pull/4345)
* [ENHANCEMENT] Memberlist: expose configuration of memberlist packet compression via `-memberlist.compression-enabled`. [#4346](https://github.com/cortexproject/cortex/pull/4346)
* [ENHANCEMENT] Memberlist: Add `-memberlist.advertise-addr` and `-memberlist.advertise-port` options for setting the address to advertise to other members of the cluster to enable NAT traversal. #260
* [ENHANCEMENT] Memberlist: reduce CPU utilization for rings with a large number of members. #537 #563 #634
* [ENHANCEMENT] Overrides exporter: include additional limits in the per-tenant override exporter. The following limits have been added to the `cortex_limit_overrides` metric: #21
  * `max_fetched_series_per_query`
  * `max_fetched_chunk_bytes_per_query`
  * `ruler_max_rules_per_rule_group`
  * `ruler_max_rule_groups_per_tenant`
* [ENHANCEMENT] Overrides exporter: add a metrics `cortex_limits_defaults` to expose the default values of limits. #173
* [ENHANCEMENT] Overrides exporter: Add `max_fetched_chunks_per_query` and `max_global_exemplars_per_user` limits to the default and per-tenant limits exported as metrics. #471 #515
* [ENHANCEMENT] Upgrade Go to 1.17.8. #1347 #1381
* [ENHANCEMENT] Upgrade Docker base images to `alpine:3.15.0`. #1348
* [BUGFIX] Azure storage: only create HTTP client once, to reduce memory utilization. #605
* [BUGFIX] Ingester: fixed ingester stuck on start up (LEAVING ring state) when `-ingester.ring.heartbeat-period=0` and `-ingester.unregister-on-shutdown=false`. [#4366](https://github.com/cortexproject/cortex/pull/4366)
* [BUGFIX] Ingester: prevent any reads or writes while the ingester is stopping. This will prevent accessing TSDB blocks once they have been already closed. [#4304](https://github.com/cortexproject/cortex/pull/4304)
* [BUGFIX] Ingester: TSDB now waits for pending readers before truncating Head block, fixing the `chunk not found` error and preventing wrong query results. #16
* [BUGFIX] Ingester: don't create TSDB or appender if no samples are sent by a tenant. #162
* [BUGFIX] Ingester: fix out-of-order chunks in TSDB head in-memory series after WAL replay in case some samples were appended to TSDB WAL before series. #530
* [BUGFIX] Distributor: when cleaning up obsolete elected replicas from KV store, HA tracker didn't update number of cluster per user correctly. [#4336](https://github.com/cortexproject/cortex/pull/4336)
* [BUGFIX] Distributor: fix bug in query-exemplar where some results would get dropped. #583
* [BUGFIX] Query-frontend: Fixes @ modifier functions (start/end) when splitting queries by time. #206
* [BUGFIX] Query-frontend: Ensure query_range requests handled by the query-frontend return JSON formatted errors. #360 #499
* [BUGFIX] Query-frontend: don't reuse cached results for queries that are not step-aligned. #424
* [BUGFIX] Query-frontend: fix API error messages that were mentioning Prometheus `--enable-feature=promql-negative-offset` and `--enable-feature=promql-at-modifier` flags. #688
* [BUGFIX] Query-frontend: worker's cancellation channels are now buffered to ensure that all request cancellations are properly handled. #741
* [BUGFIX] Querier: fixed `/api/v1/user_stats` endpoint. When zone-aware replication is enabled, `MaxUnavailableZones` param is used instead of `MaxErrors`, so setting `MaxErrors = 0` doesn't make the Querier wait for all Ingesters responses. #474
* [BUGFIX] Querier: Disable query scheduler SRV DNS lookup. #689
* [BUGFIX] Ruler: fixed counting of PromQL evaluation errors as user-errors when updating `cortex_ruler_queries_failed_total`. [#4335](https://github.com/cortexproject/cortex/pull/4335)
* [BUGFIX] Ruler: fix formatting of rule groups in `/ruler/rule_groups` endpoint. #655
* [BUGFIX] Ruler: do not log `unable to read rules directory` at startup if the directory hasn't been created yet. #1058
* [BUGFIX] Ruler: enable Prometheus-compatible endpoints regardless of `-ruler.enable-api`. The flag now only controls the configuration API. This is what the config flag description stated, but not what was happening. #1216
* [BUGFIX] Compactor: fixed panic while collecting Prometheus metrics. #28
* [BUGFIX] Compactor: compactor should now be able to correctly mark blocks for deletion and no-compaction, if such marking was previously interrupted. #1015
* [BUGFIX] Alertmanager: remove stale template files. #4495
* [BUGFIX] Alertmanager: don't replace user configurations with blank fallback configurations (when enabled), particularly during scaling up/down instances when sharding is enabled. #224
* [BUGFIX] Ring: multi KV runtime config changes are now propagated to all rings, not just ingester ring. #1047
* [BUGFIX] Memberlist: fixed corrupted packets when sending compound messages with more than 255 messages or messages bigger than 64KB. #551
* [BUGFIX] Overrides exporter: successfully startup even if runtime config is not set. #1056
* [BUGFIX] Fix internal modules to wait for other modules depending on them before stopping. #1472

### Mixin

_Changes since `grafana/cortex-jsonnet` `1.9.0`._

* [CHANGE] Removed chunks storage support from mixin. #641 #643 #645 #811 #812 #813
  * Removed `tsdb.libsonnet`: no need to import it anymore (its content is already automatically included when using Jsonnet)
  * Removed the following fields from `_config`:
    * `storage_engine` (defaults to `blocks`)
    * `chunk_index_backend`
    * `chunk_store_backend`
  * Removed schema config map
  * Removed the following dashboards:
    * "Cortex / Chunks"
    * "Cortex / WAL"
    * "Cortex / Blocks vs Chunks"
  * Removed the following alerts:
    * `CortexOldChunkInMemory`
    * `CortexCheckpointCreationFailed`
    * `CortexCheckpointDeletionFailed`
    * `CortexProvisioningMemcachedTooSmall`
    * `CortexWALCorruption`
    * `CortexTableSyncFailure`
    * `CortexTransferFailed`
  * Removed the following recording rules:
    * `cortex_chunk_store_index_lookups_per_query`
    * `cortex_chunk_store_series_pre_intersection_per_query`
    * `cortex_chunk_store_series_post_intersection_per_query`
    * `cortex_chunk_store_chunks_per_query`
    * `cortex_bigtable_request_duration_seconds`
    * `cortex_cassandra_request_duration_seconds`
    * `cortex_dynamo_request_duration_seconds`
    * `cortex_database_request_duration_seconds`
    * `cortex_gcs_request_duration_seconds`
* [CHANGE] Update grafana-builder dependency: use $__rate_interval in qpsPanel and latencyPanel. [#372](https://github.com/grafana/cortex-jsonnet/pull/372)
* [CHANGE] `namespace` template variable in dashboards now only selects namespaces for selected clusters. [#311](https://github.com/grafana/cortex-jsonnet/pull/311)
* [CHANGE] `CortexIngesterRestarts` alert severity changed from `critical` to `warning`. [#321](https://github.com/grafana/cortex-jsonnet/pull/321)
* [CHANGE] Dashboards: added overridable `job_labels` and `cluster_labels` to the configuration object as label lists to uniquely identify jobs and clusters in the metric names and group-by lists in dashboards. [#319](https://github.com/grafana/cortex-jsonnet/pull/319)
* [CHANGE] Dashboards: `alert_aggregation_labels` has been removed from the configuration and overriding this value has been deprecated. Instead the labels are now defined by the `cluster_labels` list, and should be overridden accordingly through that list. [#319](https://github.com/grafana/cortex-jsonnet/pull/319)
* [CHANGE] Renamed `CortexCompactorHasNotUploadedBlocksSinceStart` to `CortexCompactorHasNotUploadedBlocks`. [#334](https://github.com/grafana/cortex-jsonnet/pull/334)
* [CHANGE] Renamed `CortexCompactorRunFailed` to `CortexCompactorHasNotSuccessfullyRunCompaction`. [#334](https://github.com/grafana/cortex-jsonnet/pull/334)
* [CHANGE] Renamed `CortexInconsistentConfig` alert to `CortexInconsistentRuntimeConfig` and increased severity to `critical`. [#335](https://github.com/grafana/cortex-jsonnet/pull/335)
* [CHANGE] Increased `CortexBadRuntimeConfig` alert severity to `critical` and removed support for `cortex_overrides_last_reload_successful` metric (was removed in Cortex 1.3.0). [#335](https://github.com/grafana/cortex-jsonnet/pull/335)
* [CHANGE] Grafana 'min step' changed to 15s so dashboard show better detail. [#340](https://github.com/grafana/cortex-jsonnet/pull/340)
* [CHANGE] Replace `CortexRulerFailedEvaluations` with two new alerts: `CortexRulerTooManyFailedPushes` and `CortexRulerTooManyFailedQueries`. [#347](https://github.com/grafana/cortex-jsonnet/pull/347)
* [CHANGE] Removed `CortexCacheRequestErrors` alert. This alert was not working because the legacy Cortex cache client instrumentation doesn't track errors. [#346](https://github.com/grafana/cortex-jsonnet/pull/346)
* [CHANGE] Removed `CortexQuerierCapacityFull` alert. [#342](https://github.com/grafana/cortex-jsonnet/pull/342)
* [CHANGE] Changes blocks storage alerts to group metrics by the configured `cluster_labels` (supporting the deprecated `alert_aggregation_labels`). [#351](https://github.com/grafana/cortex-jsonnet/pull/351)
* [CHANGE] Increased `CortexIngesterReachingSeriesLimit` critical alert threshold from 80% to 85%. [#363](https://github.com/grafana/cortex-jsonnet/pull/363)
* [CHANGE] Changed default `job_names` for query-frontend, query-scheduler and querier to match custom deployments too. [#376](https://github.com/grafana/cortex-jsonnet/pull/376)
* [CHANGE] Split `cortex_api` recording rule group into three groups. This is a workaround for large clusters where this group can become slow to evaluate. [#401](https://github.com/grafana/cortex-jsonnet/pull/401)
* [CHANGE] Increased `CortexIngesterReachingSeriesLimit` warning threshold from 70% to 80% and critical threshold from 85% to 90%. [#404](https://github.com/grafana/cortex-jsonnet/pull/404)
* [CHANGE] Raised `CortexKVStoreFailure` alert severity from warning to critical. #493
* [CHANGE] Increase `CortexRolloutStuck` alert "for" duration from 15m to 30m. #493 #573
* [CHANGE] The Alertmanager and Ruler compiled dashboards (`alertmanager.json` and `ruler.json`) have been respectively renamed to `mimir-alertmanager.json` and `mimir-ruler.json`. #869
* [CHANGE] Removed `cortex_overrides_metric` from `_config`. #871
* [CHANGE] Renamed recording rule groups (`cortex_` prefix changed to `mimir_`). #871
* [CHANGE] Alerts name prefix has been changed from `Cortex` to `Mimir` (eg. alert `CortexIngesterUnhealthy` has been renamed to `MimirIngesterUnhealthy`). #879
* [CHANGE] Enabled resources dashboards by default. Can be disabled setting `resources_dashboards_enabled` config field to `false`. #920
* [FEATURE] Added `Cortex / Overrides` dashboard, displaying default limits and per-tenant overrides applied to Mimir. #673
* [FEATURE] Added `Mimir / Tenants` and `Mimir / Top tenants` dashboards, displaying user-based metrics. #776
* [FEATURE] Added querier autoscaling panels and alerts. #1006 #1016
* [FEATURE] Mimir / Top tenants dashboard now has tenants ranked by rule group size and evaluation time. #1338
* [ENHANCEMENT] cortex-mixin: Make `cluster_namespace_deployment:kube_pod_container_resource_requests_{cpu_cores,memory_bytes}:sum` backwards compatible with `kube-state-metrics` v2.0.0. [#317](https://github.com/grafana/cortex-jsonnet/pull/317)
* [ENHANCEMENT] Cortex-mixin: Include `cortex-gw-internal` naming variation in default `gateway` job names. [#328](https://github.com/grafana/cortex-jsonnet/pull/328)
* [ENHANCEMENT] Ruler dashboard: added object storage metrics. [#354](https://github.com/grafana/cortex-jsonnet/pull/354)
* [ENHANCEMENT] Alertmanager dashboard: added object storage metrics. [#354](https://github.com/grafana/cortex-jsonnet/pull/354)
* [ENHANCEMENT] Added documentation text panels and descriptions to reads and writes dashboards. [#324](https://github.com/grafana/cortex-jsonnet/pull/324)
* [ENHANCEMENT] Dashboards: defined container functions for common resources panels: containerDiskWritesPanel, containerDiskReadsPanel, containerDiskSpaceUtilization. [#331](https://github.com/grafana/cortex-jsonnet/pull/331)
* [ENHANCEMENT] cortex-mixin: Added `alert_excluded_routes` config to exclude specific routes from alerts. [#338](https://github.com/grafana/cortex-jsonnet/pull/338)
* [ENHANCEMENT] Added `CortexMemcachedRequestErrors` alert. [#346](https://github.com/grafana/cortex-jsonnet/pull/346)
* [ENHANCEMENT] Ruler dashboard: added "Per route p99 latency" panel in the "Configuration API" row. [#353](https://github.com/grafana/cortex-jsonnet/pull/353)
* [ENHANCEMENT] Increased the `for` duration of the `CortexIngesterReachingSeriesLimit` warning alert to 3h. [#362](https://github.com/grafana/cortex-jsonnet/pull/362)
* [ENHANCEMENT] Added a new tier (`medium_small_user`) so we have another tier between 100K and 1Mil active series. [#364](https://github.com/grafana/cortex-jsonnet/pull/364)
* [ENHANCEMENT] Extend Alertmanager dashboard: [#313](https://github.com/grafana/cortex-jsonnet/pull/313)
  * "Tenants" stat panel - shows number of discovered tenant configurations.
  * "Replication" row - information about the replication of tenants/alerts/silences over instances.
  * "Tenant Configuration Sync" row - information about the configuration sync procedure.
  * "Sharding Initial State Sync" row - information about the initial state sync procedure when sharding is enabled.
  * "Sharding Runtime State Sync" row - information about various state operations which occur when sharding is enabled (replication, fetch, marge, persist).
* [ENHANCEMENT] Update gsutil command for `not healthy index found` playbook [#370](https://github.com/grafana/cortex-jsonnet/pull/370)
* [ENHANCEMENT] Added Alertmanager alerts and playbooks covering configuration syncs and sharding operation: [#377 [#378](https://github.com/grafana/cortex-jsonnet/pull/378)
  * `CortexAlertmanagerSyncConfigsFailing`
  * `CortexAlertmanagerRingCheckFailing`
  * `CortexAlertmanagerPartialStateMergeFailing`
  * `CortexAlertmanagerReplicationFailing`
  * `CortexAlertmanagerPersistStateFailing`
  * `CortexAlertmanagerInitialSyncFailed`
* [ENHANCEMENT] Add recording rules to improve responsiveness of Alertmanager dashboard. [#387](https://github.com/grafana/cortex-jsonnet/pull/387)
* [ENHANCEMENT] Add `CortexRolloutStuck` alert. [#405](https://github.com/grafana/cortex-jsonnet/pull/405)
* [ENHANCEMENT] Added `CortexKVStoreFailure` alert. [#406](https://github.com/grafana/cortex-jsonnet/pull/406)
* [ENHANCEMENT] Use configured `ruler` jobname for ruler dashboard panels. [#409](https://github.com/grafana/cortex-jsonnet/pull/409)
* [ENHANCEMENT] Add ability to override `datasource` for generated dashboards. [#407](https://github.com/grafana/cortex-jsonnet/pull/407)
* [ENHANCEMENT] Use alertmanager jobname for alertmanager dashboard panels [#411](https://github.com/grafana/cortex-jsonnet/pull/411)
* [ENHANCEMENT] Added `CortexDistributorReachingInflightPushRequestLimit` alert. [#408](https://github.com/grafana/cortex-jsonnet/pull/408)
* [ENHANCEMENT] Added `CortexReachingTCPConnectionsLimit` alert. #403
* [ENHANCEMENT] Added "Cortex / Writes Networking" and "Cortex / Reads Networking" dashboards. #405
* [ENHANCEMENT] Improved "Queue length" panel in "Cortex / Queries" dashboard. #408
* [ENHANCEMENT] Add `CortexDistributorReachingInflightPushRequestLimit` alert and playbook. #401
* [ENHANCEMENT] Added "Recover accidentally deleted blocks (Google Cloud specific)" playbook. #475
* [ENHANCEMENT] Added support to multi-zone store-gateway deployments. #608 #615
* [ENHANCEMENT] Show supplementary alertmanager services in the Rollout Progress dashboard. #738 #855
* [ENHANCEMENT] Added `mimir` to default job names. This makes dashboards and alerts working when Mimir is installed in single-binary mode and the deployment is named `mimir`. #921
* [ENHANCEMENT] Introduced a new alert for the Alertmanager: `MimirAlertmanagerAllocatingTooMuchMemory`. It has two severities based on the memory usage against limits, a `warning` level at 80% and a `critical` level at 90%. #1206
* [BUGFIX] Fixed `CortexIngesterHasNotShippedBlocks` alert false positive in case an ingester instance had ingested samples in the past, then no traffic was received for a long period and then it started receiving samples again. [#308](https://github.com/grafana/cortex-jsonnet/pull/308)
* [BUGFIX] Fixed `CortexInconsistentRuntimeConfig` metric. [#335](https://github.com/grafana/cortex-jsonnet/pull/335)
* [BUGFIX] Fixed scaling dashboard to correctly work when a Cortex service deployment spans across multiple zones (a zone is expected to have the `zone-[a-z]` suffix). [#365](https://github.com/grafana/cortex-jsonnet/pull/365)
* [BUGFIX] Fixed rollout progress dashboard to correctly work when a Cortex service deployment spans across multiple zones (a zone is expected to have the `zone-[a-z]` suffix). [#366](https://github.com/grafana/cortex-jsonnet/pull/366)
* [BUGFIX] Fixed rollout progress dashboard to include query-scheduler too. [#376](https://github.com/grafana/cortex-jsonnet/pull/376)
* [BUGFIX] Upstream recording rule `node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate` renamed. [#379](https://github.com/grafana/cortex-jsonnet/pull/379)
* [BUGFIX] Fixed writes/reads/alertmanager resources dashboards to use `$._config.job_names.gateway`. [#403](https://github.com/grafana/cortex-jsonnet/pull/403)
* [BUGFIX] Span the annotation.message in alerts as YAML multiline strings. [#412](https://github.com/grafana/cortex-jsonnet/pull/412)
* [BUGFIX] Fixed "Instant queries / sec" in "Cortex / Reads" dashboard. #445
* [BUGFIX] Fixed and added missing KV store panels in Writes, Reads, Ruler and Compactor dashboards. #448
* [BUGFIX] Fixed Alertmanager dashboard when alertmanager is running as part of single binary. #1064
* [BUGFIX] Fixed Ruler dashboard when ruler is running as part of single binary. #1260
* [BUGFIX] Query-frontend: fixed bad querier status code mapping with query-sharding enabled. #1227

### Jsonnet

_Changes since `grafana/cortex-jsonnet` `1.9.0`._

* [CHANGE] Removed chunks storage support. #639
  * Removed the following fields from `_config`:
    * `storage_engine` (defaults to `blocks`)
    * `querier_second_storage_engine` (not supported anymore)
    * `table_manager_enabled`, `table_prefix`
    * `memcached_index_writes_enabled` and `memcached_index_writes_max_item_size_mb`
    * `storeMemcachedChunksConfig`
    * `storeConfig`
    * `max_chunk_idle`
    * `schema` (the schema configmap is still added for backward compatibility reasons)
    * `bigtable_instance` and `bigtable_project`
    * `client_configs`
    * `enabledBackends`
    * `storage_backend`
    * `cassandra_addresses`
    * `s3_bucket_name`
    * `ingester_deployment_without_wal` (was only used by chunks storage)
    * `ingester` (was only used to configure chunks storage WAL)
  * Removed the following CLI flags from `ingester_args`:
    * `ingester.max-chunk-age`
    * `ingester.max-stale-chunk-idle`
    * `ingester.max-transfer-retries`
    * `ingester.retain-period`
* [CHANGE] Changed `overrides-exporter.libsonnet` from being based on cortex-tools to Mimir `overrides-exporter` target. #646
* [CHANGE] Store gateway: set `-blocks-storage.bucket-store.index-cache.memcached.max-get-multi-concurrency`,
  `-blocks-storage.bucket-store.chunks-cache.memcached.max-get-multi-concurrency`,
  `-blocks-storage.bucket-store.metadata-cache.memcached.max-get-multi-concurrency`,
  `-blocks-storage.bucket-store.index-cache.memcached.max-idle-connections`,
  `-blocks-storage.bucket-store.chunks-cache.memcached.max-idle-connections`,
  `-blocks-storage.bucket-store.metadata-cache.memcached.max-idle-connections` to 100 [#414](https://github.com/grafana/cortex-jsonnet/pull/414)
* [CHANGE] Alertmanager: mounted overrides configmap to alertmanager too. [#315](https://github.com/grafana/cortex-jsonnet/pull/315)
* [CHANGE] Memcached: upgraded memcached from `1.5.17` to `1.6.9`. [#316](https://github.com/grafana/cortex-jsonnet/pull/316)
* [CHANGE] Store-gateway: increased memory request and limit respectively from 6GB / 6GB to 12GB / 18GB. [#322](https://github.com/grafana/cortex-jsonnet/pull/322)
* [CHANGE] Store-gateway: increased `-blocks-storage.bucket-store.max-chunk-pool-bytes` from 2GB (default) to 12GB. [#322](https://github.com/grafana/cortex-jsonnet/pull/322)
* [CHANGE] Ingester/Ruler: set `-server.grpc-max-send-msg-size-bytes` and `-server.grpc-max-send-msg-size-bytes` to sensible default values (10MB). [#326](https://github.com/grafana/cortex-jsonnet/pull/326)
* [CHANGE] Decreased `-server.grpc-max-concurrent-streams` from 100k to 10k. [#369](https://github.com/grafana/cortex-jsonnet/pull/369)
* [CHANGE] Decreased blocks storage ingesters graceful termination period from 80m to 20m. [#369](https://github.com/grafana/cortex-jsonnet/pull/369)
* [CHANGE] Increase the rules per group and rule groups limits on different tiers. [#396](https://github.com/grafana/cortex-jsonnet/pull/396)
* [CHANGE] Removed `max_samples_per_query` limit, since it only works with chunks and only when using `-distributor.shard-by-all-labels=false`. [#397](https://github.com/grafana/cortex-jsonnet/pull/397)
* [CHANGE] Removed chunks storage query sharding config support. The following config options have been removed: [#398](https://github.com/grafana/cortex-jsonnet/pull/398)
  * `_config` > `queryFrontend` > `shard_factor`
  * `_config` > `queryFrontend` > `sharded_queries_enabled`
  * `_config` > `queryFrontend` > `query_split_factor`
* [CHANGE] Rename ruler_s3_bucket_name and ruler_gcs_bucket_name to ruler_storage_bucket_name: [#415](https://github.com/grafana/cortex-jsonnet/pull/415)
* [CHANGE] Fine-tuned rolling update policy for distributor, querier, query-frontend, query-scheduler. [#420](https://github.com/grafana/cortex-jsonnet/pull/420)
* [CHANGE] Increased memcached metadata/chunks/index-queries max connections from 4k to 16k. [#420](https://github.com/grafana/cortex-jsonnet/pull/420)
* [CHANGE] Disabled step alignment in query-frontend to be compliant with PromQL. [#420](https://github.com/grafana/cortex-jsonnet/pull/420)
* [CHANGE] Do not limit compactor CPU and request a number of cores equal to the configured concurrency. [#420](https://github.com/grafana/cortex-jsonnet/pull/420)
* [CHANGE] Configured split-and-merge compactor. #853
  * The following CLI flags are set on compactor:
    * `-compactor.split-and-merge-shards=0`
    * `-compactor.compactor-tenant-shard-size=1`
    * `-compactor.split-groups=1`
    * `-compactor.max-opening-blocks-concurrency=4`
    * `-compactor.max-closing-blocks-concurrency=2`
    * `-compactor.symbols-flushers-concurrency=4`
  * The following per-tenant overrides have been set on `super_user` and `mega_user` classes:
    ```
    compactor_split_and_merge_shards: 2,
    compactor_tenant_shard_size: 2,
    compactor_split_groups: 2,
    ```
* [CHANGE] The entrypoint file to include has been renamed from `cortex.libsonnet` to `mimir.libsonnet`. #897
* [CHANGE] The default image config field has been renamed from `cortex` to `mimir`. #896
   ```
   {
     _images+:: {
       mimir: '...',
     },
   }
   ```
* [CHANGE] Removed `cortex_` prefix from config fields. #898
  * The following config fields have been renamed:
    * `cortex_bucket_index_enabled` renamed to `bucket_index_enabled`
    * `cortex_compactor_cleanup_interval` renamed to `compactor_cleanup_interval`
    * `cortex_compactor_data_disk_class` renamed to `compactor_data_disk_class`
    * `cortex_compactor_data_disk_size` renamed to `compactor_data_disk_size`
    * `cortex_compactor_max_concurrency` renamed to `compactor_max_concurrency`
    * `cortex_distributor_allow_multiple_replicas_on_same_node` renamed to `distributor_allow_multiple_replicas_on_same_node`
    * `cortex_ingester_data_disk_class` renamed to `ingester_data_disk_class`
    * `cortex_ingester_data_disk_size` renamed to `ingester_data_disk_size`
    * `cortex_querier_allow_multiple_replicas_on_same_node` renamed to `querier_allow_multiple_replicas_on_same_node`
    * `cortex_query_frontend_allow_multiple_replicas_on_same_node` renamed to `query_frontend_allow_multiple_replicas_on_same_node`
    * `cortex_query_sharding_enabled` renamed to `query_sharding_enabled`
    * `cortex_query_sharding_msg_size_factor` renamed to `query_sharding_msg_size_factor`
    * `cortex_ruler_allow_multiple_replicas_on_same_node` renamed to `ruler_allow_multiple_replicas_on_same_node`
    * `cortex_store_gateway_data_disk_class` renamed to `store_gateway_data_disk_class`
    * `cortex_store_gateway_data_disk_size` renamed to `store_gateway_data_disk_size`
* [CHANGE] The overrides configmap default mountpoint has changed from `/etc/cortex` to `/etc/mimir`. It can be customized via the `overrides_configmap_mountpoint` config field. #899
* [CHANGE] Enabled in the querier the features to query label names with matchers, PromQL at modifier and query long-term storage for labels. #905
* [CHANGE] Reduced TSDB blocks retention on ingesters disk from 96h to 24h. #905
* [CHANGE] Enabled closing of idle TSDB in ingesters. #905
* [CHANGE] Disabled TSDB isolation in ingesters for better performances. #905
* [CHANGE] Changed log level of querier, query-frontend, query-scheduler and alertmanager from `debug` to `info`. #905
* [CHANGE] Enabled attributes in-memory cache in store-gateway. #905
* [CHANGE] Configured store-gateway to not load blocks containing samples more recent than 10h (because such samples are queried from ingesters). #905
* [CHANGE] Dynamically compute `-compactor.deletion-delay` based on other settings, in order to reduce the deletion delay as much as possible and lower the number of live blocks in the storage. #907
* [CHANGE] The config field `distributorConfig` has been renamed to `ingesterRingClientConfig`. Config field `ringClient` has been removed in favor of `ingesterRingClientConfig`. #997 #1057
* [CHANGE] Gossip.libsonnet has been fixed to modify all ring configurations, not only the ingester ring config. Furthermore it now supports migration via multi KV store. #1057 #1099
* [CHANGE] Changed the default of `bucket_index_enabled` to `true`. #924
* [CHANGE] Remove the support for the test-exporter. #1133
* [CHANGE] Removed `$.distributor_deployment_labels`, `$.ingester_deployment_labels` and `$.querier_deployment_labels` fields, that were used by gossip.libsonnet to inject additional label. Now the label is injected directly into pods of statefulsets and deployments. #1297
* [CHANGE] Disabled `-ingester.readiness-check-ring-health`. #1352
* [CHANGE] Changed Alertmanager CPU request from `100m` to `2` cores, and memory request from `1Gi` to `10Gi`. Set Alertmanager memory limit to `15Gi`. #1206
* [CHANGE] gossip.libsonnet has been renamed to memberlist.libsonnet, and is now imported by default. Use of memberlist for ring is enabled by setting `_config.memberlist_ring_enabled` to true. #1526
* [FEATURE] Added query sharding support. It can be enabled setting `cortex_query_sharding_enabled: true` in the `_config` object. #653
* [FEATURE] Added shuffle-sharding support. It can be enabled and configured using the following config: #902
   ```
   _config+:: {
     shuffle_sharding:: {
       ingester_write_path_enabled: true,
       ingester_read_path_enabled: true,
       querier_enabled: true,
       ruler_enabled: true,
       store_gateway_enabled: true,
     },
   }
   ```
* [FEATURE] Added multi-zone ingesters and store-gateways support. #1352 #1552
* [ENHANCEMENT] Add overrides config to compactor. This allows setting retention configs per user. [#386](https://github.com/grafana/cortex-jsonnet/pull/386)
* [ENHANCEMENT] Added 256MB memory ballast to querier. [#369](https://github.com/grafana/cortex-jsonnet/pull/369)
* [ENHANCEMENT] Update `etcd-operator` to latest version (see https://github.com/grafana/jsonnet-libs/pull/480). [#263](https://github.com/grafana/cortex-jsonnet/pull/263)
* [ENHANCEMENT] Add support for Azure storage in Alertmanager configuration. [#381](https://github.com/grafana/cortex-jsonnet/pull/381)
* [ENHANCEMENT] Add support for running Alertmanager in sharding mode. [#394](https://github.com/grafana/cortex-jsonnet/pull/394)
* [ENHANCEMENT] Allow to customize PromQL engine settings via `queryEngineConfig`. [#399](https://github.com/grafana/cortex-jsonnet/pull/399)
* [ENHANCEMENT] Define Azure object storage ruler args. [#416](https://github.com/grafana/cortex-jsonnet/pull/416)
* [ENHANCEMENT] Added the following config options to allow to schedule multiple replicas of the same service on the same node: [#418](https://github.com/grafana/cortex-jsonnet/pull/418)
  * `cortex_distributor_allow_multiple_replicas_on_same_node`
  * `cortex_ruler_allow_multiple_replicas_on_same_node`
  * `cortex_querier_allow_multiple_replicas_on_same_node`
  * `cortex_query_frontend_allow_multiple_replicas_on_same_node`
* [BUGFIX] Alertmanager: fixed `--alertmanager.cluster.peers` CLI flag passed to alertmanager when HA is enabled. [#329](https://github.com/grafana/cortex-jsonnet/pull/329)
* [BUGFIX] Fixed `-distributor.extend-writes` setting on ruler when `unregister_ingesters_on_shutdown` is disabled. [#369](https://github.com/grafana/cortex-jsonnet/pull/369)
* [BUGFIX] Treat `compactor_blocks_retention_period` type as string rather than int.[#395](https://github.com/grafana/cortex-jsonnet/pull/395)
* [BUGFIX] Pass `-ruler-storage.s3.endpoint` to ruler when using S3. [#421](https://github.com/grafana/cortex-jsonnet/pull/421)
* [BUGFIX] Remove service selector on label `gossip_ring_member` from other services than `gossip-ring`. [#1008](https://github.com/grafana/mimir/pull/1008)
* [BUGFIX] Rename `-ingester.readiness-check-ring-health` to `-ingester.ring.readiness-check-ring-health`, to reflect current name of flag. #1460

### Mimirtool

_Changes since cortextool `0.10.7`._

* [CHANGE] The following environment variables have been renamed: #883
  * `CORTEX_ADDRESS` to `MIMIR_ADDRESS`
  * `CORTEX_API_USER` to `MIMIR_API_USER`
  * `CORTEX_API_KEY` to `MIMIR_API_KEY`
  * `CORTEX_TENANT_ID` to `MIMIR_TENANT_ID`
  * `CORTEX_TLS_CA_PATH` to `MIMIR_TLS_CA_PATH`
  * `CORTEX_TLS_CERT_PATH` to `MIMIR_TLS_CERT_PATH`
  * `CORTEX_TLS_KEY_PATH` to `MIMIR_TLS_KEY_PATH`
* [CHANGE] Change `cortex` backend to `mimir`. #883
* [CHANGE] Do not publish `mimirtool` binary for 386 windows architecture. #1263
* [CHANGE] `analyse` command has been renamed to `analyze`. #1318
* [FEATURE] Support Arm64 on Darwin for all binaries (benchtool etc). https://github.com/grafana/cortex-tools/pull/215
* [ENHANCEMENT] Correctly support federated rules. #823
* [BUGFIX] Fix `cortextool rules` legends displaying wrong symbols for updates and deletions. https://github.com/grafana/cortex-tools/pull/226

### Query-tee

_Changes since Cortex `1.10.0`._

* [ENHANCEMENT] Added `/api/v1/query_exemplars` API endpoint support (no results comparison). #168
* [ENHANCEMENT] Add a flag (`--proxy.compare-use-relative-error`) in the query-tee to compare floating point values using relative error. #208
* [ENHANCEMENT] Add a flag (`--proxy.compare-skip-recent-samples`) in the query-tee to skip comparing recent samples. By default samples not older than 1 minute are skipped. #234
* [BUGFIX] Fixes a panic in the query-tee when comparing result. #207
* [BUGFIX] Ensure POST requests are handled correctly #286

### Blocksconvert

_Changes since Cortex `1.10.0`._

* [CHANGE] Blocksconvert tool was removed from Mimir. #637

### Metaconvert

_Changes since Cortex `1.10.0`._

* [CHANGE] `thanosconvert` tool has been renamed to `metaconvert`. `-config.file` option has been removed, while it now requires `-tenant` option to work on single tenant only. It now also preserves labels recognized by Mimir. #1120

### Test-exporter

_Changes since Cortex `1.10.0`._

* [CHANGE] Removed the test-exporter tool. #1133

### Tools

_Changes since Cortex `1.10.0`._

* [CHANGE] Removed `query-audit`. You can use `query-tee` to compare query results and performances of two Grafana Mimir backends. #1380

## Cortex 1.10.0 / 2021-08-03

* [CHANGE] Prevent path traversal attack from users able to control the HTTP header `X-Scope-OrgID`. #4375 (CVE-2021-36157)
  * Users only have control of the HTTP header when Cortex is not frontend by an auth proxy validating the tenant IDs
* [CHANGE] Enable strict JSON unmarshal for `pkg/util/validation.Limits` struct. The custom `UnmarshalJSON()` will now fail if the input has unknown fields. #4298
* [CHANGE] Cortex chunks storage has been deprecated and it's now in maintenance mode: all Cortex users are encouraged to migrate to the blocks storage. No new features will be added to the chunks storage. The default Cortex configuration still runs the chunks engine; please check out the [blocks storage doc](https://cortexmetrics.io/docs/blocks-storage/) on how to configure Cortex to run with the blocks storage.  #4268
* [CHANGE] The example Kubernetes manifests (stored at `k8s/`) have been removed due to a lack of proper support and maintenance. #4268
* [CHANGE] Querier / ruler: deprecated `-store.query-chunk-limit` CLI flag (and its respective YAML config option `max_chunks_per_query`) in favour of `-querier.max-fetched-chunks-per-query` (and its respective YAML config option `max_fetched_chunks_per_query`). The new limit specifies the maximum number of chunks that can be fetched in a single query from ingesters and long-term storage: the total number of actual fetched chunks could be 2x the limit, being independently applied when querying ingesters and long-term storage. #4125
* [CHANGE] Alertmanager: allowed to configure the experimental receivers firewall on a per-tenant basis. The following CLI flags (and their respective YAML config options) have been changed and moved to the limits config section: #4143
  - `-alertmanager.receivers-firewall.block.cidr-networks` renamed to `-alertmanager.receivers-firewall-block-cidr-networks`
  - `-alertmanager.receivers-firewall.block.private-addresses` renamed to `-alertmanager.receivers-firewall-block-private-addresses`
* [CHANGE] Change default value of `-server.grpc.keepalive.min-time-between-pings` from `5m` to `10s` and `-server.grpc.keepalive.ping-without-stream-allowed` to `true`. #4168
* [CHANGE] Ingester: Change default value of `-ingester.active-series-metrics-enabled` to `true`. This incurs a small increase in memory usage, between 1.2% and 1.6% as measured on ingesters with 1.3M active series. #4257
* [CHANGE] Dependency: update go-redis from v8.2.3 to v8.9.0. #4236
* [FEATURE] Querier: Added new `-querier.max-fetched-series-per-query` flag. When Cortex is running with blocks storage, the max series per query limit is enforced in the querier and applies to unique series received from ingesters and store-gateway (long-term storage). #4179
* [FEATURE] Querier/Ruler: Added new `-querier.max-fetched-chunk-bytes-per-query` flag. When Cortex is running with blocks storage, the max chunk bytes limit is enforced in the querier and ruler and limits the size of all aggregated chunks returned from ingesters and storage as bytes for a query. #4216
* [FEATURE] Alertmanager: support negative matchers, time-based muting - [upstream release notes](https://github.com/prometheus/alertmanager/releases/tag/v0.22.0). #4237
* [FEATURE] Alertmanager: Added rate-limits to notifiers. Rate limits used by all integrations can be configured using `-alertmanager.notification-rate-limit`, while per-integration rate limits can be specified via `-alertmanager.notification-rate-limit-per-integration` parameter. Both shared and per-integration limits can be overwritten using overrides mechanism. These limits are applied on individual (per-tenant) alertmanagers. Rate-limited notifications are failed notifications. It is possible to monitor rate-limited notifications via new `cortex_alertmanager_notification_rate_limited_total` metric. #4135 #4163
* [FEATURE] Alertmanager: Added `-alertmanager.max-config-size-bytes` limit to control size of configuration files that Cortex users can upload to Alertmanager via API. This limit is configurable per-tenant. #4201
* [FEATURE] Alertmanager: Added `-alertmanager.max-templates-count` and `-alertmanager.max-template-size-bytes` options to control number and size of templates uploaded to Alertmanager via API. These limits are configurable per-tenant. #4223
* [FEATURE] Added flag `-debug.block-profile-rate` to enable goroutine blocking events profiling. #4217
* [FEATURE] Alertmanager: The experimental sharding feature is now considered complete. Detailed information about the configuration options can be found [here for alertmanager](https://cortexmetrics.io/docs/configuration/configuration-file/#alertmanager_config) and [here for the alertmanager storage](https://cortexmetrics.io/docs/configuration/configuration-file/#alertmanager_storage_config). To use the feature: #3925 #4020 #4021 #4031 #4084 #4110 #4126 #4127 #4141 #4146 #4161 #4162 #4222
  * Ensure that a remote storage backend is configured for Alertmanager to store state using `-alertmanager-storage.backend`, and flags related to the backend. Note that the `local` and `configdb` storage backends are not supported.
  * Ensure that a ring store is configured using `-alertmanager.sharding-ring.store`, and set the flags relevant to the chosen store type.
  * Enable the feature using `-alertmanager.sharding-enabled`.
  * Note the prior addition of a new configuration option `-alertmanager.persist-interval`. This sets the interval between persisting the current alertmanager state (notification log and silences) to object storage. See the [configuration file reference](https://cortexmetrics.io/docs/configuration/configuration-file/#alertmanager_config) for more information.
* [ENHANCEMENT] Alertmanager: Cleanup persisted state objects from remote storage when a tenant configuration is deleted. #4167
* [ENHANCEMENT] Storage: Added the ability to disable Open Census within GCS client (e.g `-gcs.enable-opencensus=false`). #4219
* [ENHANCEMENT] Etcd: Added username and password to etcd config. #4205
* [ENHANCEMENT] Alertmanager: introduced new metrics to monitor operation when using `-alertmanager.sharding-enabled`: #4149
  * `cortex_alertmanager_state_fetch_replica_state_total`
  * `cortex_alertmanager_state_fetch_replica_state_failed_total`
  * `cortex_alertmanager_state_initial_sync_total`
  * `cortex_alertmanager_state_initial_sync_completed_total`
  * `cortex_alertmanager_state_initial_sync_duration_seconds`
  * `cortex_alertmanager_state_persist_total`
  * `cortex_alertmanager_state_persist_failed_total`
* [ENHANCEMENT] Blocks storage: support ingesting exemplars and querying of exemplars.  Enabled by setting new CLI flag `-blocks-storage.tsdb.max-exemplars=<n>` or config option `blocks_storage.tsdb.max_exemplars` to positive value. #4124 #4181
* [ENHANCEMENT] Distributor: Added distributors ring status section in the admin page. #4151
* [ENHANCEMENT] Added zone-awareness support to alertmanager for use when sharding is enabled. When zone-awareness is enabled, alerts will be replicated across availability zones. #4204
* [ENHANCEMENT] Added `tenant_ids` tag to tracing spans #4186
* [ENHANCEMENT] Ring, query-frontend: Avoid using automatic private IPs (APIPA) when discovering IP address from the interface during the registration of the instance in the ring, or by query-frontend when used with query-scheduler. APIPA still used as last resort with logging indicating usage. #4032
* [ENHANCEMENT] Memberlist: introduced new metrics to aid troubleshooting tombstone convergence: #4231
  * `memberlist_client_kv_store_value_tombstones`
  * `memberlist_client_kv_store_value_tombstones_removed_total`
  * `memberlist_client_messages_to_broadcast_dropped_total`
* [ENHANCEMENT] Alertmanager: Added `-alertmanager.max-dispatcher-aggregation-groups` option to control max number of active dispatcher groups in Alertmanager (per tenant, also overrideable). When the limit is reached, Dispatcher produces log message and increases `cortex_alertmanager_dispatcher_aggregation_group_limit_reached_total` metric. #4254
* [ENHANCEMENT] Alertmanager: Added `-alertmanager.max-alerts-count` and `-alertmanager.max-alerts-size-bytes` to control max number of alerts and total size of alerts that a single user can have in Alertmanager's memory. Adding more alerts will fail with a log message and incrementing `cortex_alertmanager_alerts_insert_limited_total` metric (per-user). These limits can be overrided by using per-tenant overrides. Current values are tracked in `cortex_alertmanager_alerts_limiter_current_alerts` and `cortex_alertmanager_alerts_limiter_current_alerts_size_bytes` metrics. #4253
* [ENHANCEMENT] Store-gateway: added `-store-gateway.sharding-ring.wait-stability-min-duration` and `-store-gateway.sharding-ring.wait-stability-max-duration` support to store-gateway, to wait for ring stability at startup. #4271
* [ENHANCEMENT] Ruler: added `rule_group` label to metrics `cortex_prometheus_rule_group_iterations_total` and `cortex_prometheus_rule_group_iterations_missed_total`. #4121
* [ENHANCEMENT] Ruler: added new metrics for tracking total number of queries and push requests sent to ingester, as well as failed queries and push requests. Failures are only counted for internal errors, but not user-errors like limits or invalid query. This is in contrast to existing `cortex_prometheus_rule_evaluation_failures_total`, which is incremented also when query or samples appending fails due to user-errors. #4281
  * `cortex_ruler_write_requests_total`
  * `cortex_ruler_write_requests_failed_total`
  * `cortex_ruler_queries_total`
  * `cortex_ruler_queries_failed_total`
* [ENHANCEMENT] Ingester: Added option `-ingester.ignore-series-limit-for-metric-names` with comma-separated list of metric names that will be ignored in max series per metric limit. #4302
* [ENHANCEMENT] Added instrumentation to Redis client, with the following metrics: #3976
  - `cortex_rediscache_request_duration_seconds`
* [BUGFIX] Purger: fix `Invalid null value in condition for column range` caused by `nil` value in range for WriteBatch query. #4128
* [BUGFIX] Ingester: fixed infrequent panic caused by a race condition between TSDB mmap-ed head chunks truncation and queries. #4176
* [BUGFIX] Alertmanager: fix Alertmanager status page if clustering via gossip is disabled or sharding is enabled. #4184
* [BUGFIX] Ruler: fix `/ruler/rule_groups` endpoint doesn't work when used with object store. #4182
* [BUGFIX] Ruler: Honor the evaluation delay for the `ALERTS` and `ALERTS_FOR_STATE` series. #4227
* [BUGFIX] Make multiple Get requests instead of MGet on Redis Cluster. #4056
* [BUGFIX] Ingester: fix issue where runtime limits erroneously override default limits. #4246
* [BUGFIX] Ruler: fix startup in single-binary mode when the new `ruler_storage` is used. #4252
* [BUGFIX] Querier: fix queries failing with "at least 1 healthy replica required, could only find 0" error right after scaling up store-gateways until they're ACTIVE in the ring. #4263
* [BUGFIX] Store-gateway: when blocks sharding is enabled, do not load all blocks in each store-gateway in case of a cold startup, but load only blocks owned by the store-gateway replica. #4271
* [BUGFIX] Memberlist: fix to setting the default configuration value for `-memberlist.retransmit-factor` when not provided. This should improve propagation delay of the ring state (including, but not limited to, tombstones). Note that if the configuration is already explicitly given, this fix has no effect. #4269
* [BUGFIX] Querier: Fix issue where samples in a chunk might get skipped by batch iterator. #4218

### Blocksconvert

* [ENHANCEMENT] Scanner: add support for DynamoDB (v9 schema only). #3828
* [ENHANCEMENT] Add Cassandra support. #3795
* [ENHANCEMENT] Scanner: retry failed uploads. #4188

## Cortex 1.9.0 / 2021-05-14

* [CHANGE] Alertmanager now removes local files after Alertmanager is no longer running for removed or resharded user. #3910
* [CHANGE] Alertmanager now stores local files in per-tenant folders. Files stored by Alertmanager previously are migrated to new hierarchy. Support for this migration will be removed in Cortex 1.11. #3910
* [CHANGE] Ruler: deprecated `-ruler.storage.*` CLI flags (and their respective YAML config options) in favour of `-ruler-storage.*`. The deprecated config will be removed in Cortex 1.11. #3945
* [CHANGE] Alertmanager: deprecated `-alertmanager.storage.*` CLI flags (and their respective YAML config options) in favour of `-alertmanager-storage.*`. This change doesn't apply to `alertmanager.storage.path` and `alertmanager.storage.retention`. The deprecated config will be removed in Cortex 1.11. #4002
* [CHANGE] Alertmanager: removed `-cluster.` CLI flags deprecated in Cortex 1.7. The new config options to use are: #3946
  * `-alertmanager.cluster.listen-address` instead of `-cluster.listen-address`
  * `-alertmanager.cluster.advertise-address` instead of `-cluster.advertise-address`
  * `-alertmanager.cluster.peers` instead of `-cluster.peer`
  * `-alertmanager.cluster.peer-timeout` instead of `-cluster.peer-timeout`
* [CHANGE] Blocks storage: removed the config option `-blocks-storage.bucket-store.index-cache.postings-compression-enabled`, which was deprecated in Cortex 1.6. Postings compression is always enabled. #4101
* [CHANGE] Querier: removed the config option `-store.max-look-back-period`, which was deprecated in Cortex 1.6 and was used only by the chunks storage. You should use `-querier.max-query-lookback` instead. #4101
* [CHANGE] Query Frontend: removed the config option `-querier.compress-http-responses`, which was deprecated in Cortex 1.6. You should use`-api.response-compression-enabled` instead. #4101
* [CHANGE] Runtime-config / overrides: removed the config options `-limits.per-user-override-config` (use `-runtime-config.file`) and `-limits.per-user-override-period` (use `-runtime-config.reload-period`), both deprecated since Cortex 0.6.0. #4112
* [CHANGE] Cortex now fails fast on startup if unable to connect to the ring backend. #4068
* [FEATURE] The following features have been marked as stable: #4101
  - Shuffle-sharding
  - Querier support for querying chunks and blocks store at the same time
  - Tracking of active series and exporting them as metrics (`-ingester.active-series-metrics-enabled` and related flags)
  - Blocks storage: lazy mmap of block indexes in the store-gateway (`-blocks-storage.bucket-store.index-header-lazy-loading-enabled`)
  - Ingester: close idle TSDB and remove them from local disk (`-blocks-storage.tsdb.close-idle-tsdb-timeout`)
* [FEATURE] Memberlist: add TLS configuration options for the memberlist transport layer used by the gossip KV store. #4046
  * New flags added for memberlist communication:
    * `-memberlist.tls-enabled`
    * `-memberlist.tls-cert-path`
    * `-memberlist.tls-key-path`
    * `-memberlist.tls-ca-path`
    * `-memberlist.tls-server-name`
    * `-memberlist.tls-insecure-skip-verify`
* [FEATURE] Ruler: added `local` backend support to the ruler storage configuration under the `-ruler-storage.` flag prefix. #3932
* [ENHANCEMENT] Store-gateway: cache object attributes looked up when fetching chunks in the metadata cache when configured (`-blocks-storage.bucket-store.metadata-cache.backend`) instead of the chunk cache. #270
* [ENHANCEMENT] Upgraded Docker base images to `alpine:3.13`. #4042
* [ENHANCEMENT] Blocks storage: reduce ingester memory by eliminating series reference cache. #3951
* [ENHANCEMENT] Ruler: optimized `<prefix>/api/v1/rules` and `<prefix>/api/v1/alerts` when ruler sharding is enabled. #3916
* [ENHANCEMENT] Ruler: added the following metrics when ruler sharding is enabled: #3916
  * `cortex_ruler_clients`
  * `cortex_ruler_client_request_duration_seconds`
* [ENHANCEMENT] Alertmanager: Add API endpoint to list all tenant alertmanager configs: `GET /multitenant_alertmanager/configs`. #3529
* [ENHANCEMENT] Ruler: Add API endpoint to list all tenant ruler rule groups: `GET /ruler/rule_groups`. #3529
* [ENHANCEMENT] Query-frontend/scheduler: added querier forget delay (`-query-frontend.querier-forget-delay` and `-query-scheduler.querier-forget-delay`) to mitigate the blast radius in the event queriers crash because of a repeatedly sent "query of death" when shuffle-sharding is enabled. #3901
* [ENHANCEMENT] Query-frontend: reduced memory allocations when serializing query response. #3964
* [ENHANCEMENT] Querier / ruler: some optimizations to PromQL query engine. #3934 #3989
* [ENHANCEMENT] Ingester: reduce CPU and memory when an high number of errors are returned by the ingester on the write path with the blocks storage. #3969 #3971 #3973
* [ENHANCEMENT] Distributor: reduce CPU and memory when an high number of errors are returned by the distributor on the write path. #3990
* [ENHANCEMENT] Put metric before label value in the "label value too long" error message. #4018
* [ENHANCEMENT] Allow use of `y|w|d` suffixes for duration related limits and per-tenant limits. #4044
* [ENHANCEMENT] Query-frontend: Small optimization on top of PR #3968 to avoid unnecessary Extents merging. #4026
* [ENHANCEMENT] Add a metric `cortex_compactor_compaction_interval_seconds` for the compaction interval config value. #4040
* [ENHANCEMENT] Ingester: added following per-ingester (instance) experimental limits: max number of series in memory (`-ingester.instance-limits.max-series`), max number of users in memory (`-ingester.instance-limits.max-tenants`), max ingestion rate (`-ingester.instance-limits.max-ingestion-rate`), and max inflight requests (`-ingester.instance-limits.max-inflight-push-requests`). These limits are only used when using blocks storage. Limits can also be configured using runtime-config feature, and current values are exported as `cortex_ingester_instance_limits` metric. #3992.
* [ENHANCEMENT] Cortex is now built with Go 1.16. #4062
* [ENHANCEMENT] Distributor: added per-distributor experimental limits: max number of inflight requests (`-distributor.instance-limits.max-inflight-push-requests`) and max ingestion rate in samples/sec (`-distributor.instance-limits.max-ingestion-rate`). If not set, these two are unlimited. Also added metrics to expose current values (`cortex_distributor_inflight_push_requests`, `cortex_distributor_ingestion_rate_samples_per_second`) as well as limits (`cortex_distributor_instance_limits` with various `limit` label values). #4071
* [ENHANCEMENT] Ruler: Added `-ruler.enabled-tenants` and `-ruler.disabled-tenants` to explicitly enable or disable rules processing for specific tenants. #4074
* [ENHANCEMENT] Block Storage Ingester: `/flush` now accepts two new parameters: `tenant` to specify tenant to flush and `wait=true` to make call synchronous. Multiple tenants can be specified by repeating `tenant` parameter. If no `tenant` is specified, all tenants are flushed, as before. #4073
* [ENHANCEMENT] Alertmanager: validate configured `-alertmanager.web.external-url` and fail if ends with `/`. #4081
* [ENHANCEMENT] Alertmanager: added `-alertmanager.receivers-firewall.block.cidr-networks` and `-alertmanager.receivers-firewall.block.private-addresses` to block specific network addresses in HTTP-based Alertmanager receiver integrations. #4085
* [ENHANCEMENT] Allow configuration of Cassandra's host selection policy. #4069
* [ENHANCEMENT] Store-gateway: retry synching blocks if a per-tenant sync fails. #3975 #4088
* [ENHANCEMENT] Add metric `cortex_tcp_connections` exposing the current number of accepted TCP connections. #4099
* [ENHANCEMENT] Querier: Allow federated queries to run concurrently. #4065
* [ENHANCEMENT] Label Values API call now supports `match[]` parameter when querying blocks on storage (assuming `-querier.query-store-for-labels-enabled` is enabled). #4133
* [BUGFIX] Ruler-API: fix bug where `/api/v1/rules/<namespace>/<group_name>` endpoint return `400` instead of `404`. #4013
* [BUGFIX] Distributor: reverted changes done to rate limiting in #3825. #3948
* [BUGFIX] Ingester: Fix race condition when opening and closing tsdb concurrently. #3959
* [BUGFIX] Querier: streamline tracing spans. #3924
* [BUGFIX] Ruler Storage: ignore objects with empty namespace or group in the name. #3999
* [BUGFIX] Distributor: fix issue causing distributors to not extend the replication set because of failing instances when zone-aware replication is enabled. #3977
* [BUGFIX] Query-frontend: Fix issue where cached entry size keeps increasing when making tiny query repeatedly. #3968
* [BUGFIX] Compactor: `-compactor.blocks-retention-period` now supports weeks (`w`) and years (`y`). #4027
* [BUGFIX] Querier: returning 422 (instead of 500) when query hits `max_chunks_per_query` limit with block storage, when the limit is hit in the store-gateway. #3937
* [BUGFIX] Ruler: Rule group limit enforcement should now allow the same number of rules in a group as the limit. #3616
* [BUGFIX] Frontend, Query-scheduler: allow querier to notify about shutdown without providing any authentication. #4066
* [BUGFIX] Querier: fixed race condition causing queries to fail right after querier startup with the "empty ring" error. #4068
* [BUGFIX] Compactor: Increment `cortex_compactor_runs_failed_total` if compactor failed compact a single tenant. #4094
* [BUGFIX] Tracing: hot fix to avoid the Jaeger tracing client to indefinitely block the Cortex process shutdown in case the HTTP connection to the tracing backend is blocked. #4134
* [BUGFIX] Forward proper EndsAt from ruler to Alertmanager inline with Prometheus behaviour. #4017
* [BUGFIX] Querier: support filtering LabelValues with matchers when using tenant federation. #4277

### Blocksconvert

* [ENHANCEMENT] Builder: add `-builder.timestamp-tolerance` option which may reduce block size by rounding timestamps to make difference whole seconds. #3891

## Cortex 1.8.1 / 2021-04-27

* [CHANGE] Fix for CVE-2021-31232: Local file disclosure vulnerability when `-experimental.alertmanager.enable-api` is used. The HTTP basic auth `password_file` can be used as an attack vector to send any file content via a webhook. The alertmanager templates can be used as an attack vector to send any file content because the alertmanager can load any text file specified in the templates list.

## Cortex 1.8.0 / 2021-03-24

* [CHANGE] Alertmanager: Don't expose cluster information to tenants via the `/alertmanager/api/v1/status` API endpoint when operating with clustering enabled. #3903
* [CHANGE] Ingester: don't update internal "last updated" timestamp of TSDB if tenant only sends invalid samples. This affects how "idle" time is computed. #3727
* [CHANGE] Require explicit flag `-<prefix>.tls-enabled` to enable TLS in GRPC clients. Previously it was enough to specify a TLS flag to enable TLS validation. #3156
* [CHANGE] Query-frontend: removed `-querier.split-queries-by-day` (deprecated in Cortex 0.4.0). Please use `-querier.split-queries-by-interval` instead. #3813
* [CHANGE] Store-gateway: the chunks pool controlled by `-blocks-storage.bucket-store.max-chunk-pool-bytes` is now shared across all tenants. #3830
* [CHANGE] Ingester: return error code 400 instead of 429 when per-user/per-tenant series/metadata limits are reached. #3833
* [CHANGE] Compactor: add `reason` label to `cortex_compactor_blocks_marked_for_deletion_total` metric. Source blocks marked for deletion by compactor are labelled as `compaction`, while blocks passing the retention period are labelled as `retention`. #3879
* [CHANGE] Alertmanager: the `DELETE /api/v1/alerts` is now idempotent. No error is returned if the alertmanager config doesn't exist. #3888
* [FEATURE] Experimental Ruler Storage: Add a separate set of configuration options to configure the ruler storage backend under the `-ruler-storage.` flag prefix. All blocks storage bucket clients and the config service are currently supported. Clients using this implementation will only be enabled if the existing `-ruler.storage` flags are left unset. #3805 #3864
* [FEATURE] Experimental Alertmanager Storage: Add a separate set of configuration options to configure the alertmanager storage backend under the `-alertmanager-storage.` flag prefix. All blocks storage bucket clients and the config service are currently supported. Clients using this implementation will only be enabled if the existing `-alertmanager.storage` flags are left unset. #3888
* [FEATURE] Adds support to S3 server-side encryption using KMS. The S3 server-side encryption config can be overridden on a per-tenant basis for the blocks storage, ruler and alertmanager. Deprecated `-<prefix>.s3.sse-encryption`, please use the following CLI flags that have been added. #3651 #3810 #3811 #3870 #3886 #3906
  - `-<prefix>.s3.sse.type`
  - `-<prefix>.s3.sse.kms-key-id`
  - `-<prefix>.s3.sse.kms-encryption-context`
* [FEATURE] Querier: Enable `@ <timestamp>` modifier in PromQL using the new `-querier.at-modifier-enabled` flag. #3744
* [FEATURE] Overrides Exporter: Add `overrides-exporter` module for exposing per-tenant resource limit overrides as metrics. It is not included in `all` target (single-binary mode), and must be explicitly enabled. #3785
* [FEATURE] Experimental thanosconvert: introduce an experimental tool `thanosconvert` to migrate Thanos block metadata to Cortex metadata. #3770
* [FEATURE] Alertmanager: It now shards the `/api/v1/alerts` API using the ring when sharding is enabled. #3671
  * Added `-alertmanager.max-recv-msg-size` (defaults to 16M) to limit the size of HTTP request body handled by the alertmanager.
  * New flags added for communication between alertmanagers:
    * `-alertmanager.max-recv-msg-size`
    * `-alertmanager.alertmanager-client.remote-timeout`
    * `-alertmanager.alertmanager-client.tls-enabled`
    * `-alertmanager.alertmanager-client.tls-cert-path`
    * `-alertmanager.alertmanager-client.tls-key-path`
    * `-alertmanager.alertmanager-client.tls-ca-path`
    * `-alertmanager.alertmanager-client.tls-server-name`
    * `-alertmanager.alertmanager-client.tls-insecure-skip-verify`
* [FEATURE] Compactor: added blocks storage per-tenant retention support. This is configured via `-compactor.retention-period`, and can be overridden on a per-tenant basis. #3879
* [ENHANCEMENT] Queries: Instrument queries that were discarded due to the configured `max_outstanding_requests_per_tenant`. #3894
  * `cortex_query_frontend_discarded_requests_total`
  * `cortex_query_scheduler_discarded_requests_total`
* [ENHANCEMENT] Ruler: Add TLS and explicit basis authentication configuration options for the HTTP client the ruler uses to communicate with the alertmanager. #3752
  * `-ruler.alertmanager-client.basic-auth-username`: Configure the basic authentication username used by the client. Takes precedent over a URL configured username.
  * `-ruler.alertmanager-client.basic-auth-password`: Configure the basic authentication password used by the client. Takes precedent over a URL configured password.
  * `-ruler.alertmanager-client.tls-ca-path`: File path to the CA file.
  * `-ruler.alertmanager-client.tls-cert-path`: File path to the TLS certificate.
  * `-ruler.alertmanager-client.tls-insecure-skip-verify`: Boolean to disable verifying the certificate.
  * `-ruler.alertmanager-client.tls-key-path`: File path to the TLS key certificate.
  * `-ruler.alertmanager-client.tls-server-name`: Expected name on the TLS certificate.
* [ENHANCEMENT] Ingester: exposed metric `cortex_ingester_oldest_unshipped_block_timestamp_seconds`, tracking the unix timestamp of the oldest TSDB block not shipped to the storage yet. #3705
* [ENHANCEMENT] Prometheus upgraded. #3739 #3806
  * Avoid unnecessary `runtime.GC()` during compactions.
  * Prevent compaction loop in TSDB on data gap.
* [ENHANCEMENT] Query-Frontend now returns server side performance metrics using `Server-Timing` header when query stats is enabled. #3685
* [ENHANCEMENT] Runtime Config: Add a `mode` query parameter for the runtime config endpoint. `/runtime_config?mode=diff` now shows the YAML runtime configuration with all values that differ from the defaults. #3700
* [ENHANCEMENT] Distributor: Enable downstream projects to wrap distributor push function and access the deserialized write requests berfore/after they are pushed. #3755
* [ENHANCEMENT] Add flag `-<prefix>.tls-server-name` to require a specific server name instead of the hostname on the certificate. #3156
* [ENHANCEMENT] Alertmanager: Remove a tenant's alertmanager instead of pausing it as we determine it is no longer needed. #3722
* [ENHANCEMENT] Blocks storage: added more configuration options to S3 client. #3775
  * `-blocks-storage.s3.tls-handshake-timeout`: Maximum time to wait for a TLS handshake. 0 means no limit.
  * `-blocks-storage.s3.expect-continue-timeout`: The time to wait for a server's first response headers after fully writing the request headers if the request has an Expect header. 0 to send the request body immediately.
  * `-blocks-storage.s3.max-idle-connections`: Maximum number of idle (keep-alive) connections across all hosts. 0 means no limit.
  * `-blocks-storage.s3.max-idle-connections-per-host`: Maximum number of idle (keep-alive) connections to keep per-host. If 0, a built-in default value is used.
  * `-blocks-storage.s3.max-connections-per-host`: Maximum number of connections per host. 0 means no limit.
* [ENHANCEMENT] Ingester: when tenant's TSDB is closed, Ingester now removes pushed metrics-metadata from memory, and removes metadata (`cortex_ingester_memory_metadata`, `cortex_ingester_memory_metadata_created_total`, `cortex_ingester_memory_metadata_removed_total`) and validation metrics (`cortex_discarded_samples_total`, `cortex_discarded_metadata_total`). #3782
* [ENHANCEMENT] Distributor: cleanup metrics for inactive tenants. #3784
* [ENHANCEMENT] Ingester: Have ingester to re-emit following TSDB metrics. #3800
  * `cortex_ingester_tsdb_blocks_loaded`
  * `cortex_ingester_tsdb_reloads_total`
  * `cortex_ingester_tsdb_reloads_failures_total`
  * `cortex_ingester_tsdb_symbol_table_size_bytes`
  * `cortex_ingester_tsdb_storage_blocks_bytes`
  * `cortex_ingester_tsdb_time_retentions_total`
* [ENHANCEMENT] Querier: distribute workload across `-store-gateway.sharding-ring.replication-factor` store-gateway replicas when querying blocks and `-store-gateway.sharding-enabled=true`. #3824
* [ENHANCEMENT] Distributor / HA Tracker: added cleanup of unused elected HA replicas from KV store. Added following metrics to monitor this process: #3809
  * `cortex_ha_tracker_replicas_cleanup_started_total`
  * `cortex_ha_tracker_replicas_cleanup_marked_for_deletion_total`
  * `cortex_ha_tracker_replicas_cleanup_deleted_total`
  * `cortex_ha_tracker_replicas_cleanup_delete_failed_total`
* [ENHANCEMENT] Ruler now has new API endpoint `/ruler/delete_tenant_config` that can be used to delete all ruler groups for tenant. It is intended to be used by administrators who wish to clean up state after removed user. Note that this endpoint is enabled regardless of `-experimental.ruler.enable-api`. #3750 #3899
* [ENHANCEMENT] Query-frontend, query-scheduler: cleanup metrics for inactive tenants. #3826
* [ENHANCEMENT] Blocks storage: added `-blocks-storage.s3.region` support to S3 client configuration. #3811
* [ENHANCEMENT] Distributor: Remove cached subrings for inactive users when using shuffle sharding. #3849
* [ENHANCEMENT] Store-gateway: Reduced memory used to fetch chunks at query time. #3855
* [ENHANCEMENT] Ingester: attempt to prevent idle compaction from happening in concurrent ingesters by introducing a 25% jitter to the configured idle timeout (`-blocks-storage.tsdb.head-compaction-idle-timeout`). #3850
* [ENHANCEMENT] Compactor: cleanup local files for users that are no longer owned by compactor. #3851
* [ENHANCEMENT] Store-gateway: close empty bucket stores, and delete leftover local files for tenants that no longer belong to store-gateway. #3853
* [ENHANCEMENT] Store-gateway: added metrics to track partitioner behaviour. #3877
  * `cortex_bucket_store_partitioner_requested_bytes_total`
  * `cortex_bucket_store_partitioner_requested_ranges_total`
  * `cortex_bucket_store_partitioner_expanded_bytes_total`
  * `cortex_bucket_store_partitioner_expanded_ranges_total`
* [ENHANCEMENT] Store-gateway: added metrics to monitor chunk buffer pool behaviour. #3880
  * `cortex_bucket_store_chunk_pool_requested_bytes_total`
  * `cortex_bucket_store_chunk_pool_returned_bytes_total`
* [ENHANCEMENT] Alertmanager: load alertmanager configurations from object storage concurrently, and only load necessary configurations, speeding configuration synchronization process and executing fewer "GET object" operations to the storage when sharding is enabled. #3898
* [ENHANCEMENT] Ingester (blocks storage): Ingester can now stream entire chunks instead of individual samples to the querier. At the moment this feature must be explicitly enabled either by using `-ingester.stream-chunks-when-using-blocks` flag or `ingester_stream_chunks_when_using_blocks` (boolean) field in runtime config file, but these configuration options are temporary and will be removed when feature is stable. #3889
* [ENHANCEMENT] Alertmanager: New endpoint `/multitenant_alertmanager/delete_tenant_config` to delete configuration for tenant identified by `X-Scope-OrgID` header. This is an internal endpoint, available even if Alertmanager API is not enabled by using `-experimental.alertmanager.enable-api`. #3900
* [ENHANCEMENT] MemCached: Add `max_item_size` support. #3929
* [BUGFIX] Cortex: Fixed issue where fatal errors and various log messages where not logged. #3778
* [BUGFIX] HA Tracker: don't track as error in the `cortex_kv_request_duration_seconds` metric a CAS operation intentionally aborted. #3745
* [BUGFIX] Querier / ruler: do not log "error removing stale clients" if the ring is empty. #3761
* [BUGFIX] Store-gateway: fixed a panic caused by a race condition when the index-header lazy loading is enabled. #3775 #3789
* [BUGFIX] Compactor: fixed "could not guess file size" log when uploading blocks deletion marks to the global location. #3807
* [BUGFIX] Prevent panic at start if the http_prefix setting doesn't have a valid value. #3796
* [BUGFIX] Memberlist: fixed panic caused by race condition in `armon/go-metrics` used by memberlist client. #3725
* [BUGFIX] Querier: returning 422 (instead of 500) when query hits `max_chunks_per_query` limit with block storage. #3895
* [BUGFIX] Alertmanager: Ensure that experimental `/api/v1/alerts` endpoints work when `-http.prefix` is empty. #3905
* [BUGFIX] Chunk store: fix panic in inverted index when deleted fingerprint is no longer in the index. #3543

## Cortex 1.7.1 / 2021-04-27

* [CHANGE] Fix for CVE-2021-31232: Local file disclosure vulnerability when `-experimental.alertmanager.enable-api` is used. The HTTP basic auth `password_file` can be used as an attack vector to send any file content via a webhook. The alertmanager templates can be used as an attack vector to send any file content because the alertmanager can load any text file specified in the templates list.

## Cortex 1.7.0 / 2021-02-23

Note the blocks storage compactor runs a migration task at startup in this version, which can take many minutes and use a lot of RAM.
[Turn this off after first run](https://cortexmetrics.io/docs/blocks-storage/production-tips/#ensure-deletion-marks-migration-is-disabled-after-first-run).

* [CHANGE] FramedSnappy encoding support has been removed from Push and Remote Read APIs. This means Prometheus 1.6 support has been removed and the oldest Prometheus version supported in the remote write is 1.7. #3682
* [CHANGE] Ruler: removed the flag `-ruler.evaluation-delay-duration-deprecated` which was deprecated in 1.4.0. Please use the `ruler_evaluation_delay_duration` per-tenant limit instead. #3694
* [CHANGE] Removed the flags `-<prefix>.grpc-use-gzip-compression` which were deprecated in 1.3.0: #3694
  * `-query-scheduler.grpc-client-config.grpc-use-gzip-compression`: use `-query-scheduler.grpc-client-config.grpc-compression` instead
  * `-frontend.grpc-client-config.grpc-use-gzip-compression`: use `-frontend.grpc-client-config.grpc-compression` instead
  * `-ruler.client.grpc-use-gzip-compression`: use `-ruler.client.grpc-compression` instead
  * `-bigtable.grpc-use-gzip-compression`: use `-bigtable.grpc-compression` instead
  * `-ingester.client.grpc-use-gzip-compression`: use `-ingester.client.grpc-compression` instead
  * `-querier.frontend-client.grpc-use-gzip-compression`: use `-querier.frontend-client.grpc-compression` instead
* [CHANGE] Querier: it's not required to set `-frontend.query-stats-enabled=true` in the querier anymore to enable query statistics logging in the query-frontend. The flag is now required to be configured only in the query-frontend and it will be propagated to the queriers. #3595 #3695
* [CHANGE] Blocks storage: compactor is now required when running a Cortex cluster with the blocks storage, because it also keeps the bucket index updated. #3583
* [CHANGE] Blocks storage: block deletion marks are now stored in a per-tenant global markers/ location too, other than within the block location. The compactor, at startup, will copy deletion marks from the block location to the global location. This migration is required only once, so it can be safely disabled via `-compactor.block-deletion-marks-migration-enabled=false` after new compactor has successfully started at least once in the cluster. #3583
* [CHANGE] OpenStack Swift: the default value for the `-ruler.storage.swift.container-name` and `-swift.container-name` config options has changed from `cortex` to empty string. If you were relying on the default value, please set it back to `cortex`. #3660
* [CHANGE] HA Tracker: configured replica label is now verified against label value length limit (`-validation.max-length-label-value`). #3668
* [CHANGE] Distributor: `extend_writes` field in YAML configuration has moved from `lifecycler` (inside `ingester_config`) to `distributor_config`. This doesn't affect command line option `-distributor.extend-writes`, which stays the same. #3719
* [CHANGE] Alertmanager: Deprecated `-cluster.` CLI flags in favor of their `-alertmanager.cluster.` equivalent. The deprecated flags (and their respective YAML config options) are: #3677
  * `-cluster.listen-address` in favor of `-alertmanager.cluster.listen-address`
  * `-cluster.advertise-address` in favor of `-alertmanager.cluster.advertise-address`
  * `-cluster.peer` in favor of `-alertmanager.cluster.peers`
  * `-cluster.peer-timeout` in favor of `-alertmanager.cluster.peer-timeout`
* [CHANGE] Blocks storage: the default value of `-blocks-storage.bucket-store.sync-interval` has been changed from `5m` to `15m`. #3724
* [FEATURE] Querier: Queries can be federated across multiple tenants. The tenants IDs involved need to be specified separated by a `|` character in the `X-Scope-OrgID` request header. This is an experimental feature, which can be enabled by setting `-tenant-federation.enabled=true` on all Cortex services. #3250
* [FEATURE] Alertmanager: introduced the experimental option `-alertmanager.sharding-enabled` to shard tenants across multiple Alertmanager instances. This feature is still under heavy development and its usage is discouraged. The following new metrics are exported by the Alertmanager: #3664
  * `cortex_alertmanager_ring_check_errors_total`
  * `cortex_alertmanager_sync_configs_total`
  * `cortex_alertmanager_sync_configs_failed_total`
  * `cortex_alertmanager_tenants_discovered`
  * `cortex_alertmanager_tenants_owned`
* [ENHANCEMENT] Allow specifying JAEGER_ENDPOINT instead of sampling server or local agent port. #3682
* [ENHANCEMENT] Blocks storage: introduced a per-tenant bucket index, periodically updated by the compactor, used to avoid full bucket scanning done by queriers, store-gateways and rulers. The bucket index is updated by the compactor during blocks cleanup, on every `-compactor.cleanup-interval`. #3553 #3555 #3561 #3583 #3625 #3711 #3715
* [ENHANCEMENT] Blocks storage: introduced an option `-blocks-storage.bucket-store.bucket-index.enabled` to enable the usage of the bucket index in the querier, store-gateway and ruler. When enabled, the querier, store-gateway and ruler will use the bucket index to find a tenant's blocks instead of running the periodic bucket scan. The following new metrics are exported by the querier and ruler: #3614 #3625
  * `cortex_bucket_index_loads_total`
  * `cortex_bucket_index_load_failures_total`
  * `cortex_bucket_index_load_duration_seconds`
  * `cortex_bucket_index_loaded`
* [ENHANCEMENT] Compactor: exported the following metrics. #3583 #3625
  * `cortex_bucket_blocks_count`: Total number of blocks per tenant in the bucket. Includes blocks marked for deletion, but not partial blocks.
  * `cortex_bucket_blocks_marked_for_deletion_count`: Total number of blocks per tenant marked for deletion in the bucket.
  * `cortex_bucket_blocks_partials_count`: Total number of partial blocks.
  * `cortex_bucket_index_last_successful_update_timestamp_seconds`: Timestamp of the last successful update of a tenant's bucket index.
* [ENHANCEMENT] Ruler: Add `cortex_prometheus_last_evaluation_samples` to expose the number of samples generated by a rule group per tenant. #3582
* [ENHANCEMENT] Memberlist: add status page (/memberlist) with available details about memberlist-based KV store and memberlist cluster. It's also possible to view KV values in Go struct or JSON format, or download for inspection. #3575
* [ENHANCEMENT] Memberlist: client can now keep a size-bounded buffer with sent and received messages and display them in the admin UI (/memberlist) for troubleshooting. #3581 #3602
* [ENHANCEMENT] Blocks storage: added block index attributes caching support to metadata cache. The TTL can be configured via `-blocks-storage.bucket-store.metadata-cache.block-index-attributes-ttl`. #3629
* [ENHANCEMENT] Alertmanager: Add support for Azure blob storage. #3634
* [ENHANCEMENT] Compactor: tenants marked for deletion will now be fully cleaned up after some delay since deletion of last block. Cleanup includes removal of remaining marker files (including tenant deletion mark file) and files under `debug/metas`. #3613
* [ENHANCEMENT] Compactor: retry compaction of a single tenant on failure instead of re-running compaction for all tenants. #3627
* [ENHANCEMENT] Querier: Implement result caching for tenant query federation. #3640
* [ENHANCEMENT] API: Add a `mode` query parameter for the config endpoint: #3645
  * `/config?mode=diff`: Shows the YAML configuration with all values that differ from the defaults.
  * `/config?mode=defaults`: Shows the YAML configuration with all the default values.
* [ENHANCEMENT] OpenStack Swift: added the following config options to OpenStack Swift backend client: #3660
  - Chunks storage: `-swift.auth-version`, `-swift.max-retries`, `-swift.connect-timeout`, `-swift.request-timeout`.
  - Blocks storage: ` -blocks-storage.swift.auth-version`, ` -blocks-storage.swift.max-retries`, ` -blocks-storage.swift.connect-timeout`, ` -blocks-storage.swift.request-timeout`.
  - Ruler: `-ruler.storage.swift.auth-version`, `-ruler.storage.swift.max-retries`, `-ruler.storage.swift.connect-timeout`, `-ruler.storage.swift.request-timeout`.
* [ENHANCEMENT] Disabled in-memory shuffle-sharding subring cache in the store-gateway, ruler and compactor. This should reduce the memory utilisation in these services when shuffle-sharding is enabled, without introducing a significantly increase CPU utilisation. #3601
* [ENHANCEMENT] Shuffle sharding: optimised subring generation used by shuffle sharding. #3601
* [ENHANCEMENT] New /runtime_config endpoint that returns the defined runtime configuration in YAML format. The returned configuration includes overrides. #3639
* [ENHANCEMENT] Query-frontend: included the parameter name failed to validate in HTTP 400 message. #3703
* [ENHANCEMENT] Fail to startup Cortex if provided runtime config is invalid. #3707
* [ENHANCEMENT] Alertmanager: Add flags to customize the cluster configuration: #3667
  * `-alertmanager.cluster.gossip-interval`: The interval between sending gossip messages. By lowering this value (more frequent) gossip messages are propagated across cluster more quickly at the expense of increased bandwidth usage.
  * `-alertmanager.cluster.push-pull-interval`: The interval between gossip state syncs. Setting this interval lower (more frequent) will increase convergence speeds across larger clusters at the expense of increased bandwidth usage.
* [ENHANCEMENT] Distributor: change the error message returned when a received series has too many label values. The new message format has the series at the end and this plays better with Prometheus logs truncation. #3718
  - From: `sample for '<series>' has <value> label names; limit <value>`
  - To: `series has too many labels (actual: <value>, limit: <value>) series: '<series>'`
* [ENHANCEMENT] Improve bucket index loader to handle edge case where new tenant has not had blocks uploaded to storage yet. #3717
* [BUGFIX] Allow `-querier.max-query-lookback` use `y|w|d` suffix like deprecated `-store.max-look-back-period`. #3598
* [BUGFIX] Memberlist: Entry in the ring should now not appear again after using "Forget" feature (unless it's still heartbeating). #3603
* [BUGFIX] Ingester: do not close idle TSDBs while blocks shipping is in progress. #3630 #3632
* [BUGFIX] Ingester: correctly update `cortex_ingester_memory_users` and `cortex_ingester_active_series` when a tenant's idle TSDB is closed, when running Cortex with the blocks storage. #3646
* [BUGFIX] Querier: fix default value incorrectly overriding `-querier.frontend-address` in single-binary mode. #3650
* [BUGFIX] Compactor: delete `deletion-mark.json` at last when deleting a block in order to not leave partial blocks without deletion mark in the bucket if the compactor is interrupted while deleting a block. #3660
* [BUGFIX] Blocks storage: do not cleanup a partially uploaded block when `meta.json` upload fails. Despite failure to upload `meta.json`, this file may in some cases still appear in the bucket later. By skipping early cleanup, we avoid having corrupted blocks in the storage. #3660
* [BUGFIX] Alertmanager: disable access to `/alertmanager/metrics` (which exposes all Cortex metrics), `/alertmanager/-/reload` and `/alertmanager/debug/*`, which were available to any authenticated user with enabled AlertManager. #3678
* [BUGFIX] Query-Frontend: avoid creating many small sub-queries by discarding cache extents under 5 minutes #3653
* [BUGFIX] Ruler: Ensure the stale markers generated for evaluated rules respect the configured `-ruler.evaluation-delay-duration`. This will avoid issues with samples with NaN be persisted with timestamps set ahead of the next rule evaluation. #3687
* [BUGFIX] Alertmanager: don't serve HTTP requests until Alertmanager has fully started. Serving HTTP requests earlier may result in loss of configuration for the user. #3679
* [BUGFIX] Do not log "failed to load config" if runtime config file is empty. #3706
* [BUGFIX] Do not allow to use a runtime config file containing multiple YAML documents. #3706
* [BUGFIX] HA Tracker: don't track as error in the `cortex_kv_request_duration_seconds` metric a CAS operation intentionally aborted. #3745

## Cortex 1.6.0 / 2020-12-29

* [CHANGE] Query Frontend: deprecate `-querier.compress-http-responses` in favour of `-api.response-compression-enabled`. #3544
* [CHANGE] Querier: deprecated `-store.max-look-back-period`. You should use `-querier.max-query-lookback` instead. #3452
* [CHANGE] Blocks storage: increased `-blocks-storage.bucket-store.chunks-cache.attributes-ttl` default from `24h` to `168h` (1 week). #3528
* [CHANGE] Blocks storage: the config option `-blocks-storage.bucket-store.index-cache.postings-compression-enabled` has been deprecated and postings compression is always enabled. #3538
* [CHANGE] Ruler: gRPC message size default limits on the Ruler-client side have changed: #3523
  - limit for outgoing gRPC messages has changed from 2147483647 to 16777216 bytes
  - limit for incoming gRPC messages has changed from 4194304 to 104857600 bytes
* [FEATURE] Distributor/Ingester: Provide ability to not overflow writes in the presence of a leaving or unhealthy ingester. This allows for more efficient ingester rolling restarts. #3305
* [FEATURE] Query-frontend: introduced query statistics logged in the query-frontend when enabled via `-frontend.query-stats-enabled=true`. When enabled, the metric `cortex_query_seconds_total` is tracked, counting the sum of the wall time spent across all queriers while running queries (on a per-tenant basis). The metrics `cortex_request_duration_seconds` and `cortex_query_seconds_total` are different: the first one tracks the request duration (eg. HTTP request from the client), while the latter tracks the sum of the wall time on all queriers involved executing the query. #3539
* [ENHANCEMENT] API: Add GZIP HTTP compression to the API responses. Compression can be enabled via `-api.response-compression-enabled`. #3536
* [ENHANCEMENT] Added zone-awareness support on queries. When zone-awareness is enabled, queries will still succeed if all ingesters in a single zone will fail. #3414
* [ENHANCEMENT] Blocks storage ingester: exported more TSDB-related metrics. #3412
  - `cortex_ingester_tsdb_wal_corruptions_total`
  - `cortex_ingester_tsdb_head_truncations_failed_total`
  - `cortex_ingester_tsdb_head_truncations_total`
  - `cortex_ingester_tsdb_head_gc_duration_seconds`
* [ENHANCEMENT] Enforced keepalive on all gRPC clients used for inter-service communication. #3431
* [ENHANCEMENT] Added `cortex_alertmanager_config_hash` metric to expose hash of Alertmanager Config loaded per user. #3388
* [ENHANCEMENT] Query-Frontend / Query-Scheduler: New component called "Query-Scheduler" has been introduced. Query-Scheduler is simply a queue of requests, moved outside of Query-Frontend. This allows Query-Frontend to be scaled separately from number of queues. To make Query-Frontend and Querier use Query-Scheduler, they need to be started with `-frontend.scheduler-address` and `-querier.scheduler-address` options respectively. #3374 #3471
* [ENHANCEMENT] Query-frontend / Querier / Ruler: added `-querier.max-query-lookback` to limit how long back data (series and metadata) can be queried. This setting can be overridden on a per-tenant basis and is enforced in the query-frontend, querier and ruler. #3452 #3458
* [ENHANCEMENT] Querier: added `-querier.query-store-for-labels-enabled` to query store for label names, label values and series APIs. Only works with blocks storage engine. #3461 #3520
* [ENHANCEMENT] Ingester: exposed `-blocks-storage.tsdb.wal-segment-size-bytes` config option to customise the TSDB WAL segment max size. #3476
* [ENHANCEMENT] Compactor: concurrently run blocks cleaner for multiple tenants. Concurrency can be configured via `-compactor.cleanup-concurrency`. #3483
* [ENHANCEMENT] Compactor: shuffle tenants before running compaction. #3483
* [ENHANCEMENT] Compactor: wait for a stable ring at startup, when sharding is enabled. #3484
* [ENHANCEMENT] Store-gateway: added `-blocks-storage.bucket-store.index-header-lazy-loading-enabled` to enable index-header lazy loading (experimental). When enabled, index-headers will be mmap-ed only once required by a query and will be automatically released after `-blocks-storage.bucket-store.index-header-lazy-loading-idle-timeout` time of inactivity. #3498
* [ENHANCEMENT] Alertmanager: added metrics `cortex_alertmanager_notification_requests_total` and `cortex_alertmanager_notification_requests_failed_total`. #3518
* [ENHANCEMENT] Ingester: added `-blocks-storage.tsdb.head-chunks-write-buffer-size-bytes` to fine-tune the TSDB head chunks write buffer size when running Cortex blocks storage. #3518
* [ENHANCEMENT] /metrics now supports OpenMetrics output. HTTP and gRPC servers metrics can now include exemplars. #3524
* [ENHANCEMENT] Expose gRPC keepalive policy options by gRPC server. #3524
* [ENHANCEMENT] Blocks storage: enabled caching of `meta.json` attributes, configurable via `-blocks-storage.bucket-store.metadata-cache.metafile-attributes-ttl`. #3528
* [ENHANCEMENT] Compactor: added a config validation check to fail fast if the compactor has been configured invalid block range periods (each period is expected to be a multiple of the previous one). #3534
* [ENHANCEMENT] Blocks storage: concurrently fetch deletion marks from object storage. #3538
* [ENHANCEMENT] Blocks storage ingester: ingester can now close idle TSDB and delete local data. #3491 #3552
* [ENHANCEMENT] Blocks storage: add option to use V2 signatures for S3 authentication. #3540
* [ENHANCEMENT] Exported process metrics to monitor the number of memory map areas allocated. #3537
  * - `process_memory_map_areas`
  * - `process_memory_map_areas_limit`
* [ENHANCEMENT] Ruler: Expose gRPC client options. #3523
* [ENHANCEMENT] Compactor: added metrics to track on-going compaction. #3535
  * `cortex_compactor_tenants_discovered`
  * `cortex_compactor_tenants_skipped`
  * `cortex_compactor_tenants_processing_succeeded`
  * `cortex_compactor_tenants_processing_failed`
* [ENHANCEMENT] Added new experimental API endpoints: `POST /purger/delete_tenant` and `GET /purger/delete_tenant_status` for deleting all tenant data. Only works with blocks storage. Compactor removes blocks that belong to user marked for deletion. #3549 #3558
* [ENHANCEMENT] Chunks storage: add option to use V2 signatures for S3 authentication. #3560
* [ENHANCEMENT] HA Tracker: Added new limit `ha_max_clusters` to set the max number of clusters tracked for single user. This limit is disabled by default. #3668
* [BUGFIX] Query-Frontend: `cortex_query_seconds_total` now return seconds not nanoseconds. #3589
* [BUGFIX] Blocks storage ingester: fixed some cases leading to a TSDB WAL corruption after a partial write to disk. #3423
* [BUGFIX] Blocks storage: Fix the race between ingestion and `/flush` call resulting in overlapping blocks. #3422
* [BUGFIX] Querier: fixed `-querier.max-query-into-future` which wasn't correctly enforced on range queries. #3452
* [BUGFIX] Fixed float64 precision stability when aggregating metrics before exposing them. This could have lead to false counters resets when querying some metrics exposed by Cortex. #3506
* [BUGFIX] Querier: the meta.json sync concurrency done when running Cortex with the blocks storage is now controlled by `-blocks-storage.bucket-store.meta-sync-concurrency` instead of the incorrect `-blocks-storage.bucket-store.block-sync-concurrency` (default values are the same). #3531
* [BUGFIX] Querier: fixed initialization order of querier module when using blocks storage. It now (again) waits until blocks have been synchronized. #3551

### Blocksconvert

* [ENHANCEMENT] Scheduler: ability to ignore users based on regexp, using `-scheduler.ignore-users-regex` flag. #3477
* [ENHANCEMENT] Builder: Parallelize reading chunks in the final stage of building block. #3470
* [ENHANCEMENT] Builder: remove duplicate label names from chunk. #3547

## Cortex 1.5.0 / 2020-11-09

* [CHANGE] Blocks storage: update the default HTTP configuration values for the S3 client to the upstream Thanos default values. #3244
  - `-blocks-storage.s3.http.idle-conn-timeout` is set 90 seconds.
  - `-blocks-storage.s3.http.response-header-timeout` is set to 2 minutes.
* [CHANGE] Improved shuffle sharding support in the write path. This work introduced some config changes: #3090
  * Introduced `-distributor.sharding-strategy` CLI flag (and its respective `sharding_strategy` YAML config option) to explicitly specify which sharding strategy should be used in the write path
  * `-experimental.distributor.user-subring-size` flag renamed to `-distributor.ingestion-tenant-shard-size`
  * `user_subring_size` limit YAML config option renamed to `ingestion_tenant_shard_size`
* [CHANGE] Dropped "blank Alertmanager configuration; using fallback" message from Info to Debug level. #3205
* [CHANGE] Zone-awareness replication for time-series now should be explicitly enabled in the distributor via the `-distributor.zone-awareness-enabled` CLI flag (or its respective YAML config option). Before, zone-aware replication was implicitly enabled if a zone was set on ingesters. #3200
* [CHANGE] Removed the deprecated CLI flag `-config-yaml`. You should use `-schema-config-file` instead. #3225
* [CHANGE] Enforced the HTTP method required by some API endpoints which did (incorrectly) allow any method before that. #3228
  - `GET /`
  - `GET /config`
  - `GET /debug/fgprof`
  - `GET /distributor/all_user_stats`
  - `GET /distributor/ha_tracker`
  - `GET /all_user_stats`
  - `GET /ha-tracker`
  - `GET /api/v1/user_stats`
  - `GET /api/v1/chunks`
  - `GET <legacy-http-prefix>/user_stats`
  - `GET <legacy-http-prefix>/chunks`
  - `GET /services`
  - `GET /multitenant_alertmanager/status`
  - `GET /status` (alertmanager microservice)
  - `GET|POST /ingester/ring`
  - `GET|POST /ring`
  - `GET|POST /store-gateway/ring`
  - `GET|POST /compactor/ring`
  - `GET|POST /ingester/flush`
  - `GET|POST /ingester/shutdown`
  - `GET|POST /flush`
  - `GET|POST /shutdown`
  - `GET|POST /ruler/ring`
  - `POST /api/v1/push`
  - `POST <legacy-http-prefix>/push`
  - `POST /push`
  - `POST /ingester/push`
* [CHANGE] Renamed CLI flags to configure the network interface names from which automatically detect the instance IP. #3295
  - `-compactor.ring.instance-interface` renamed to `-compactor.ring.instance-interface-names`
  - `-store-gateway.sharding-ring.instance-interface` renamed to `-store-gateway.sharding-ring.instance-interface-names`
  - `-distributor.ring.instance-interface` renamed to `-distributor.ring.instance-interface-names`
  - `-ruler.ring.instance-interface` renamed to `-ruler.ring.instance-interface-names`
* [CHANGE] Renamed `-<prefix>.redis.enable-tls` CLI flag to `-<prefix>.redis.tls-enabled`, and its respective YAML config option from `enable_tls` to `tls_enabled`. #3298
* [CHANGE] Increased default `-<prefix>.redis.timeout` from `100ms` to `500ms`. #3301
* [CHANGE] `cortex_alertmanager_config_invalid` has been removed in favor of `cortex_alertmanager_config_last_reload_successful`. #3289
* [CHANGE] Query-frontend: POST requests whose body size exceeds 10MiB will be rejected. The max body size can be customised via `-frontend.max-body-size`. #3276
* [FEATURE] Shuffle sharding: added support for shuffle-sharding queriers in the query-frontend. When configured (`-frontend.max-queriers-per-tenant` globally, or using per-tenant limit `max_queriers_per_tenant`), each tenants's requests will be handled by different set of queriers. #3113 #3257
* [FEATURE] Shuffle sharding: added support for shuffle-sharding ingesters on the read path. When ingesters shuffle-sharding is enabled and `-querier.shuffle-sharding-ingesters-lookback-period` is set, queriers will fetch in-memory series from the minimum set of required ingesters, selecting only ingesters which may have received series since 'now - lookback period'. #3252
* [FEATURE] Query-frontend: added `compression` config to support results cache with compression. #3217
* [FEATURE] Add OpenStack Swift support to blocks storage. #3303
* [FEATURE] Added support for applying Prometheus relabel configs on series received by the distributor. A `metric_relabel_configs` field has been added to the per-tenant limits configuration. #3329
* [FEATURE] Support for Cassandra client SSL certificates. #3384
* [ENHANCEMENT] Ruler: Introduces two new limits `-ruler.max-rules-per-rule-group` and `-ruler.max-rule-groups-per-tenant` to control the number of rules per rule group and the total number of rule groups for a given user. They are disabled by default. #3366
* [ENHANCEMENT] Allow to specify multiple comma-separated Cortex services to `-target` CLI option (or its respective YAML config option). For example, `-target=all,compactor` can be used to start Cortex single-binary with compactor as well. #3275
* [ENHANCEMENT] Expose additional HTTP configs for the S3 backend client. New flag are listed below: #3244
  - `-blocks-storage.s3.http.idle-conn-timeout`
  - `-blocks-storage.s3.http.response-header-timeout`
  - `-blocks-storage.s3.http.insecure-skip-verify`
* [ENHANCEMENT] Added `cortex_query_frontend_connected_clients` metric to show the number of workers currently connected to the frontend. #3207
* [ENHANCEMENT] Shuffle sharding: improved shuffle sharding in the write path. Shuffle sharding now should be explicitly enabled via `-distributor.sharding-strategy` CLI flag (or its respective YAML config option) and guarantees stability, consistency, shuffling and balanced zone-awareness properties. #3090 #3214
* [ENHANCEMENT] Ingester: added new metric `cortex_ingester_active_series` to track active series more accurately. Also added options to control whether active series tracking is enabled (`-ingester.active-series-metrics-enabled`, defaults to false), and how often this metric is updated (`-ingester.active-series-metrics-update-period`) and max idle time for series to be considered inactive (`-ingester.active-series-metrics-idle-timeout`). #3153
* [ENHANCEMENT] Store-gateway: added zone-aware replication support to blocks replication in the store-gateway. #3200
* [ENHANCEMENT] Store-gateway: exported new metrics. #3231
  - `cortex_bucket_store_cached_series_fetch_duration_seconds`
  - `cortex_bucket_store_cached_postings_fetch_duration_seconds`
  - `cortex_bucket_stores_gate_queries_max`
* [ENHANCEMENT] Added `-version` flag to Cortex. #3233
* [ENHANCEMENT] Hash ring: added instance registered timestamp to the ring. #3248
* [ENHANCEMENT] Reduce tail latency by smoothing out spikes in rate of chunk flush operations. #3191
* [ENHANCEMENT] User Cortex as User Agent in http requests issued by Configs DB client. #3264
* [ENHANCEMENT] Experimental Ruler API: Fetch rule groups from object storage in parallel. #3218
* [ENHANCEMENT] Chunks GCS object storage client uses the `fields` selector to limit the payload size when listing objects in the bucket. #3218 #3292
* [ENHANCEMENT] Added shuffle sharding support to ruler. Added new metric `cortex_ruler_sync_rules_total`. #3235
* [ENHANCEMENT] Return an explicit error when the store-gateway is explicitly requested without a blocks storage engine. #3287
* [ENHANCEMENT] Ruler: only load rules that belong to the ruler. Improves rules synching performances when ruler sharding is enabled. #3269
* [ENHANCEMENT] Added `-<prefix>.redis.tls-insecure-skip-verify` flag. #3298
* [ENHANCEMENT] Added `cortex_alertmanager_config_last_reload_successful_seconds` metric to show timestamp of last successful AM config reload. #3289
* [ENHANCEMENT] Blocks storage: reduced number of bucket listing operations to list block content (applies to newly created blocks only). #3363
* [ENHANCEMENT] Ruler: Include the tenant ID on the notifier logs. #3372
* [ENHANCEMENT] Blocks storage Compactor: Added `-compactor.enabled-tenants` and `-compactor.disabled-tenants` to explicitly enable or disable compaction of specific tenants. #3385
* [ENHANCEMENT] Blocks storage ingester: Creating checkpoint only once even when there are multiple Head compactions in a single `Compact()` call. #3373
* [BUGFIX] Blocks storage ingester: Read repair memory-mapped chunks file which can end up being empty on abrupt shutdowns combined with faulty disks. #3373
* [BUGFIX] Blocks storage ingester: Close TSDB resources on failed startup preventing ingester OOMing. #3373
* [BUGFIX] No-longer-needed ingester operations for queries triggered by queriers and rulers are now canceled. #3178
* [BUGFIX] Ruler: directories in the configured `rules-path` will be removed on startup and shutdown in order to ensure they don't persist between runs. #3195
* [BUGFIX] Handle hash-collisions in the query path. #3192
* [BUGFIX] Check for postgres rows errors. #3197
* [BUGFIX] Ruler Experimental API: Don't allow rule groups without names or empty rule groups. #3210
* [BUGFIX] Experimental Alertmanager API: Do not allow empty Alertmanager configurations or bad template filenames to be submitted through the configuration API. #3185
* [BUGFIX] Reduce failures to update heartbeat when using Consul. #3259
* [BUGFIX] When using ruler sharding, moving all user rule groups from ruler to a different one and then back could end up with some user groups not being evaluated at all. #3235
* [BUGFIX] Fixed shuffle sharding consistency when zone-awareness is enabled and the shard size is increased or instances in a new zone are added. #3299
* [BUGFIX] Use a valid grpc header when logging IP addresses. #3307
* [BUGFIX] Fixed the metric `cortex_prometheus_rule_group_duration_seconds` in the Ruler, it wouldn't report any values. #3310
* [BUGFIX] Fixed gRPC connections leaking in rulers when rulers sharding is enabled and APIs called. #3314
* [BUGFIX] Fixed shuffle sharding consistency when zone-awareness is enabled and the shard size is increased or instances in a new zone are added. #3299
* [BUGFIX] Fixed Gossip memberlist members joining when addresses are configured using DNS-based service discovery. #3360
* [BUGFIX] Ingester: fail to start an ingester running the blocks storage, if unable to load any existing TSDB at startup. #3354
* [BUGFIX] Blocks storage: Avoid deletion of blocks in the ingester which are not shipped to the storage yet. #3346
* [BUGFIX] Fix common prefixes returned by List method of S3 client. #3358
* [BUGFIX] Honor configured timeout in Azure and GCS object clients. #3285
* [BUGFIX] Blocks storage: Avoid creating blocks larger than configured block range period on forced compaction and when TSDB is idle. #3344
* [BUGFIX] Shuffle sharding: fixed max global series per user/metric limit when shuffle sharding and `-distributor.shard-by-all-labels=true` are both enabled in distributor. When using these global limits you should now set `-distributor.sharding-strategy` and `-distributor.zone-awareness-enabled` to ingesters too. #3369
* [BUGFIX] Slow query logging: when using downstream server request parameters were not logged. #3276
* [BUGFIX] Fixed tenant detection in the ruler and alertmanager API when running without auth. #3343

### Blocksconvert

* [ENHANCEMENT] Blocksconvert – Builder: download plan file locally before processing it. #3209
* [ENHANCEMENT] Blocksconvert – Cleaner: added new tool for deleting chunks data. #3283
* [ENHANCEMENT] Blocksconvert – Scanner: support for scanning specific date-range only. #3222
* [ENHANCEMENT] Blocksconvert – Scanner: metrics for tracking progress. #3222
* [ENHANCEMENT] Blocksconvert – Builder: retry block upload before giving up. #3245
* [ENHANCEMENT] Blocksconvert – Scanner: upload plans concurrently. #3340
* [BUGFIX] Blocksconvert: fix chunks ordering in the block. Chunks in different order than series work just fine in TSDB blocks at the moment, but it's not consistent with what Prometheus does and future Prometheus and Cortex optimizations may rely on this ordering. #3371

## Cortex 1.4.0 / 2020-10-02

* [CHANGE] TLS configuration for gRPC, HTTP and etcd clients is now marked as experimental. These features are not yet fully baked, and we expect possible small breaking changes in Cortex 1.5. #3198
* [CHANGE] Cassandra backend support is now GA (stable). #3180
* [CHANGE] Blocks storage is now GA (stable). The `-experimental` prefix has been removed from all CLI flags related to the blocks storage (no YAML config changes). #3180 #3201
  - `-experimental.blocks-storage.*` flags renamed to `-blocks-storage.*`
  - `-experimental.store-gateway.*` flags renamed to `-store-gateway.*`
  - `-experimental.querier.store-gateway-client.*` flags renamed to `-querier.store-gateway-client.*`
  - `-experimental.querier.store-gateway-addresses` flag renamed to `-querier.store-gateway-addresses`
  - `-store-gateway.replication-factor` flag renamed to `-store-gateway.sharding-ring.replication-factor`
  - `-store-gateway.tokens-file-path` flag renamed to `store-gateway.sharding-ring.tokens-file-path`
* [CHANGE] Ingester: Removed deprecated untyped record from chunks WAL. Only if you are running `v1.0` or below, it is recommended to first upgrade to `v1.1`/`v1.2`/`v1.3` and run it for a day before upgrading to `v1.4` to avoid data loss. #3115
* [CHANGE] Distributor API endpoints are no longer served unless target is set to `distributor` or `all`. #3112
* [CHANGE] Increase the default Cassandra client replication factor to 3. #3007
* [CHANGE] Blocks storage: removed the support to transfer blocks between ingesters on shutdown. When running the Cortex blocks storage, ingesters are expected to run with a persistent disk. The following metrics have been removed: #2996
  * `cortex_ingester_sent_files`
  * `cortex_ingester_received_files`
  * `cortex_ingester_received_bytes_total`
  * `cortex_ingester_sent_bytes_total`
* [CHANGE] The buckets for the `cortex_chunk_store_index_lookups_per_query` metric have been changed to 1, 2, 4, 8, 16. #3021
* [CHANGE] Blocks storage: the `operation` label value `getrange` has changed into `get_range` for the metrics `thanos_store_bucket_cache_operation_requests_total` and `thanos_store_bucket_cache_operation_hits_total`. #3000
* [CHANGE] Experimental Delete Series: `/api/v1/admin/tsdb/delete_series` and `/api/v1/admin/tsdb/cancel_delete_request` purger APIs to return status code `204` instead of `200` for success. #2946
* [CHANGE] Histogram `cortex_memcache_request_duration_seconds` `method` label value changes from `Memcached.Get` to `Memcached.GetBatched` for batched lookups, and is not reported for non-batched lookups (label value `Memcached.GetMulti` remains, and had exactly the same value as `Get` in nonbatched lookups).  The same change applies to tracing spans. #3046
* [CHANGE] TLS server validation is now enabled by default, a new parameter `tls_insecure_skip_verify` can be set to true to skip validation optionally. #3030
* [CHANGE] `cortex_ruler_config_update_failures_total` has been removed in favor of `cortex_ruler_config_last_reload_successful`. #3056
* [CHANGE] `ruler.evaluation_delay_duration` field in YAML config has been moved and renamed to `limits.ruler_evaluation_delay_duration`. #3098
* [CHANGE] Removed obsolete `results_cache.max_freshness` from YAML config (deprecated since Cortex 1.2). #3145
* [CHANGE] Removed obsolete `-promql.lookback-delta` option (deprecated since Cortex 1.2, replaced with `-querier.lookback-delta`). #3144
* [CHANGE] Cache: added support for Redis Cluster and Redis Sentinel. #2961
  - The following changes have been made in Redis configuration:
   - `-redis.master_name` added
   - `-redis.db` added
   - `-redis.max-active-conns` changed to `-redis.pool-size`
   - `-redis.max-conn-lifetime` changed to `-redis.max-connection-age`
   - `-redis.max-idle-conns` removed
   - `-redis.wait-on-pool-exhaustion` removed
* [CHANGE] TLS configuration for gRPC, HTTP and etcd clients is now marked as experimental. These features are not yet fully baked, and we expect possible small breaking changes in Cortex 1.5. #3198
* [CHANGE] Fixed store-gateway CLI flags inconsistencies. #3201
  - `-store-gateway.replication-factor` flag renamed to `-store-gateway.sharding-ring.replication-factor`
  - `-store-gateway.tokens-file-path` flag renamed to `store-gateway.sharding-ring.tokens-file-path`
* [FEATURE] Logging of the source IP passed along by a reverse proxy is now supported by setting the `-server.log-source-ips-enabled`. For non standard headers the settings `-server.log-source-ips-header` and `-server.log-source-ips-regex` can be used. #2985
* [FEATURE] Blocks storage: added shuffle sharding support to store-gateway blocks sharding. Added the following additional metrics to store-gateway: #3069
  * `cortex_bucket_stores_tenants_discovered`
  * `cortex_bucket_stores_tenants_synced`
* [FEATURE] Experimental blocksconvert: introduce an experimental tool `blocksconvert` to migrate long-term storage chunks to blocks. #3092 #3122 #3127 #3162
* [ENHANCEMENT] Improve the Alertmanager logging when serving requests from its API / UI. #3397
* [ENHANCEMENT] Add support for azure storage in China, German and US Government environments. #2988
* [ENHANCEMENT] Query-tee: added a small tolerance to floating point sample values comparison. #2994
* [ENHANCEMENT] Query-tee: add support for doing a passthrough of requests to preferred backend for unregistered routes #3018
* [ENHANCEMENT] Expose `storage.aws.dynamodb.backoff_config` configuration file field. #3026
* [ENHANCEMENT] Added `cortex_request_message_bytes` and `cortex_response_message_bytes` histograms to track received and sent gRPC message and HTTP request/response sizes. Added `cortex_inflight_requests` gauge to track number of inflight gRPC and HTTP requests. #3064
* [ENHANCEMENT] Publish ruler's ring metrics. #3074
* [ENHANCEMENT] Add config validation to the experimental Alertmanager API. Invalid configs are no longer accepted. #3053
* [ENHANCEMENT] Add "integration" as a label for `cortex_alertmanager_notifications_total` and `cortex_alertmanager_notifications_failed_total` metrics. #3056
* [ENHANCEMENT] Add `cortex_ruler_config_last_reload_successful` and `cortex_ruler_config_last_reload_successful_seconds` to check status of users rule manager. #3056
* [ENHANCEMENT] The configuration validation now fails if an empty YAML node has been set for a root YAML config property. #3080
* [ENHANCEMENT] Memcached dial() calls now have a circuit-breaker to avoid hammering a broken cache. #3051, #3189
* [ENHANCEMENT] `-ruler.evaluation-delay-duration` is now overridable as a per-tenant limit, `ruler_evaluation_delay_duration`. #3098
* [ENHANCEMENT] Add TLS support to etcd client. #3102
* [ENHANCEMENT] When a tenant accesses the Alertmanager UI or its API, if we have valid `-alertmanager.configs.fallback` we'll use that to start the manager and avoid failing the request. #3073
* [ENHANCEMENT] Add `DELETE api/v1/rules/{namespace}` to the Ruler. It allows all the rule groups of a namespace to be deleted. #3120
* [ENHANCEMENT] Experimental Delete Series: Retry processing of Delete requests during failures. #2926
* [ENHANCEMENT] Improve performance of QueryStream() in ingesters. #3177
* [ENHANCEMENT] Modules included in "All" target are now visible in output of `-modules` CLI flag. #3155
* [ENHANCEMENT] Added `/debug/fgprof` endpoint to debug running Cortex process using `fgprof`. This adds up to the existing `/debug/...` endpoints. #3131
* [ENHANCEMENT] Blocks storage: optimised `/api/v1/series` for blocks storage. (#2976)
* [BUGFIX] Ruler: when loading rules from "local" storage, check for directory after resolving symlink. #3137
* [BUGFIX] Query-frontend: Fixed rounding for incoming query timestamps, to be 100% Prometheus compatible. #2990
* [BUGFIX] Querier: Merge results from chunks and blocks ingesters when using streaming of results. #3013
* [BUGFIX] Querier: query /series from ingesters regardless the `-querier.query-ingesters-within` setting. #3035
* [BUGFIX] Blocks storage: Ingester is less likely to hit gRPC message size limit when streaming data to queriers. #3015
* [BUGFIX] Blocks storage: fixed memberlist support for the store-gateways and compactors ring used when blocks sharding is enabled. #3058 #3095
* [BUGFIX] Fix configuration for TLS server validation, TLS skip verify was hardcoded to true for all TLS configurations and prevented validation of server certificates. #3030
* [BUGFIX] Fixes the Alertmanager panicking when no `-alertmanager.web.external-url` is provided. #3017
* [BUGFIX] Fixes the registration of the Alertmanager API metrics `cortex_alertmanager_alerts_received_total` and `cortex_alertmanager_alerts_invalid_total`. #3065
* [BUGFIX] Fixes `flag needs an argument: -config.expand-env` error. #3087
* [BUGFIX] An index optimisation actually slows things down when using caching. Moved it to the right location. #2973
* [BUGFIX] Ingester: If push request contained both valid and invalid samples, valid samples were ingested but not stored to WAL of the chunks storage. This has been fixed. #3067
* [BUGFIX] Cassandra: fixed consistency setting in the CQL session when creating the keyspace. #3105
* [BUGFIX] Ruler: Config API would return both the `record` and `alert` in `YAML` response keys even when one of them must be empty. #3120
* [BUGFIX] Index page now uses configured HTTP path prefix when creating links. #3126
* [BUGFIX] Purger: fixed deadlock when reloading of tombstones failed. #3182
* [BUGFIX] Fixed panic in flusher job, when error writing chunks to the store would cause "idle" chunks to be flushed, which triggered panic. #3140
* [BUGFIX] Index page no longer shows links that are not valid for running Cortex instance. #3133
* [BUGFIX] Configs: prevent validation of templates to fail when using template functions. #3157
* [BUGFIX] Configuring the S3 URL with an `@` but without username and password doesn't enable the AWS static credentials anymore. #3170
* [BUGFIX] Limit errors on ranged queries (`api/v1/query_range`) no longer return a status code `500` but `422` instead. #3167
* [BUGFIX] Handle hash-collisions in the query path. Before this fix, Cortex could occasionally mix up two different series in a query, leading to invalid results, when `-querier.ingester-streaming` was used. #3192

## Cortex 1.3.0 / 2020-08-21

* [CHANGE] Replace the metric `cortex_alertmanager_configs` with `cortex_alertmanager_config_invalid` exposed by Alertmanager. #2960
* [CHANGE] Experimental Delete Series: Change target flag for purger from `data-purger` to `purger`. #2777
* [CHANGE] Experimental blocks storage: The max concurrent queries against the long-term storage, configured via `-experimental.blocks-storage.bucket-store.max-concurrent`, is now a limit shared across all tenants and not a per-tenant limit anymore. The default value has changed from `20` to `100` and the following new metrics have been added: #2797
  * `cortex_bucket_stores_gate_queries_concurrent_max`
  * `cortex_bucket_stores_gate_queries_in_flight`
  * `cortex_bucket_stores_gate_duration_seconds`
* [CHANGE] Metric `cortex_ingester_flush_reasons` has been renamed to `cortex_ingester_flushing_enqueued_series_total`, and new metric `cortex_ingester_flushing_dequeued_series_total` with `outcome` label (superset of reason) has been added. #2802 #2818 #2998
* [CHANGE] Experimental Delete Series: Metric `cortex_purger_oldest_pending_delete_request_age_seconds` would track age of delete requests since they are over their cancellation period instead of their creation time. #2806
* [CHANGE] Experimental blocks storage: the store-gateway service is required in a Cortex cluster running with the experimental blocks storage. Removed the `-experimental.tsdb.store-gateway-enabled` CLI flag and `store_gateway_enabled` YAML config option. The store-gateway is now always enabled when the storage engine is `blocks`. #2822
* [CHANGE] Experimental blocks storage: removed support for `-experimental.blocks-storage.bucket-store.max-sample-count` flag because the implementation was flawed. To limit the number of samples/chunks processed by a single query you can set `-store.query-chunk-limit`, which is now supported by the blocks storage too. #2852
* [CHANGE] Ingester: Chunks flushed via /flush stay in memory until retention period is reached. This affects `cortex_ingester_memory_chunks` metric. #2778
* [CHANGE] Querier: the error message returned when the query time range exceeds `-store.max-query-length` has changed from `invalid query, length > limit (X > Y)` to `the query time range exceeds the limit (query length: X, limit: Y)`. #2826
* [CHANGE] Add `component` label to metrics exposed by chunk, delete and index store clients. #2774
* [CHANGE] Querier: when `-querier.query-ingesters-within` is configured, the time range of the query sent to ingesters is now manipulated to ensure the query start time is not older than 'now - query-ingesters-within'. #2904
* [CHANGE] KV: The `role` label which was a label of `multi` KV store client only has been added to metrics of every KV store client. If KV store client is not `multi`, then the value of `role` label is `primary`. #2837
* [CHANGE] Added the `engine` label to the metrics exposed by the Prometheus query engine, to distinguish between `ruler` and `querier` metrics. #2854
* [CHANGE] Added ruler to the single binary when started with `-target=all` (default). #2854
* [CHANGE] Experimental blocks storage: compact head when opening TSDB. This should only affect ingester startup after it was unable to compact head in previous run. #2870
* [CHANGE] Metric `cortex_overrides_last_reload_successful` has been renamed to `cortex_runtime_config_last_reload_successful`. #2874
* [CHANGE] HipChat support has been removed from the alertmanager (because removed from the Prometheus upstream too). #2902
* [CHANGE] Add constant label `name` to metric `cortex_cache_request_duration_seconds`. #2903
* [CHANGE] Add `user` label to metric `cortex_query_frontend_queue_length`. #2939
* [CHANGE] Experimental blocks storage: cleaned up the config and renamed "TSDB" to "blocks storage". #2937
  - The storage engine setting value has been changed from `tsdb` to `blocks`; this affects `-store.engine` CLI flag and its respective YAML option.
  - The root level YAML config has changed from `tsdb` to `blocks_storage`
  - The prefix of all CLI flags has changed from `-experimental.tsdb.` to `-experimental.blocks-storage.`
  - The following settings have been grouped under `tsdb` property in the YAML config and their CLI flags changed:
    - `-experimental.tsdb.dir` changed to `-experimental.blocks-storage.tsdb.dir`
    - `-experimental.tsdb.block-ranges-period` changed to `-experimental.blocks-storage.tsdb.block-ranges-period`
    - `-experimental.tsdb.retention-period` changed to `-experimental.blocks-storage.tsdb.retention-period`
    - `-experimental.tsdb.ship-interval` changed to `-experimental.blocks-storage.tsdb.ship-interval`
    - `-experimental.tsdb.ship-concurrency` changed to `-experimental.blocks-storage.tsdb.ship-concurrency`
    - `-experimental.tsdb.max-tsdb-opening-concurrency-on-startup` changed to `-experimental.blocks-storage.tsdb.max-tsdb-opening-concurrency-on-startup`
    - `-experimental.tsdb.head-compaction-interval` changed to `-experimental.blocks-storage.tsdb.head-compaction-interval`
    - `-experimental.tsdb.head-compaction-concurrency` changed to `-experimental.blocks-storage.tsdb.head-compaction-concurrency`
    - `-experimental.tsdb.head-compaction-idle-timeout` changed to `-experimental.blocks-storage.tsdb.head-compaction-idle-timeout`
    - `-experimental.tsdb.stripe-size` changed to `-experimental.blocks-storage.tsdb.stripe-size`
    - `-experimental.tsdb.wal-compression-enabled` changed to `-experimental.blocks-storage.tsdb.wal-compression-enabled`
    - `-experimental.tsdb.flush-blocks-on-shutdown` changed to `-experimental.blocks-storage.tsdb.flush-blocks-on-shutdown`
* [CHANGE] Flags `-bigtable.grpc-use-gzip-compression`, `-ingester.client.grpc-use-gzip-compression`, `-querier.frontend-client.grpc-use-gzip-compression` are now deprecated. #2940
* [CHANGE] Limit errors reported by ingester during query-time now return HTTP status code 422. #2941
* [FEATURE] Introduced `ruler.for-outage-tolerance`, Max time to tolerate outage for restoring "for" state of alert. #2783
* [FEATURE] Introduced `ruler.for-grace-period`, Minimum duration between alert and restored "for" state. This is maintained only for alerts with configured "for" time greater than grace period. #2783
* [FEATURE] Introduced `ruler.resend-delay`, Minimum amount of time to wait before resending an alert to Alertmanager. #2783
* [FEATURE] Ruler: added `local` filesystem support to store rules (read-only). #2854
* [ENHANCEMENT] Upgraded Docker base images to `alpine:3.12`. #2862
* [ENHANCEMENT] Experimental: Querier can now optionally query secondary store. This is specified by using `-querier.second-store-engine` option, with values `chunks` or `blocks`. Standard configuration options for this store are used. Additionally, this querying can be configured to happen only for queries that need data older than `-querier.use-second-store-before-time`. Default value of zero will always query secondary store. #2747
* [ENHANCEMENT] Query-tee: increased the `cortex_querytee_request_duration_seconds` metric buckets granularity. #2799
* [ENHANCEMENT] Query-tee: fail to start if the configured `-backend.preferred` is unknown. #2799
* [ENHANCEMENT] Ruler: Added the following metrics: #2786
  * `cortex_prometheus_notifications_latency_seconds`
  * `cortex_prometheus_notifications_errors_total`
  * `cortex_prometheus_notifications_sent_total`
  * `cortex_prometheus_notifications_dropped_total`
  * `cortex_prometheus_notifications_queue_length`
  * `cortex_prometheus_notifications_queue_capacity`
  * `cortex_prometheus_notifications_alertmanagers_discovered`
* [ENHANCEMENT] The behavior of the `/ready` was changed for the query frontend to indicate when it was ready to accept queries. This is intended for use by a read path load balancer that would want to wait for the frontend to have attached queriers before including it in the backend. #2733
* [ENHANCEMENT] Experimental Delete Series: Add support for deletion of chunks for remaining stores. #2801
* [ENHANCEMENT] Add `-modules` command line flag to list possible values for `-target`. Also, log warning if given target is internal component. #2752
* [ENHANCEMENT] Added `-ingester.flush-on-shutdown-with-wal-enabled` option to enable chunks flushing even when WAL is enabled. #2780
* [ENHANCEMENT] Query-tee: Support for custom API prefix by using `-server.path-prefix` option. #2814
* [ENHANCEMENT] Query-tee: Forward `X-Scope-OrgId` header to backend, if present in the request. #2815
* [ENHANCEMENT] Experimental blocks storage: Added `-experimental.blocks-storage.tsdb.head-compaction-idle-timeout` option to force compaction of data in memory into a block. #2803
* [ENHANCEMENT] Experimental blocks storage: Added support for flushing blocks via `/flush`, `/shutdown` (previously these only worked for chunks storage) and by using `-experimental.blocks-storage.tsdb.flush-blocks-on-shutdown` option. #2794
* [ENHANCEMENT] Experimental blocks storage: Added support to enforce max query time range length via `-store.max-query-length`. #2826
* [ENHANCEMENT] Experimental blocks storage: Added support to limit the max number of chunks that can be fetched from the long-term storage while executing a query. The limit is enforced both in the querier and store-gateway, and is configurable via `-store.query-chunk-limit`. #2852 #2922
* [ENHANCEMENT] Ingester: Added new metric `cortex_ingester_flush_series_in_progress` that reports number of ongoing flush-series operations. Useful when calling `/flush` handler: if `cortex_ingester_flush_queue_length + cortex_ingester_flush_series_in_progress` is 0, all flushes are finished. #2778
* [ENHANCEMENT] Memberlist members can join cluster via SRV records. #2788
* [ENHANCEMENT] Added configuration options for chunks s3 client. #2831
  * `s3.endpoint`
  * `s3.region`
  * `s3.access-key-id`
  * `s3.secret-access-key`
  * `s3.insecure`
  * `s3.sse-encryption`
  * `s3.http.idle-conn-timeout`
  * `s3.http.response-header-timeout`
  * `s3.http.insecure-skip-verify`
* [ENHANCEMENT] Prometheus upgraded. #2798 #2849 #2867 #2902 #2918
  * Optimized labels regex matchers for patterns containing literals (eg. `foo.*`, `.*foo`, `.*foo.*`)
* [ENHANCEMENT] Add metric `cortex_ruler_config_update_failures_total` to Ruler to track failures of loading rules files. #2857
* [ENHANCEMENT] Experimental Alertmanager: Alertmanager configuration persisted to object storage using an experimental API that accepts and returns YAML-based Alertmanager configuration. #2768
* [ENHANCEMENT] Ruler: `-ruler.alertmanager-url` now supports multiple URLs. Each URL is treated as a separate Alertmanager group. Support for multiple Alertmanagers in a group can be achieved by using DNS service discovery. #2851
* [ENHANCEMENT] Experimental blocks storage: Cortex Flusher now works with blocks engine. Flusher needs to be provided with blocks-engine configuration, existing Flusher flags are not used (they are only relevant for chunks engine). Note that flush errors are only reported via log. #2877
* [ENHANCEMENT] Flusher: Added `-flusher.exit-after-flush` option (defaults to true) to control whether Cortex should stop completely after Flusher has finished its work. #2877
* [ENHANCEMENT] Added metrics `cortex_config_hash` and `cortex_runtime_config_hash` to expose hash of the currently active config file. #2874
* [ENHANCEMENT] Logger: added JSON logging support, configured via the `-log.format=json` CLI flag or its respective YAML config option. #2386
* [ENHANCEMENT] Added new flags `-bigtable.grpc-compression`, `-ingester.client.grpc-compression`, `-querier.frontend-client.grpc-compression` to configure compression used by gRPC. Valid values are `gzip`, `snappy`, or empty string (no compression, default). #2940
* [ENHANCEMENT] Clarify limitations of the `/api/v1/series`, `/api/v1/labels` and `/api/v1/label/{name}/values` endpoints. #2953
* [ENHANCEMENT] Ingester: added `Dropped` outcome to metric `cortex_ingester_flushing_dequeued_series_total`. #2998
* [BUGFIX] Fixed a bug with `api/v1/query_range` where no responses would return null values for `result` and empty values for `resultType`. #2962
* [BUGFIX] Fixed a bug in the index intersect code causing storage to return more chunks/series than required. #2796
* [BUGFIX] Fixed the number of reported keys in the background cache queue. #2764
* [BUGFIX] Fix race in processing of headers in sharded queries. #2762
* [BUGFIX] Query Frontend: Do not re-split sharded requests around ingester boundaries. #2766
* [BUGFIX] Experimental Delete Series: Fixed a problem with cache generation numbers prefixed to cache keys. #2800
* [BUGFIX] Ingester: Flushing chunks via `/flush` endpoint could previously lead to panic, if chunks were already flushed before and then removed from memory during the flush caused by `/flush` handler. Immediate flush now doesn't cause chunks to be flushed again. Samples received during flush triggered via `/flush` handler are no longer discarded. #2778
* [BUGFIX] Prometheus upgraded. #2849
  * Fixed unknown symbol error during head compaction
* [BUGFIX] Fix panic when using cassandra as store for both index and delete requests. #2774
* [BUGFIX] Experimental Delete Series: Fixed a data race in Purger. #2817
* [BUGFIX] KV: Fixed a bug that triggered a panic due to metrics being registered with the same name but different labels when using a `multi` configured KV client. #2837
* [BUGFIX] Query-frontend: Fix passing HTTP `Host` header if `-frontend.downstream-url` is configured. #2880
* [BUGFIX] Ingester: Improve time-series distribution when `-experimental.distributor.user-subring-size` is enabled. #2887
* [BUGFIX] Set content type to `application/x-protobuf` for remote_read responses. #2915
* [BUGFIX] Fixed ruler and store-gateway instance registration in the ring (when sharding is enabled) when a new instance replaces abruptly terminated one, and the only difference between the two instances is the address. #2954
* [BUGFIX] Fixed `Missing chunks and index config causing silent failure` Absence of chunks and index from schema config is not validated. #2732
* [BUGFIX] Fix panic caused by KVs from boltdb being used beyond their life. #2971
* [BUGFIX] Experimental blocks storage: `/api/v1/series`, `/api/v1/labels` and `/api/v1/label/{name}/values` only query the TSDB head regardless of the configured `-experimental.blocks-storage.tsdb.retention-period`. #2974
* [BUGFIX] Ingester: Avoid indefinite checkpointing in case of surge in number of series. #2955
* [BUGFIX] Querier: query /series from ingesters regardless the `-querier.query-ingesters-within` setting. #3035
* [BUGFIX] Ruler: fixed an unintentional breaking change introduced in the ruler's `alertmanager_url` YAML config option, which changed the value from a string to a list of strings. #2989

## Cortex 1.2.0 / 2020-07-01

* [CHANGE] Metric `cortex_kv_request_duration_seconds` now includes `name` label to denote which client is being used as well as the `backend` label to denote the KV backend implementation in use. #2648
* [CHANGE] Experimental Ruler: Rule groups persisted to object storage using the experimental API have an updated object key encoding to better handle special characters. Rule groups previously-stored using object storage must be renamed to the new format. #2646
* [CHANGE] Query Frontend now uses Round Robin to choose a tenant queue to service next. #2553
* [CHANGE] `-promql.lookback-delta` is now deprecated and has been replaced by `-querier.lookback-delta` along with `lookback_delta` entry under `querier` in the config file. `-promql.lookback-delta` will be removed in v1.4.0. #2604
* [CHANGE] Experimental TSDB: removed `-experimental.tsdb.bucket-store.binary-index-header-enabled` flag. Now the binary index-header is always enabled.
* [CHANGE] Experimental TSDB: Renamed index-cache metrics to use original metric names from Thanos, as Cortex is not aggregating them in any way: #2627
  * `cortex_<service>_blocks_index_cache_items_evicted_total` => `thanos_store_index_cache_items_evicted_total{name="index-cache"}`
  * `cortex_<service>_blocks_index_cache_items_added_total` => `thanos_store_index_cache_items_added_total{name="index-cache"}`
  * `cortex_<service>_blocks_index_cache_requests_total` => `thanos_store_index_cache_requests_total{name="index-cache"}`
  * `cortex_<service>_blocks_index_cache_items_overflowed_total` => `thanos_store_index_cache_items_overflowed_total{name="index-cache"}`
  * `cortex_<service>_blocks_index_cache_hits_total` => `thanos_store_index_cache_hits_total{name="index-cache"}`
  * `cortex_<service>_blocks_index_cache_items` => `thanos_store_index_cache_items{name="index-cache"}`
  * `cortex_<service>_blocks_index_cache_items_size_bytes` => `thanos_store_index_cache_items_size_bytes{name="index-cache"}`
  * `cortex_<service>_blocks_index_cache_total_size_bytes` => `thanos_store_index_cache_total_size_bytes{name="index-cache"}`
  * `cortex_<service>_blocks_index_cache_memcached_operations_total` =>  `thanos_memcached_operations_total{name="index-cache"}`
  * `cortex_<service>_blocks_index_cache_memcached_operation_failures_total` =>  `thanos_memcached_operation_failures_total{name="index-cache"}`
  * `cortex_<service>_blocks_index_cache_memcached_operation_duration_seconds` =>  `thanos_memcached_operation_duration_seconds{name="index-cache"}`
  * `cortex_<service>_blocks_index_cache_memcached_operation_skipped_total` =>  `thanos_memcached_operation_skipped_total{name="index-cache"}`
* [CHANGE] Experimental TSDB: Renamed metrics in bucket stores: #2627
  * `cortex_<service>_blocks_meta_syncs_total` => `cortex_blocks_meta_syncs_total{component="<service>"}`
  * `cortex_<service>_blocks_meta_sync_failures_total` => `cortex_blocks_meta_sync_failures_total{component="<service>"}`
  * `cortex_<service>_blocks_meta_sync_duration_seconds` => `cortex_blocks_meta_sync_duration_seconds{component="<service>"}`
  * `cortex_<service>_blocks_meta_sync_consistency_delay_seconds` => `cortex_blocks_meta_sync_consistency_delay_seconds{component="<service>"}`
  * `cortex_<service>_blocks_meta_synced` => `cortex_blocks_meta_synced{component="<service>"}`
  * `cortex_<service>_bucket_store_block_loads_total` => `cortex_bucket_store_block_loads_total{component="<service>"}`
  * `cortex_<service>_bucket_store_block_load_failures_total` => `cortex_bucket_store_block_load_failures_total{component="<service>"}`
  * `cortex_<service>_bucket_store_block_drops_total` => `cortex_bucket_store_block_drops_total{component="<service>"}`
  * `cortex_<service>_bucket_store_block_drop_failures_total` => `cortex_bucket_store_block_drop_failures_total{component="<service>"}`
  * `cortex_<service>_bucket_store_blocks_loaded` => `cortex_bucket_store_blocks_loaded{component="<service>"}`
  * `cortex_<service>_bucket_store_series_data_touched` => `cortex_bucket_store_series_data_touched{component="<service>"}`
  * `cortex_<service>_bucket_store_series_data_fetched` => `cortex_bucket_store_series_data_fetched{component="<service>"}`
  * `cortex_<service>_bucket_store_series_data_size_touched_bytes` => `cortex_bucket_store_series_data_size_touched_bytes{component="<service>"}`
  * `cortex_<service>_bucket_store_series_data_size_fetched_bytes` => `cortex_bucket_store_series_data_size_fetched_bytes{component="<service>"}`
  * `cortex_<service>_bucket_store_series_blocks_queried` => `cortex_bucket_store_series_blocks_queried{component="<service>"}`
  * `cortex_<service>_bucket_store_series_get_all_duration_seconds` => `cortex_bucket_store_series_get_all_duration_seconds{component="<service>"}`
  * `cortex_<service>_bucket_store_series_merge_duration_seconds` => `cortex_bucket_store_series_merge_duration_seconds{component="<service>"}`
  * `cortex_<service>_bucket_store_series_refetches_total` => `cortex_bucket_store_series_refetches_total{component="<service>"}`
  * `cortex_<service>_bucket_store_series_result_series` => `cortex_bucket_store_series_result_series{component="<service>"}`
  * `cortex_<service>_bucket_store_cached_postings_compressions_total` => `cortex_bucket_store_cached_postings_compressions_total{component="<service>"}`
  * `cortex_<service>_bucket_store_cached_postings_compression_errors_total` => `cortex_bucket_store_cached_postings_compression_errors_total{component="<service>"}`
  * `cortex_<service>_bucket_store_cached_postings_compression_time_seconds` => `cortex_bucket_store_cached_postings_compression_time_seconds{component="<service>"}`
  * `cortex_<service>_bucket_store_cached_postings_original_size_bytes_total` => `cortex_bucket_store_cached_postings_original_size_bytes_total{component="<service>"}`
  * `cortex_<service>_bucket_store_cached_postings_compressed_size_bytes_total` => `cortex_bucket_store_cached_postings_compressed_size_bytes_total{component="<service>"}`
  * `cortex_<service>_blocks_sync_seconds` => `cortex_bucket_stores_blocks_sync_seconds{component="<service>"}`
  * `cortex_<service>_blocks_last_successful_sync_timestamp_seconds` => `cortex_bucket_stores_blocks_last_successful_sync_timestamp_seconds{component="<service>"}`
* [CHANGE] Available command-line flags are printed to stdout, and only when requested via `-help`. Using invalid flag no longer causes printing of all available flags. #2691
* [CHANGE] Experimental Memberlist ring: randomize gossip node names to avoid conflicts when running multiple clients on the same host, or reusing host names (eg. pods in statefulset). Node name randomization can be disabled by using `-memberlist.randomize-node-name=false`. #2715
* [CHANGE] Memberlist KV client is no longer considered experimental. #2725
* [CHANGE] Experimental Delete Series: Make delete request cancellation duration configurable. #2760
* [CHANGE] Removed `-store.fullsize-chunks` option which was undocumented and unused (it broke ingester hand-overs). #2656
* [CHANGE] Query with no metric name that has previously resulted in HTTP status code 500 now returns status code 422 instead. #2571
* [FEATURE] TLS config options added for GRPC clients in Querier (Query-frontend client & Ingester client), Ruler, Store Gateway, as well as HTTP client in Config store client. #2502
* [FEATURE] The flag `-frontend.max-cache-freshness` is now supported within the limits overrides, to specify per-tenant max cache freshness values. The corresponding YAML config parameter has been changed from `results_cache.max_freshness` to `limits_config.max_cache_freshness`. The legacy YAML config parameter (`results_cache.max_freshness`) will continue to be supported till Cortex release `v1.4.0`. #2609
* [FEATURE] Experimental gRPC Store: Added support to 3rd parties index and chunk stores using gRPC client/server plugin mechanism. #2220
* [FEATURE] Add `-cassandra.table-options` flag to customize table options of Cassandra when creating the index or chunk table. #2575
* [ENHANCEMENT] Propagate GOPROXY value when building `build-image`. This is to help the builders building the code in a Network where default Go proxy is not accessible (e.g. when behind some corporate VPN). #2741
* [ENHANCEMENT] Querier: Added metric `cortex_querier_request_duration_seconds` for all requests to the querier. #2708
* [ENHANCEMENT] Cortex is now built with Go 1.14. #2480 #2749 #2753
* [ENHANCEMENT] Experimental TSDB: added the following metrics to the ingester: #2580 #2583 #2589 #2654
  * `cortex_ingester_tsdb_appender_add_duration_seconds`
  * `cortex_ingester_tsdb_appender_commit_duration_seconds`
  * `cortex_ingester_tsdb_refcache_purge_duration_seconds`
  * `cortex_ingester_tsdb_compactions_total`
  * `cortex_ingester_tsdb_compaction_duration_seconds`
  * `cortex_ingester_tsdb_wal_fsync_duration_seconds`
  * `cortex_ingester_tsdb_wal_page_flushes_total`
  * `cortex_ingester_tsdb_wal_completed_pages_total`
  * `cortex_ingester_tsdb_wal_truncations_failed_total`
  * `cortex_ingester_tsdb_wal_truncations_total`
  * `cortex_ingester_tsdb_wal_writes_failed_total`
  * `cortex_ingester_tsdb_checkpoint_deletions_failed_total`
  * `cortex_ingester_tsdb_checkpoint_deletions_total`
  * `cortex_ingester_tsdb_checkpoint_creations_failed_total`
  * `cortex_ingester_tsdb_checkpoint_creations_total`
  * `cortex_ingester_tsdb_wal_truncate_duration_seconds`
  * `cortex_ingester_tsdb_head_active_appenders`
  * `cortex_ingester_tsdb_head_series_not_found_total`
  * `cortex_ingester_tsdb_head_chunks`
  * `cortex_ingester_tsdb_mmap_chunk_corruptions_total`
  * `cortex_ingester_tsdb_head_chunks_created_total`
  * `cortex_ingester_tsdb_head_chunks_removed_total`
* [ENHANCEMENT] Experimental TSDB: added metrics useful to alert on critical conditions of the blocks storage: #2573
  * `cortex_compactor_last_successful_run_timestamp_seconds`
  * `cortex_querier_blocks_last_successful_sync_timestamp_seconds` (when store-gateway is disabled)
  * `cortex_querier_blocks_last_successful_scan_timestamp_seconds` (when store-gateway is enabled)
  * `cortex_storegateway_blocks_last_successful_sync_timestamp_seconds`
* [ENHANCEMENT] Experimental TSDB: added the flag `-experimental.tsdb.wal-compression-enabled` to allow to enable TSDB WAL compression. #2585
* [ENHANCEMENT] Experimental TSDB: Querier and store-gateway components can now use so-called "caching bucket", which can currently cache fetched chunks into shared memcached server. #2572
* [ENHANCEMENT] Ruler: Automatically remove unhealthy rulers from the ring. #2587
* [ENHANCEMENT] Query-tee: added support to `/metadata`, `/alerts`, and `/rules` endpoints #2600
* [ENHANCEMENT] Query-tee: added support to query results comparison between two different backends. The comparison is disabled by default and can be enabled via `-proxy.compare-responses=true`. #2611
* [ENHANCEMENT] Query-tee: improved the query-tee to not wait all backend responses before sending back the response to the client. The query-tee now sends back to the client first successful response, while honoring the `-backend.preferred` option. #2702
* [ENHANCEMENT] Thanos and Prometheus upgraded. #2602 #2604 #2634 #2659 #2686 #2756
  * TSDB now holds less WAL files after Head Truncation.
  * TSDB now does memory-mapping of Head chunks and reduces memory usage.
* [ENHANCEMENT] Experimental TSDB: decoupled blocks deletion from blocks compaction in the compactor, so that blocks deletion is not blocked by a busy compactor. The following metrics have been added: #2623
  * `cortex_compactor_block_cleanup_started_total`
  * `cortex_compactor_block_cleanup_completed_total`
  * `cortex_compactor_block_cleanup_failed_total`
  * `cortex_compactor_block_cleanup_last_successful_run_timestamp_seconds`
* [ENHANCEMENT] Experimental TSDB: Use shared cache for metadata. This is especially useful when running multiple querier and store-gateway components to reduce number of object store API calls. #2626 #2640
* [ENHANCEMENT] Experimental TSDB: when `-querier.query-store-after` is configured and running the experimental blocks storage, the time range of the query sent to the store is now manipulated to ensure the query end time is not more recent than 'now - query-store-after'. #2642
* [ENHANCEMENT] Experimental TSDB: small performance improvement in concurrent usage of RefCache, used during samples ingestion. #2651
* [ENHANCEMENT] The following endpoints now respond appropriately to an `Accept` header with the value `application/json` #2673
  * `/distributor/all_user_stats`
  * `/distributor/ha_tracker`
  * `/ingester/ring`
  * `/store-gateway/ring`
  * `/compactor/ring`
  * `/ruler/ring`
  * `/services`
* [ENHANCEMENT] Experimental Cassandra backend: Add `-cassandra.num-connections` to allow increasing the number of TCP connections to each Cassandra server. #2666
* [ENHANCEMENT] Experimental Cassandra backend: Use separate Cassandra clients and connections for reads and writes. #2666
* [ENHANCEMENT] Experimental Cassandra backend: Add `-cassandra.reconnect-interval` to allow specifying the reconnect interval to a Cassandra server that has been marked `DOWN` by the gocql driver. Also change the default value of the reconnect interval from `60s` to `1s`. #2687
* [ENHANCEMENT] Experimental Cassandra backend: Add option `-cassandra.convict-hosts-on-failure=false` to not convict host of being down when a request fails. #2684
* [ENHANCEMENT] Experimental TSDB: Applied a jitter to the period bucket scans in order to better distribute bucket operations over the time and increase the probability of hitting the shared cache (if configured). #2693
* [ENHANCEMENT] Experimental TSDB: Series limit per user and per metric now work in TSDB blocks. #2676
* [ENHANCEMENT] Experimental Memberlist: Added ability to periodically rejoin the memberlist cluster. #2724
* [ENHANCEMENT] Experimental Delete Series: Added the following metrics for monitoring processing of delete requests: #2730
  - `cortex_purger_load_pending_requests_attempts_total`: Number of attempts that were made to load pending requests with status.
  - `cortex_purger_oldest_pending_delete_request_age_seconds`: Age of oldest pending delete request in seconds.
  - `cortex_purger_pending_delete_requests_count`: Count of requests which are in process or are ready to be processed.
* [ENHANCEMENT] Experimental TSDB: Improved compactor to hard-delete also partial blocks with an deletion mark (even if the deletion mark threshold has not been reached). #2751
* [ENHANCEMENT] Experimental TSDB: Introduced a consistency check done by the querier to ensure all expected blocks have been queried via the store-gateway. If a block is missing on a store-gateway, the querier retries fetching series from missing blocks up to 3 times. If the consistency check fails once all retries have been exhausted, the query execution fails. The following metrics have been added: #2593 #2630 #2689 #2695
  * `cortex_querier_blocks_consistency_checks_total`
  * `cortex_querier_blocks_consistency_checks_failed_total`
  * `cortex_querier_storegateway_refetches_per_query`
* [ENHANCEMENT] Delete requests can now be canceled #2555
* [ENHANCEMENT] Table manager can now provision tables for delete store #2546
* [BUGFIX] Ruler: Ensure temporary rule files with special characters are properly mapped and cleaned up. #2506
* [BUGFIX] Fixes #2411, Ensure requests are properly routed to the prometheus api embedded in the query if `-server.path-prefix` is set. #2372
* [BUGFIX] Experimental TSDB: fixed chunk data corruption when querying back series using the experimental blocks storage. #2400
* [BUGFIX] Fixed collection of tracing spans from Thanos components used internally. #2655
* [BUGFIX] Experimental TSDB: fixed memory leak in ingesters. #2586
* [BUGFIX] QueryFrontend: fixed a situation where HTTP error is ignored and an incorrect status code is set. #2590
* [BUGFIX] Ingester: Fix an ingester starting up in the JOINING state and staying there forever. #2565
* [BUGFIX] QueryFrontend: fixed a panic (`integer divide by zero`) in the query-frontend. The query-frontend now requires the `-querier.default-evaluation-interval` config to be set to the same value of the querier. #2614
* [BUGFIX] Experimental TSDB: when the querier receives a `/series` request with a time range older than the data stored in the ingester, it now ignores the requested time range and returns known series anyway instead of returning an empty response. This aligns the behaviour with the chunks storage. #2617
* [BUGFIX] Cassandra: fixed an edge case leading to an invalid CQL query when querying the index on a Cassandra store. #2639
* [BUGFIX] Ingester: increment series per metric when recovering from WAL or transfer. #2674
* [BUGFIX] Fixed `wrong number of arguments for 'mget' command` Redis error when a query has no chunks to lookup from storage. #2700 #2796
* [BUGFIX] Ingester: Automatically remove old tmp checkpoints, fixing a potential disk space leak after an ingester crashes. #2726

## Cortex 1.1.0 / 2020-05-21

This release brings the usual mix of bugfixes and improvements. The biggest change is that WAL support for chunks is now considered to be production-ready!

Please make sure to review renamed metrics, and update your dashboards and alerts accordingly.

* [CHANGE] Added v1 API routes documented in #2327. #2372
  * Added `-http.alertmanager-http-prefix` flag which allows the configuration of the path where the Alertmanager API and UI can be reached. The default is set to `/alertmanager`.
  * Added `-http.prometheus-http-prefix` flag which allows the configuration of the path where the Prometheus API and UI can be reached. The default is set to `/prometheus`.
  * Updated the index hosted at the root prefix to point to the updated routes.
  * Legacy routes hardcoded with the `/api/prom` prefix now respect the `-http.prefix` flag.
* [CHANGE] The metrics `cortex_distributor_ingester_appends_total` and `distributor_ingester_append_failures_total` now include a `type` label to differentiate between `samples` and `metadata`. #2336
* [CHANGE] The metrics for number of chunks and bytes flushed to the chunk store are renamed. Note that previous metrics were counted pre-deduplication, while new metrics are counted after deduplication. #2463
  * `cortex_ingester_chunks_stored_total` > `cortex_chunk_store_stored_chunks_total`
  * `cortex_ingester_chunk_stored_bytes_total` > `cortex_chunk_store_stored_chunk_bytes_total`
* [CHANGE] Experimental TSDB: renamed blocks meta fetcher metrics: #2375
  * `cortex_querier_bucket_store_blocks_meta_syncs_total` > `cortex_querier_blocks_meta_syncs_total`
  * `cortex_querier_bucket_store_blocks_meta_sync_failures_total` > `cortex_querier_blocks_meta_sync_failures_total`
  * `cortex_querier_bucket_store_blocks_meta_sync_duration_seconds` > `cortex_querier_blocks_meta_sync_duration_seconds`
  * `cortex_querier_bucket_store_blocks_meta_sync_consistency_delay_seconds` > `cortex_querier_blocks_meta_sync_consistency_delay_seconds`
* [CHANGE] Experimental TSDB: Modified default values for `compactor.deletion-delay` option from 48h to 12h and `-experimental.tsdb.bucket-store.ignore-deletion-marks-delay` from 24h to 6h. #2414
* [CHANGE] WAL: Default value of `-ingester.checkpoint-enabled` changed to `true`. #2416
* [CHANGE] `trace_id` field in log files has been renamed to `traceID`. #2518
* [CHANGE] Slow query log has a different output now. Previously used `url` field has been replaced with `host` and `path`, and query parameters are logged as individual log fields with `qs_` prefix. #2520
* [CHANGE] WAL: WAL and checkpoint compression is now disabled. #2436
* [CHANGE] Update in dependency `go-kit/kit` from `v0.9.0` to `v0.10.0`. HTML escaping disabled in JSON Logger. #2535
* [CHANGE] Experimental TSDB: Removed `cortex_<service>_` prefix from Thanos objstore metrics and added `component` label to distinguish which Cortex component is doing API calls to the object storage when running in single-binary mode: #2568
  - `cortex_<service>_thanos_objstore_bucket_operations_total` renamed to `thanos_objstore_bucket_operations_total{component="<name>"}`
  - `cortex_<service>_thanos_objstore_bucket_operation_failures_total` renamed to `thanos_objstore_bucket_operation_failures_total{component="<name>"}`
  - `cortex_<service>_thanos_objstore_bucket_operation_duration_seconds` renamed to `thanos_objstore_bucket_operation_duration_seconds{component="<name>"}`
  - `cortex_<service>_thanos_objstore_bucket_last_successful_upload_time` renamed to `thanos_objstore_bucket_last_successful_upload_time{component="<name>"}`
* [CHANGE] FIFO cache: The `-<prefix>.fifocache.size` CLI flag has been renamed to `-<prefix>.fifocache.max-size-items` as well as its YAML config option `size` renamed to `max_size_items`. #2319
* [FEATURE] Ruler: The `-ruler.evaluation-delay` flag was added to allow users to configure a default evaluation delay for all rules in cortex. The default value is 0 which is the current behavior. #2423
* [FEATURE] Experimental: Added a new object storage client for OpenStack Swift. #2440
* [FEATURE] TLS config options added to the Server. #2535
* [FEATURE] Experimental: Added support for `/api/v1/metadata` Prometheus-based endpoint. #2549
* [FEATURE] Add ability to limit concurrent queries to Cassandra with `-cassandra.query-concurrency` flag. #2562
* [FEATURE] Experimental TSDB: Introduced store-gateway service used by the experimental blocks storage to load and query blocks. The store-gateway optionally supports blocks sharding and replication via a dedicated hash ring, configurable via `-experimental.store-gateway.sharding-enabled` and `-experimental.store-gateway.sharding-ring.*` flags. The following metrics have been added: #2433 #2458 #2469 #2523
  * `cortex_querier_storegateway_instances_hit_per_query`
* [ENHANCEMENT] Experimental TSDB: sample ingestion errors are now reported via existing `cortex_discarded_samples_total` metric. #2370
* [ENHANCEMENT] Failures on samples at distributors and ingesters return the first validation error as opposed to the last. #2383
* [ENHANCEMENT] Experimental TSDB: Added `cortex_querier_blocks_meta_synced`, which reflects current state of synced blocks over all tenants. #2392
* [ENHANCEMENT] Added `cortex_distributor_latest_seen_sample_timestamp_seconds` metric to see how far behind Prometheus servers are in sending data. #2371
* [ENHANCEMENT] FIFO cache to support eviction based on memory usage. Added `-<prefix>.fifocache.max-size-bytes` CLI flag and YAML config option `max_size_bytes` to specify memory limit of the cache. #2319, #2527
* [ENHANCEMENT] Added `-querier.worker-match-max-concurrent`. Force worker concurrency to match the `-querier.max-concurrent` option.  Overrides `-querier.worker-parallelism`.  #2456
* [ENHANCEMENT] Added the following metrics for monitoring delete requests: #2445
  - `cortex_purger_delete_requests_received_total`: Number of delete requests received per user.
  - `cortex_purger_delete_requests_processed_total`: Number of delete requests processed per user.
  - `cortex_purger_delete_requests_chunks_selected_total`: Number of chunks selected while building delete plans per user.
  - `cortex_purger_delete_requests_processing_failures_total`: Number of delete requests processing failures per user.
* [ENHANCEMENT] Single Binary: Added query-frontend to the single binary.  Single binary users will now benefit from various query-frontend features.  Primarily: sharding, parallelization, load shedding, additional caching (if configured), and query retries. #2437
* [ENHANCEMENT] Allow 1w (where w denotes week) and 1y (where y denotes year) when setting `-store.cache-lookups-older-than` and `-store.max-look-back-period`. #2454
* [ENHANCEMENT] Optimize index queries for matchers using "a|b|c"-type regex. #2446 #2475
* [ENHANCEMENT] Added per tenant metrics for queries and chunks and bytes read from chunk store: #2463
  * `cortex_chunk_store_fetched_chunks_total` and `cortex_chunk_store_fetched_chunk_bytes_total`
  * `cortex_query_frontend_queries_total` (per tenant queries counted by the frontend)
* [ENHANCEMENT] WAL: New metrics `cortex_ingester_wal_logged_bytes_total` and `cortex_ingester_checkpoint_logged_bytes_total` added to track total bytes logged to disk for WAL and checkpoints. #2497
* [ENHANCEMENT] Add de-duplicated chunks counter `cortex_chunk_store_deduped_chunks_total` which counts every chunk not sent to the store because it was already sent by another replica. #2485
* [ENHANCEMENT] Query-frontend now also logs the POST data of long queries. #2481
* [ENHANCEMENT] WAL: Ingester WAL records now have type header and the custom WAL records have been replaced by Prometheus TSDB's WAL records. Old records will not be supported from 1.3 onwards. Note: once this is deployed, you cannot downgrade without data loss. #2436
* [ENHANCEMENT] Redis Cache: Added `idle_timeout`, `wait_on_pool_exhaustion` and `max_conn_lifetime` options to redis cache configuration. #2550
* [ENHANCEMENT] WAL: the experimental tag has been removed on the WAL in ingesters. #2560
* [ENHANCEMENT] Use newer AWS API for paginated queries - removes 'Deprecated' message from logfiles. #2452
* [ENHANCEMENT] Experimental memberlist: Add retry with backoff on memberlist join other members. #2705
* [ENHANCEMENT] Experimental TSDB: when the store-gateway sharding is enabled, unhealthy store-gateway instances are automatically removed from the ring after 10 consecutive `-experimental.store-gateway.sharding-ring.heartbeat-timeout` periods. #2526
* [BUGFIX] Ruler: Ensure temporary rule files with special characters are properly mapped and cleaned up. #2506
* [BUGFIX] Ensure requests are properly routed to the prometheus api embedded in the query if `-server.path-prefix` is set. Fixes #2411. #2372
* [BUGFIX] Experimental TSDB: Fixed chunk data corruption when querying back series using the experimental blocks storage. #2400
* [BUGFIX] Cassandra Storage: Fix endpoint TLS host verification. #2109
* [BUGFIX] Experimental TSDB: Fixed response status code from `422` to `500` when an error occurs while iterating chunks with the experimental blocks storage. #2402
* [BUGFIX] Ring: Fixed a situation where upgrading from pre-1.0 cortex with a rolling strategy caused new 1.0 ingesters to lose their zone value in the ring until manually forced to re-register. #2404
* [BUGFIX] Distributor: `/all_user_stats` now show API and Rule Ingest Rate correctly. #2457
* [BUGFIX] Fixed `version`, `revision` and `branch` labels exported by the `cortex_build_info` metric. #2468
* [BUGFIX] QueryFrontend: fixed a situation where span context missed when downstream_url is used. #2539
* [BUGFIX] Querier: Fixed a situation where querier would crash because of an unresponsive frontend instance. #2569

## Cortex 1.0.1 / 2020-04-23

* [BUGFIX] Fix gaps when querying ingesters with replication factor = 3 and 2 ingesters in the cluster. #2503

## Cortex 1.0.0 / 2020-04-02

This is the first major release of Cortex. We made a lot of **breaking changes** in this release which have been detailed below. Please also see the stability guarantees we provide as part of a major release: https://cortexmetrics.io/docs/configuration/v1guarantees/

* [CHANGE] Remove the following deprecated flags: #2339
  - `-metrics.error-rate-query` (use `-metrics.write-throttle-query` instead).
  - `-store.cardinality-cache-size` (use `-store.index-cache-read.enable-fifocache` and `-store.index-cache-read.fifocache.size` instead).
  - `-store.cardinality-cache-validity` (use `-store.index-cache-read.enable-fifocache` and `-store.index-cache-read.fifocache.duration` instead).
  - `-distributor.limiter-reload-period` (flag unused)
  - `-ingester.claim-on-rollout` (flag unused)
  - `-ingester.normalise-tokens` (flag unused)
* [CHANGE] Renamed YAML file options to be more consistent. See [full config file changes below](#config-file-breaking-changes). #2273
* [CHANGE] AWS based autoscaling has been removed. You can only use metrics based autoscaling now. `-applicationautoscaling.url` has been removed. See https://cortexmetrics.io/docs/production/aws/#dynamodb-capacity-provisioning on how to migrate. #2328
* [CHANGE] Renamed the `memcache.write-back-goroutines` and `memcache.write-back-buffer` flags to `background.write-back-concurrency` and `background.write-back-buffer`. This affects the following flags: #2241
  - `-frontend.memcache.write-back-buffer` --> `-frontend.background.write-back-buffer`
  - `-frontend.memcache.write-back-goroutines` --> `-frontend.background.write-back-concurrency`
  - `-store.index-cache-read.memcache.write-back-buffer` --> `-store.index-cache-read.background.write-back-buffer`
  - `-store.index-cache-read.memcache.write-back-goroutines` --> `-store.index-cache-read.background.write-back-concurrency`
  - `-store.index-cache-write.memcache.write-back-buffer` --> `-store.index-cache-write.background.write-back-buffer`
  - `-store.index-cache-write.memcache.write-back-goroutines` --> `-store.index-cache-write.background.write-back-concurrency`
  - `-memcache.write-back-buffer` --> `-store.chunks-cache.background.write-back-buffer`. Note the next change log for the difference.
  - `-memcache.write-back-goroutines` --> `-store.chunks-cache.background.write-back-concurrency`. Note the next change log for the difference.

* [CHANGE] Renamed the chunk cache flags to have `store.chunks-cache.` as prefix. This means the following flags have been changed: #2241
  - `-cache.enable-fifocache` --> `-store.chunks-cache.cache.enable-fifocache`
  - `-default-validity` --> `-store.chunks-cache.default-validity`
  - `-fifocache.duration` --> `-store.chunks-cache.fifocache.duration`
  - `-fifocache.size` --> `-store.chunks-cache.fifocache.size`
  - `-memcache.write-back-buffer` --> `-store.chunks-cache.background.write-back-buffer`. Note the previous change log for the difference.
  - `-memcache.write-back-goroutines` --> `-store.chunks-cache.background.write-back-concurrency`. Note the previous change log for the difference.
  - `-memcached.batchsize` --> `-store.chunks-cache.memcached.batchsize`
  - `-memcached.consistent-hash` --> `-store.chunks-cache.memcached.consistent-hash`
  - `-memcached.expiration` --> `-store.chunks-cache.memcached.expiration`
  - `-memcached.hostname` --> `-store.chunks-cache.memcached.hostname`
  - `-memcached.max-idle-conns` --> `-store.chunks-cache.memcached.max-idle-conns`
  - `-memcached.parallelism` --> `-store.chunks-cache.memcached.parallelism`
  - `-memcached.service` --> `-store.chunks-cache.memcached.service`
  - `-memcached.timeout` --> `-store.chunks-cache.memcached.timeout`
  - `-memcached.update-interval` --> `-store.chunks-cache.memcached.update-interval`
  - `-redis.enable-tls` --> `-store.chunks-cache.redis.enable-tls`
  - `-redis.endpoint` --> `-store.chunks-cache.redis.endpoint`
  - `-redis.expiration` --> `-store.chunks-cache.redis.expiration`
  - `-redis.max-active-conns` --> `-store.chunks-cache.redis.max-active-conns`
  - `-redis.max-idle-conns` --> `-store.chunks-cache.redis.max-idle-conns`
  - `-redis.password` --> `-store.chunks-cache.redis.password`
  - `-redis.timeout` --> `-store.chunks-cache.redis.timeout`
* [CHANGE] Rename the `-store.chunk-cache-stubs` to `-store.chunks-cache.cache-stubs` to be more inline with above. #2241
* [CHANGE] Change prefix of flags `-dynamodb.periodic-table.*` to `-table-manager.index-table.*`. #2359
* [CHANGE] Change prefix of flags `-dynamodb.chunk-table.*` to `-table-manager.chunk-table.*`. #2359
* [CHANGE] Change the following flags: #2359
  - `-dynamodb.poll-interval` --> `-table-manager.poll-interval`
  - `-dynamodb.periodic-table.grace-period` --> `-table-manager.periodic-table.grace-period`
* [CHANGE] Renamed the following flags: #2273
  - `-dynamodb.chunk.gang.size` --> `-dynamodb.chunk-gang-size`
  - `-dynamodb.chunk.get.max.parallelism` --> `-dynamodb.chunk-get-max-parallelism`
* [CHANGE] Don't support mixed time units anymore for duration. For example, 168h5m0s doesn't work anymore, please use just one unit (s|m|h|d|w|y). #2252
* [CHANGE] Utilize separate protos for rule state and storage. Experimental ruler API will not be functional until the rollout is complete. #2226
* [CHANGE] Frontend worker in querier now starts after all Querier module dependencies are started. This fixes issue where frontend worker started to send queries to querier before it was ready to serve them (mostly visible when using experimental blocks storage). #2246
* [CHANGE] Lifecycler component now enters Failed state on errors, and doesn't exit the process. (Important if you're vendoring Cortex and use Lifecycler) #2251
* [CHANGE] `/ready` handler now returns 200 instead of 204. #2330
* [CHANGE] Better defaults for the following options: #2344
  - `-<prefix>.consul.consistent-reads`: Old default: `true`, new default: `false`. This reduces the load on Consul.
  - `-<prefix>.consul.watch-rate-limit`: Old default: 0, new default: 1. This rate limits the reads to 1 per second. Which is good enough for ring watches.
  - `-distributor.health-check-ingesters`: Old default: `false`, new default: `true`.
  - `-ingester.max-stale-chunk-idle`: Old default: 0, new default: 2m. This lets us expire series that we know are stale early.
  - `-ingester.spread-flushes`: Old default: false, new default: true. This allows to better de-duplicate data and use less space.
  - `-ingester.chunk-age-jitter`: Old default: 20mins, new default: 0. This is to enable the `-ingester.spread-flushes` to true.
  - `-<prefix>.memcached.batchsize`: Old default: 0, new default: 1024. This allows batching of requests and keeps the concurrent requests low.
  - `-<prefix>.memcached.consistent-hash`: Old default: false, new default: true. This allows for better cache hits when the memcaches are scaled up and down.
  - `-querier.batch-iterators`: Old default: false, new default: true.
  - `-querier.ingester-streaming`: Old default: false, new default: true.
* [CHANGE] Experimental TSDB: Added `-experimental.tsdb.bucket-store.postings-cache-compression-enabled` to enable postings compression when storing to cache. #2335
* [CHANGE] Experimental TSDB: Added `-compactor.deletion-delay`, which is time before a block marked for deletion is deleted from bucket. If not 0, blocks will be marked for deletion and compactor component will delete blocks marked for deletion from the bucket. If delete-delay is 0, blocks will be deleted straight away. Note that deleting blocks immediately can cause query failures, if store gateway / querier still has the block loaded, or compactor is ignoring the deletion because it's compacting the block at the same time. Default value is 48h. #2335
* [CHANGE] Experimental TSDB: Added `-experimental.tsdb.bucket-store.index-cache.postings-compression-enabled`, to set duration after which the blocks marked for deletion will be filtered out while fetching blocks used for querying. This option allows querier to ignore blocks that are marked for deletion with some delay. This ensures store can still serve blocks that are meant to be deleted but do not have a replacement yet. Default is 24h, half of the default value for `-compactor.deletion-delay`. #2335
* [CHANGE] Experimental TSDB: Added `-experimental.tsdb.bucket-store.index-cache.memcached.max-item-size` to control maximum size of item that is stored to memcached. Defaults to 1 MiB. #2335
* [FEATURE] Added experimental storage API to the ruler service that is enabled when the `-experimental.ruler.enable-api` is set to true #2269
  * `-ruler.storage.type` flag now allows `s3`,`gcs`, and `azure` values
  * `-ruler.storage.(s3|gcs|azure)` flags exist to allow the configuration of object clients set for rule storage
* [CHANGE] Renamed table manager metrics. #2307 #2359
  * `cortex_dynamo_sync_tables_seconds` -> `cortex_table_manager_sync_duration_seconds`
  * `cortex_dynamo_table_capacity_units` -> `cortex_table_capacity_units`
* [FEATURE] Flusher target to flush the WAL. #2075
  * `-flusher.wal-dir` for the WAL directory to recover from.
  * `-flusher.concurrent-flushes` for number of concurrent flushes.
  * `-flusher.flush-op-timeout` is duration after which a flush should timeout.
* [FEATURE] Ingesters can now have an optional availability zone set, to ensure metric replication is distributed across zones. This is set via the `-ingester.availability-zone` flag or the `availability_zone` field in the config file. #2317
* [ENHANCEMENT] Better re-use of connections to DynamoDB and S3. #2268
* [ENHANCEMENT] Reduce number of goroutines used while executing a single index query. #2280
* [ENHANCEMENT] Experimental TSDB: Add support for local `filesystem` backend. #2245
* [ENHANCEMENT] Experimental TSDB: Added memcached support for the TSDB index cache. #2290
* [ENHANCEMENT] Experimental TSDB: Removed gRPC server to communicate between querier and BucketStore. #2324
* [ENHANCEMENT] Allow 1w (where w denotes week) and 1y (where y denotes year) when setting table period and retention. #2252
* [ENHANCEMENT] Added FIFO cache metrics for current number of entries and memory usage. #2270
* [ENHANCEMENT] Output all config fields to /config API, including those with empty value. #2209
* [ENHANCEMENT] Add "missing_metric_name" and "metric_name_invalid" reasons to cortex_discarded_samples_total metric. #2346
* [ENHANCEMENT] Experimental TSDB: sample ingestion errors are now reported via existing `cortex_discarded_samples_total` metric. #2370
* [BUGFIX] Ensure user state metrics are updated if a transfer fails. #2338
* [BUGFIX] Fixed etcd client keepalive settings. #2278
* [BUGFIX] Register the metrics of the WAL. #2295
* [BUXFIX] Experimental TSDB: fixed error handling when ingesting out of bound samples. #2342

### Known issues

- This experimental blocks storage in Cortex `1.0.0` has a bug which may lead to the error `cannot iterate chunk for series` when running queries. This bug has been fixed in #2400. If you're running the experimental blocks storage, please build Cortex from `master`.

### Config file breaking changes

In this section you can find a config file diff showing the breaking changes introduced in Cortex. You can also find the [full configuration file reference doc](https://cortexmetrics.io/docs/configuration/configuration-file/) in the website.

```diff
### ingester_config

 # Period with which to attempt to flush chunks.
 # CLI flag: -ingester.flush-period
-[flushcheckperiod: <duration> | default = 1m0s]
+[flush_period: <duration> | default = 1m0s]

 # Period chunks will remain in memory after flushing.
 # CLI flag: -ingester.retain-period
-[retainperiod: <duration> | default = 5m0s]
+[retain_period: <duration> | default = 5m0s]

 # Maximum chunk idle time before flushing.
 # CLI flag: -ingester.max-chunk-idle
-[maxchunkidle: <duration> | default = 5m0s]
+[max_chunk_idle_time: <duration> | default = 5m0s]

 # Maximum chunk idle time for chunks terminating in stale markers before
 # flushing. 0 disables it and a stale series is not flushed until the
 # max-chunk-idle timeout is reached.
 # CLI flag: -ingester.max-stale-chunk-idle
-[maxstalechunkidle: <duration> | default = 0s]
+[max_stale_chunk_idle_time: <duration> | default = 2m0s]

 # Timeout for individual flush operations.
 # CLI flag: -ingester.flush-op-timeout
-[flushoptimeout: <duration> | default = 1m0s]
+[flush_op_timeout: <duration> | default = 1m0s]

 # Maximum chunk age before flushing.
 # CLI flag: -ingester.max-chunk-age
-[maxchunkage: <duration> | default = 12h0m0s]
+[max_chunk_age: <duration> | default = 12h0m0s]

-# Range of time to subtract from MaxChunkAge to spread out flushes
+# Range of time to subtract from -ingester.max-chunk-age to spread out flushes
 # CLI flag: -ingester.chunk-age-jitter
-[chunkagejitter: <duration> | default = 20m0s]
+[chunk_age_jitter: <duration> | default = 0]

 # Number of concurrent goroutines flushing to dynamodb.
 # CLI flag: -ingester.concurrent-flushes
-[concurrentflushes: <int> | default = 50]
+[concurrent_flushes: <int> | default = 50]

-# If true, spread series flushes across the whole period of MaxChunkAge
+# If true, spread series flushes across the whole period of
+# -ingester.max-chunk-age.
 # CLI flag: -ingester.spread-flushes
-[spreadflushes: <boolean> | default = false]
+[spread_flushes: <boolean> | default = true]

 # Period with which to update the per-user ingestion rates.
 # CLI flag: -ingester.rate-update-period
-[rateupdateperiod: <duration> | default = 15s]
+[rate_update_period: <duration> | default = 15s]


### querier_config

 # The maximum number of concurrent queries.
 # CLI flag: -querier.max-concurrent
-[maxconcurrent: <int> | default = 20]
+[max_concurrent: <int> | default = 20]

 # Use batch iterators to execute query, as opposed to fully materialising the
 # series in memory.  Takes precedent over the -querier.iterators flag.
 # CLI flag: -querier.batch-iterators
-[batchiterators: <boolean> | default = false]
+[batch_iterators: <boolean> | default = true]

 # Use streaming RPCs to query ingester.
 # CLI flag: -querier.ingester-streaming
-[ingesterstreaming: <boolean> | default = false]
+[ingester_streaming: <boolean> | default = true]

 # Maximum number of samples a single query can load into memory.
 # CLI flag: -querier.max-samples
-[maxsamples: <int> | default = 50000000]
+[max_samples: <int> | default = 50000000]

 # The default evaluation interval or step size for subqueries.
 # CLI flag: -querier.default-evaluation-interval
-[defaultevaluationinterval: <duration> | default = 1m0s]
+[default_evaluation_interval: <duration> | default = 1m0s]

### query_frontend_config

 # URL of downstream Prometheus.
 # CLI flag: -frontend.downstream-url
-[downstream: <string> | default = ""]
+[downstream_url: <string> | default = ""]


### ruler_config

 # URL of alerts return path.
 # CLI flag: -ruler.external.url
-[externalurl: <url> | default = ]
+[external_url: <url> | default = ]

 # How frequently to evaluate rules
 # CLI flag: -ruler.evaluation-interval
-[evaluationinterval: <duration> | default = 1m0s]
+[evaluation_interval: <duration> | default = 1m0s]

 # How frequently to poll for rule changes
 # CLI flag: -ruler.poll-interval
-[pollinterval: <duration> | default = 1m0s]
+[poll_interval: <duration> | default = 1m0s]

-storeconfig:
+storage:

 # file path to store temporary rule files for the prometheus rule managers
 # CLI flag: -ruler.rule-path
-[rulepath: <string> | default = "/rules"]
+[rule_path: <string> | default = "/rules"]

 # URL of the Alertmanager to send notifications to.
 # CLI flag: -ruler.alertmanager-url
-[alertmanagerurl: <url> | default = ]
+[alertmanager_url: <url> | default = ]

 # Use DNS SRV records to discover alertmanager hosts.
 # CLI flag: -ruler.alertmanager-discovery
-[alertmanagerdiscovery: <boolean> | default = false]
+[enable_alertmanager_discovery: <boolean> | default = false]

 # How long to wait between refreshing alertmanager hosts.
 # CLI flag: -ruler.alertmanager-refresh-interval
-[alertmanagerrefreshinterval: <duration> | default = 1m0s]
+[alertmanager_refresh_interval: <duration> | default = 1m0s]

 # If enabled requests to alertmanager will utilize the V2 API.
 # CLI flag: -ruler.alertmanager-use-v2
-[alertmanangerenablev2api: <boolean> | default = false]
+[enable_alertmanager_v2: <boolean> | default = false]

 # Capacity of the queue for notifications to be sent to the Alertmanager.
 # CLI flag: -ruler.notification-queue-capacity
-[notificationqueuecapacity: <int> | default = 10000]
+[notification_queue_capacity: <int> | default = 10000]

 # HTTP timeout duration when sending notifications to the Alertmanager.
 # CLI flag: -ruler.notification-timeout
-[notificationtimeout: <duration> | default = 10s]
+[notification_timeout: <duration> | default = 10s]

 # Distribute rule evaluation using ring backend
 # CLI flag: -ruler.enable-sharding
-[enablesharding: <boolean> | default = false]
+[enable_sharding: <boolean> | default = false]

 # Time to spend searching for a pending ruler when shutting down.
 # CLI flag: -ruler.search-pending-for
-[searchpendingfor: <duration> | default = 5m0s]
+[search_pending_for: <duration> | default = 5m0s]

 # Period with which to attempt to flush rule groups.
 # CLI flag: -ruler.flush-period
-[flushcheckperiod: <duration> | default = 1m0s]
+[flush_period: <duration> | default = 1m0s]

### alertmanager_config

 # Base path for data storage.
 # CLI flag: -alertmanager.storage.path
-[datadir: <string> | default = "data/"]
+[data_dir: <string> | default = "data/"]

 # will be used to prefix all HTTP endpoints served by Alertmanager. If omitted,
 # relevant URL components will be derived automatically.
 # CLI flag: -alertmanager.web.external-url
-[externalurl: <url> | default = ]
+[external_url: <url> | default = ]

 # How frequently to poll Cortex configs
 # CLI flag: -alertmanager.configs.poll-interval
-[pollinterval: <duration> | default = 15s]
+[poll_interval: <duration> | default = 15s]

 # Listen address for cluster.
 # CLI flag: -cluster.listen-address
-[clusterbindaddr: <string> | default = "0.0.0.0:9094"]
+[cluster_bind_address: <string> | default = "0.0.0.0:9094"]

 # Explicit address to advertise in cluster.
 # CLI flag: -cluster.advertise-address
-[clusteradvertiseaddr: <string> | default = ""]
+[cluster_advertise_address: <string> | default = ""]

 # Time to wait between peers to send notifications.
 # CLI flag: -cluster.peer-timeout
-[peertimeout: <duration> | default = 15s]
+[peer_timeout: <duration> | default = 15s]

 # Filename of fallback config to use if none specified for instance.
 # CLI flag: -alertmanager.configs.fallback
-[fallbackconfigfile: <string> | default = ""]
+[fallback_config_file: <string> | default = ""]

 # Root of URL to generate if config is http://internal.monitor
 # CLI flag: -alertmanager.configs.auto-webhook-root
-[autowebhookroot: <string> | default = ""]
+[auto_webhook_root: <string> | default = ""]

### table_manager_config

-store:
+storage:

-# How frequently to poll DynamoDB to learn our capacity.
-# CLI flag: -dynamodb.poll-interval
-[dynamodb_poll_interval: <duration> | default = 2m0s]
+# How frequently to poll backend to learn our capacity.
+# CLI flag: -table-manager.poll-interval
+[poll_interval: <duration> | default = 2m0s]

-# DynamoDB periodic tables grace period (duration which table will be
-# created/deleted before/after it's needed).
-# CLI flag: -dynamodb.periodic-table.grace-period
+# Periodic tables grace period (duration which table will be created/deleted
+# before/after it's needed).
+# CLI flag: -table-manager.periodic-table.grace-period
 [creation_grace_period: <duration> | default = 10m0s]

 index_tables_provisioning:
   # Enables on demand throughput provisioning for the storage provider (if
-  # supported). Applies only to tables which are not autoscaled
-  # CLI flag: -dynamodb.periodic-table.enable-ondemand-throughput-mode
-  [provisioned_throughput_on_demand_mode: <boolean> | default = false]
+  # supported). Applies only to tables which are not autoscaled. Supported by
+  # DynamoDB
+  # CLI flag: -table-manager.index-table.enable-ondemand-throughput-mode
+  [enable_ondemand_throughput_mode: <boolean> | default = false]


   # Enables on demand throughput provisioning for the storage provider (if
-  # supported). Applies only to tables which are not autoscaled
-  # CLI flag: -dynamodb.periodic-table.inactive-enable-ondemand-throughput-mode
-  [inactive_throughput_on_demand_mode: <boolean> | default = false]
+  # supported). Applies only to tables which are not autoscaled. Supported by
+  # DynamoDB
+  # CLI flag: -table-manager.index-table.inactive-enable-ondemand-throughput-mode
+  [enable_inactive_throughput_on_demand_mode: <boolean> | default = false]


 chunk_tables_provisioning:
   # Enables on demand throughput provisioning for the storage provider (if
-  # supported). Applies only to tables which are not autoscaled
-  # CLI flag: -dynamodb.chunk-table.enable-ondemand-throughput-mode
-  [provisioned_throughput_on_demand_mode: <boolean> | default = false]
+  # supported). Applies only to tables which are not autoscaled. Supported by
+  # DynamoDB
+  # CLI flag: -table-manager.chunk-table.enable-ondemand-throughput-mode
+  [enable_ondemand_throughput_mode: <boolean> | default = false]

### storage_config

 aws:
-  dynamodbconfig:
+  dynamodb:
     # DynamoDB endpoint URL with escaped Key and Secret encoded. If only region
     # is specified as a host, proper endpoint will be deduced. Use
     # inmemory:///<table-name> to use a mock in-memory implementation.
     # CLI flag: -dynamodb.url
-    [dynamodb: <url> | default = ]
+    [dynamodb_url: <url> | default = ]

     # DynamoDB table management requests per second limit.
     # CLI flag: -dynamodb.api-limit
-    [apilimit: <float> | default = 2]
+    [api_limit: <float> | default = 2]

     # DynamoDB rate cap to back off when throttled.
     # CLI flag: -dynamodb.throttle-limit
-    [throttlelimit: <float> | default = 10]
+    [throttle_limit: <float> | default = 10]
-
-    # ApplicationAutoscaling endpoint URL with escaped Key and Secret encoded.
-    # CLI flag: -applicationautoscaling.url
-    [applicationautoscaling: <url> | default = ]


       # Queue length above which we will scale up capacity
       # CLI flag: -metrics.target-queue-length
-      [targetqueuelen: <int> | default = 100000]
+      [target_queue_length: <int> | default = 100000]

       # Scale up capacity by this multiple
       # CLI flag: -metrics.scale-up-factor
-      [scaleupfactor: <float> | default = 1.3]
+      [scale_up_factor: <float> | default = 1.3]

       # Ignore throttling below this level (rate per second)
       # CLI flag: -metrics.ignore-throttle-below
-      [minthrottling: <float> | default = 1]
+      [ignore_throttle_below: <float> | default = 1]

       # query to fetch ingester queue length
       # CLI flag: -metrics.queue-length-query
-      [queuelengthquery: <string> | default = "sum(avg_over_time(cortex_ingester_flush_queue_length{job=\"cortex/ingester\"}[2m]))"]
+      [queue_length_query: <string> | default = "sum(avg_over_time(cortex_ingester_flush_queue_length{job=\"cortex/ingester\"}[2m]))"]

       # query to fetch throttle rates per table
       # CLI flag: -metrics.write-throttle-query
-      [throttlequery: <string> | default = "sum(rate(cortex_dynamo_throttled_total{operation=\"DynamoDB.BatchWriteItem\"}[1m])) by (table) > 0"]
+      [write_throttle_query: <string> | default = "sum(rate(cortex_dynamo_throttled_total{operation=\"DynamoDB.BatchWriteItem\"}[1m])) by (table) > 0"]

       # query to fetch write capacity usage per table
       # CLI flag: -metrics.usage-query
-      [usagequery: <string> | default = "sum(rate(cortex_dynamo_consumed_capacity_total{operation=\"DynamoDB.BatchWriteItem\"}[15m])) by (table) > 0"]
+      [write_usage_query: <string> | default = "sum(rate(cortex_dynamo_consumed_capacity_total{operation=\"DynamoDB.BatchWriteItem\"}[15m])) by (table) > 0"]

       # query to fetch read capacity usage per table
       # CLI flag: -metrics.read-usage-query
-      [readusagequery: <string> | default = "sum(rate(cortex_dynamo_consumed_capacity_total{operation=\"DynamoDB.QueryPages\"}[1h])) by (table) > 0"]
+      [read_usage_query: <string> | default = "sum(rate(cortex_dynamo_consumed_capacity_total{operation=\"DynamoDB.QueryPages\"}[1h])) by (table) > 0"]

       # query to fetch read errors per table
       # CLI flag: -metrics.read-error-query
-      [readerrorquery: <string> | default = "sum(increase(cortex_dynamo_failures_total{operation=\"DynamoDB.QueryPages\",error=\"ProvisionedThroughputExceededException\"}[1m])) by (table) > 0"]
+      [read_error_query: <string> | default = "sum(increase(cortex_dynamo_failures_total{operation=\"DynamoDB.QueryPages\",error=\"ProvisionedThroughputExceededException\"}[1m])) by (table) > 0"]

     # Number of chunks to group together to parallelise fetches (zero to
     # disable)
-    # CLI flag: -dynamodb.chunk.gang.size
-    [chunkgangsize: <int> | default = 10]
+    # CLI flag: -dynamodb.chunk-gang-size
+    [chunk_gang_size: <int> | default = 10]

     # Max number of chunk-get operations to start in parallel
-    # CLI flag: -dynamodb.chunk.get.max.parallelism
-    [chunkgetmaxparallelism: <int> | default = 32]
+    # CLI flag: -dynamodb.chunk.get-max-parallelism
+    [chunk_get_max_parallelism: <int> | default = 32]

     backoff_config:
       # Minimum delay when backing off.
       # CLI flag: -bigtable.backoff-min-period
-      [minbackoff: <duration> | default = 100ms]
+      [min_period: <duration> | default = 100ms]

       # Maximum delay when backing off.
       # CLI flag: -bigtable.backoff-max-period
-      [maxbackoff: <duration> | default = 10s]
+      [max_period: <duration> | default = 10s]

       # Number of times to backoff and retry before failing.
       # CLI flag: -bigtable.backoff-retries
-      [maxretries: <int> | default = 10]
+      [max_retries: <int> | default = 10]

   # If enabled, once a tables info is fetched, it is cached.
   # CLI flag: -bigtable.table-cache.enabled
-  [tablecacheenabled: <boolean> | default = true]
+  [table_cache_enabled: <boolean> | default = true]

   # Duration to cache tables before checking again.
   # CLI flag: -bigtable.table-cache.expiration
-  [tablecacheexpiration: <duration> | default = 30m0s]
+  [table_cache_expiration: <duration> | default = 30m0s]

 # Cache validity for active index entries. Should be no higher than
 # -ingester.max-chunk-idle.
 # CLI flag: -store.index-cache-validity
-[indexcachevalidity: <duration> | default = 5m0s]
+[index_cache_validity: <duration> | default = 5m0s]

### ingester_client_config

 grpc_client_config:
   backoff_config:
     # Minimum delay when backing off.
     # CLI flag: -ingester.client.backoff-min-period
-    [minbackoff: <duration> | default = 100ms]
+    [min_period: <duration> | default = 100ms]

     # Maximum delay when backing off.
     # CLI flag: -ingester.client.backoff-max-period
-    [maxbackoff: <duration> | default = 10s]
+    [max_period: <duration> | default = 10s]

     # Number of times to backoff and retry before failing.
     # CLI flag: -ingester.client.backoff-retries
-    [maxretries: <int> | default = 10]
+    [max_retries: <int> | default = 10]

### frontend_worker_config

-# Address of query frontend service.
+# Address of query frontend service, in host:port format.
 # CLI flag: -querier.frontend-address
-[address: <string> | default = ""]
+[frontend_address: <string> | default = ""]

 # How often to query DNS.
 # CLI flag: -querier.dns-lookup-period
-[dnslookupduration: <duration> | default = 10s]
+[dns_lookup_duration: <duration> | default = 10s]

 grpc_client_config:
   backoff_config:
     # Minimum delay when backing off.
     # CLI flag: -querier.frontend-client.backoff-min-period
-    [minbackoff: <duration> | default = 100ms]
+    [min_period: <duration> | default = 100ms]

     # Maximum delay when backing off.
     # CLI flag: -querier.frontend-client.backoff-max-period
-    [maxbackoff: <duration> | default = 10s]
+    [max_period: <duration> | default = 10s]

     # Number of times to backoff and retry before failing.
     # CLI flag: -querier.frontend-client.backoff-retries
-    [maxretries: <int> | default = 10]
+    [max_retries: <int> | default = 10]

### consul_config

 # ACL Token used to interact with Consul.
-# CLI flag: -<prefix>.consul.acltoken
-[acltoken: <string> | default = ""]
+# CLI flag: -<prefix>.consul.acl-token
+[acl_token: <string> | default = ""]

 # HTTP timeout when talking to Consul
 # CLI flag: -<prefix>.consul.client-timeout
-[httpclienttimeout: <duration> | default = 20s]
+[http_client_timeout: <duration> | default = 20s]

 # Enable consistent reads to Consul.
 # CLI flag: -<prefix>.consul.consistent-reads
-[consistentreads: <boolean> | default = true]
+[consistent_reads: <boolean> | default = false]

 # Rate limit when watching key or prefix in Consul, in requests per second. 0
 # disables the rate limit.
 # CLI flag: -<prefix>.consul.watch-rate-limit
-[watchkeyratelimit: <float> | default = 0]
+[watch_rate_limit: <float> | default = 1]

 # Burst size used in rate limit. Values less than 1 are treated as 1.
 # CLI flag: -<prefix>.consul.watch-burst-size
-[watchkeyburstsize: <int> | default = 1]
+[watch_burst_size: <int> | default = 1]


### configstore_config
 # URL of configs API server.
 # CLI flag: -<prefix>.configs.url
-[configsapiurl: <url> | default = ]
+[configs_api_url: <url> | default = ]

 # Timeout for requests to Weave Cloud configs service.
 # CLI flag: -<prefix>.configs.client-timeout
-[clienttimeout: <duration> | default = 5s]
+[client_timeout: <duration> | default = 5s]
```

## Cortex 0.7.0 / 2020-03-16

Cortex `0.7.0` is a major step forward the upcoming `1.0` release. In this release, we've got 164 contributions from 26 authors. Thanks to all contributors! ❤️

Please be aware that Cortex `0.7.0` introduces some **breaking changes**. You're encouraged to read all the `[CHANGE]` entries below before upgrading your Cortex cluster. In particular:

- Cleaned up some configuration options in preparation for the Cortex `1.0.0` release (see also the [annotated config file breaking changes](#annotated-config-file-breaking-changes) below):
  - Removed CLI flags support to configure the schema (see [how to migrate from flags to schema file](https://cortexmetrics.io/docs/configuration/schema-configuration/#migrating-from-flags-to-schema-file))
  - Renamed CLI flag `-config-yaml` to `-schema-config-file`
  - Removed CLI flag `-store.min-chunk-age` in favor of `-querier.query-store-after`. The corresponding YAML config option `ingestermaxquerylookback` has been renamed to [`query_ingesters_within`](https://cortexmetrics.io/docs/configuration/configuration-file/#querier-config)
  - Deprecated CLI flag `-frontend.cache-split-interval` in favor of `-querier.split-queries-by-interval`
  - Renamed the YAML config option `defaul_validity` to `default_validity`
  - Removed the YAML config option `config_store` (in the [`alertmanager YAML config`](https://cortexmetrics.io/docs/configuration/configuration-file/#alertmanager-config)) in favor of `store`
  - Removed the YAML config root block `configdb` in favor of [`configs`](https://cortexmetrics.io/docs/configuration/configuration-file/#configs-config). This change is also reflected in the following CLI flags renaming:
      * `-database.*` -> `-configs.database.*`
      * `-database.migrations` -> `-configs.database.migrations-dir`
  - Removed the fluentd-based billing infrastructure including the CLI flags:
      * `-distributor.enable-billing`
      * `-billing.max-buffered-events`
      * `-billing.retry-delay`
      * `-billing.ingester`
- Removed support for using denormalised tokens in the ring. Before upgrading, make sure your Cortex cluster is already running `v0.6.0` or an earlier version with `-ingester.normalise-tokens=true`

### Full changelog

* [CHANGE] Removed support for flags to configure schema. Further, the flag for specifying the config file (`-config-yaml`) has been deprecated. Please use `-schema-config-file`. See the [Schema Configuration documentation](https://cortexmetrics.io/docs/configuration/schema-configuration/) for more details on how to configure the schema using the YAML file. #2221
* [CHANGE] In the config file, the root level `config_store` config option has been moved to `alertmanager` > `store` > `configdb`. #2125
* [CHANGE] Removed unnecessary `frontend.cache-split-interval` in favor of `querier.split-queries-by-interval` both to reduce configuration complexity and guarantee alignment of these two configs. Starting from now, `-querier.cache-results` may only be enabled in conjunction with `-querier.split-queries-by-interval` (previously the cache interval default was `24h` so if you want to preserve the same behaviour you should set `-querier.split-queries-by-interval=24h`). #2040
* [CHANGE] Renamed Configs configuration options. #2187
  * configuration options
    * `-database.*` -> `-configs.database.*`
    * `-database.migrations` -> `-configs.database.migrations-dir`
  * config file
    * `configdb.uri:` -> `configs.database.uri:`
    * `configdb.migrationsdir:` -> `configs.database.migrations_dir:`
    * `configdb.passwordfile:` -> `configs.database.password_file:`
* [CHANGE] Moved `-store.min-chunk-age` to the Querier config as `-querier.query-store-after`, allowing the store to be skipped during query time if the metrics wouldn't be found. The YAML config option `ingestermaxquerylookback` has been renamed to `query_ingesters_within` to match its CLI flag. #1893
* [CHANGE] Renamed the cache configuration setting `defaul_validity` to `default_validity`. #2140
* [CHANGE] Remove fluentd-based billing infrastructure and flags such as `-distributor.enable-billing`. #1491
* [CHANGE] Removed remaining support for using denormalised tokens in the ring. If you're still running ingesters with denormalised tokens (Cortex 0.4 or earlier, with `-ingester.normalise-tokens=false`), such ingesters will now be completely invisible to distributors and need to be either switched to Cortex 0.6.0 or later, or be configured to use normalised tokens. #2034
* [CHANGE] The frontend http server will now send 502 in case of deadline exceeded and 499 if the user requested cancellation. #2156
* [CHANGE] We now enforce queries to be up to `-querier.max-query-into-future` into the future (defaults to 10m). #1929
  * `-store.min-chunk-age` has been removed
  * `-querier.query-store-after` has been added in it's place.
* [CHANGE] Removed unused `/validate_expr endpoint`. #2152
* [CHANGE] Updated Prometheus dependency to v2.16.0. This Prometheus version uses Active Query Tracker to limit concurrent queries. In order to keep `-querier.max-concurrent` working, Active Query Tracker is enabled by default, and is configured to store its data to `active-query-tracker` directory (relative to current directory when Cortex started). This can be changed by using `-querier.active-query-tracker-dir` option. Purpose of Active Query Tracker is to log queries that were running when Cortex crashes. This logging happens on next Cortex start. #2088
* [CHANGE] Default to BigChunk encoding; may result in slightly higher disk usage if many timeseries have a constant value, but should generally result in fewer, bigger chunks. #2207
* [CHANGE] WAL replays are now done while the rest of Cortex is starting, and more specifically, when HTTP server is running. This makes it possible to scrape metrics during WAL replays. Applies to both chunks and experimental blocks storage. #2222
* [CHANGE] Cortex now has `/ready` probe for all services, not just ingester and querier as before. In single-binary mode, /ready reports 204 only if all components are running properly. #2166
* [CHANGE] If you are vendoring Cortex and use its components in your project, be aware that many Cortex components no longer start automatically when they are created. You may want to review PR and attached document. #2166
* [CHANGE] Experimental TSDB: the querier in-memory index cache used by the experimental blocks storage shifted from per-tenant to per-querier. The `-experimental.tsdb.bucket-store.index-cache-size-bytes` now configures the per-querier index cache max size instead of a per-tenant cache and its default has been increased to 1GB. #2189
* [CHANGE] Experimental TSDB: TSDB head compaction interval and concurrency is now configurable (defaults to 1 min interval and 5 concurrent head compactions). New options: `-experimental.tsdb.head-compaction-interval` and `-experimental.tsdb.head-compaction-concurrency`. #2172
* [CHANGE] Experimental TSDB: switched the blocks storage index header to the binary format. This change is expected to have no visible impact, except lower startup times and memory usage in the queriers. It's possible to switch back to the old JSON format via the flag `-experimental.tsdb.bucket-store.binary-index-header-enabled=false`. #2223
* [CHANGE] Experimental Memberlist KV store can now be used in single-binary Cortex. Attempts to use it previously would fail with panic. This change also breaks existing binary protocol used to exchange gossip messages, so this version will not be able to understand gossiped Ring when used in combination with the previous version of Cortex. Easiest way to upgrade is to shutdown old Cortex installation, and restart it with new version. Incremental rollout works too, but with reduced functionality until all components run the same version. #2016
* [FEATURE] Added a read-only local alertmanager config store using files named corresponding to their tenant id. #2125
* [FEATURE] Added flag `-experimental.ruler.enable-api` to enable the ruler api which implements the Prometheus API `/api/v1/rules` and `/api/v1/alerts` endpoints under the configured `-http.prefix`. #1999
* [FEATURE] Added sharding support to compactor when using the experimental TSDB blocks storage. #2113
* [FEATURE] Added ability to override YAML config file settings using environment variables. #2147
  * `-config.expand-env`
* [FEATURE] Added flags to disable Alertmanager notifications methods. #2187
  * `-configs.notifications.disable-email`
  * `-configs.notifications.disable-webhook`
* [FEATURE] Add /config HTTP endpoint which exposes the current Cortex configuration as YAML. #2165
* [FEATURE] Allow Prometheus remote write directly to ingesters. #1491
* [FEATURE] Introduced new standalone service `query-tee` that can be used for testing purposes to send the same Prometheus query to multiple backends (ie. two Cortex clusters ingesting the same metrics) and compare the performances. #2203
* [FEATURE] Fan out parallelizable queries to backend queriers concurrently. #1878
  * `querier.parallelise-shardable-queries` (bool)
  * Requires a shard-compatible schema (v10+)
  * This causes the number of traces to increase accordingly.
  * The query-frontend now requires a schema config to determine how/when to shard queries, either from a file or from flags (i.e. by the `config-yaml` CLI flag). This is the same schema config the queriers consume. The schema is only required to use this option.
  * It's also advised to increase downstream concurrency controls as well:
    * `querier.max-outstanding-requests-per-tenant`
    * `querier.max-query-parallelism`
    * `querier.max-concurrent`
    * `server.grpc-max-concurrent-streams` (for both query-frontends and queriers)
* [FEATURE] Added user sub rings to distribute users to a subset of ingesters. #1947
  * `-experimental.distributor.user-subring-size`
* [FEATURE] Add flag `-experimental.tsdb.stripe-size` to expose TSDB stripe size option. #2185
* [FEATURE] Experimental Delete Series: Added support for Deleting Series with Prometheus style API. Needs to be enabled first by setting `-purger.enable` to `true`. Deletion only supported when using `boltdb` and `filesystem` as index and object store respectively. Support for other stores to follow in separate PRs #2103
* [ENHANCEMENT] Alertmanager: Expose Per-tenant alertmanager metrics #2124
* [ENHANCEMENT] Add `status` label to `cortex_alertmanager_configs` metric to gauge the number of valid and invalid configs. #2125
* [ENHANCEMENT] Cassandra Authentication: added the `custom_authenticators` config option that allows users to authenticate with cassandra clusters using password authenticators that are not approved by default in [gocql](https://github.com/gocql/gocql/blob/81b8263d9fe526782a588ef94d3fa5c6148e5d67/conn.go#L27) #2093
* [ENHANCEMENT] Cassandra Storage: added `max_retries`, `retry_min_backoff` and `retry_max_backoff` configuration options to enable retrying recoverable errors. #2054
* [ENHANCEMENT] Allow to configure HTTP and gRPC server listen address, maximum number of simultaneous connections and connection keepalive settings.
  * `-server.http-listen-address`
  * `-server.http-conn-limit`
  * `-server.grpc-listen-address`
  * `-server.grpc-conn-limit`
  * `-server.grpc.keepalive.max-connection-idle`
  * `-server.grpc.keepalive.max-connection-age`
  * `-server.grpc.keepalive.max-connection-age-grace`
  * `-server.grpc.keepalive.time`
  * `-server.grpc.keepalive.timeout`
* [ENHANCEMENT] PostgreSQL: Bump up `github.com/lib/pq` from `v1.0.0` to `v1.3.0` to support PostgreSQL SCRAM-SHA-256 authentication. #2097
* [ENHANCEMENT] Cassandra Storage: User no longer need `CREATE` privilege on `<all keyspaces>` if given keyspace exists. #2032
* [ENHANCEMENT] Cassandra Storage: added `password_file` configuration options to enable reading Cassandra password from file. #2096
* [ENHANCEMENT] Configs API: Allow GET/POST configs in YAML format. #2181
* [ENHANCEMENT] Background cache writes are batched to improve parallelism and observability. #2135
* [ENHANCEMENT] Add automatic repair for checkpoint and WAL. #2105
* [ENHANCEMENT] Support `lastEvaluation` and `evaluationTime` in `/api/v1/rules` endpoints and make order of groups stable. #2196
* [ENHANCEMENT] Skip expired requests in query-frontend scheduling. #2082
* [ENHANCEMENT] Add ability to configure gRPC keepalive settings. #2066
* [ENHANCEMENT] Experimental TSDB: Export TSDB Syncer metrics from Compactor component, they are prefixed with `cortex_compactor_`. #2023
* [ENHANCEMENT] Experimental TSDB: Added dedicated flag `-experimental.tsdb.bucket-store.tenant-sync-concurrency` to configure the maximum number of concurrent tenants for which blocks are synched. #2026
* [ENHANCEMENT] Experimental TSDB: Expose metrics for objstore operations (prefixed with `cortex_<component>_thanos_objstore_`, component being one of `ingester`, `querier` and `compactor`). #2027
* [ENHANCEMENT] Experimental TSDB: Added support for Azure Storage to be used for block storage, in addition to S3 and GCS. #2083
* [ENHANCEMENT] Experimental TSDB: Reduced memory allocations in the ingesters when using the experimental blocks storage. #2057
* [ENHANCEMENT] Experimental Memberlist KV: expose `-memberlist.gossip-to-dead-nodes-time` and `-memberlist.dead-node-reclaim-time` options to control how memberlist library handles dead nodes and name reuse. #2131
* [BUGFIX] Alertmanager: fixed panic upon applying a new config, caused by duplicate metrics registration in the `NewPipelineBuilder` function. #211
* [BUGFIX] Azure Blob ChunkStore: Fixed issue causing `invalid chunk checksum` errors. #2074
* [BUGFIX] The gauge `cortex_overrides_last_reload_successful` is now only exported by components that use a `RuntimeConfigManager`. Previously, for components that do not initialize a `RuntimeConfigManager` (such as the compactor) the gauge was initialized with 0 (indicating error state) and then never updated, resulting in a false-negative permanent error state. #2092
* [BUGFIX] Fixed WAL metric names, added the `cortex_` prefix.
* [BUGFIX] Restored histogram `cortex_configs_request_duration_seconds` #2138
* [BUGFIX] Fix wrong syntax for `url` in config-file-reference. #2148
* [BUGFIX] Fixed some 5xx status code returned by the query-frontend when they should actually be 4xx. #2122
* [BUGFIX] Fixed leaked goroutines in the querier. #2070
* [BUGFIX] Experimental TSDB: fixed `/all_user_stats` and `/api/prom/user_stats` endpoints when using the experimental TSDB blocks storage. #2042
* [BUGFIX] Experimental TSDB: fixed ruler to correctly work with the experimental TSDB blocks storage. #2101

### Changes to denormalised tokens in the ring

Cortex 0.4.0 is the last version that can *write* denormalised tokens. Cortex 0.5.0 and above always write normalised tokens.

Cortex 0.6.0 is the last version that can *read* denormalised tokens. Starting with Cortex 0.7.0 only normalised tokens are supported, and ingesters writing denormalised tokens to the ring (running Cortex 0.4.0 or earlier with `-ingester.normalise-tokens=false`) are ignored by distributors. Such ingesters should either switch to using normalised tokens, or be upgraded to Cortex 0.5.0 or later.

### Known issues

- The gRPC streaming for ingesters doesn't work when using the experimental TSDB blocks storage. Please do not enable `-querier.ingester-streaming` if you're using the TSDB blocks storage. If you want to enable it, you can build Cortex from `master` given the issue has been fixed after Cortex `0.7` branch has been cut and the fix wasn't included in the `0.7` because related to an experimental feature.

### Annotated config file breaking changes

In this section you can find a config file diff showing the breaking changes introduced in Cortex `0.7`. You can also find the [full configuration file reference doc](https://cortexmetrics.io/docs/configuration/configuration-file/) in the website.

 ```diff
### Root level config

 # "configdb" has been moved to "alertmanager > store > configdb".
-[configdb: <configdb_config>]

 # "config_store" has been renamed to "configs".
-[config_store: <configstore_config>]
+[configs: <configs_config>]


### `distributor_config`

 # The support to hook an external billing system has been removed.
-[enable_billing: <boolean> | default = false]
-billing:
-  [maxbufferedevents: <int> | default = 1024]
-  [retrydelay: <duration> | default = 500ms]
-  [ingesterhostport: <string> | default = "localhost:24225"]


### `querier_config`

 # "ingestermaxquerylookback" has been renamed to "query_ingesters_within".
-[ingestermaxquerylookback: <duration> | default = 0s]
+[query_ingesters_within: <duration> | default = 0s]


### `queryrange_config`

results_cache:
  cache:
     # "defaul_validity" has been renamed to "default_validity".
-    [defaul_validity: <duration> | default = 0s]
+    [default_validity: <duration> | default = 0s]

   # "cache_split_interval" has been deprecated in favor of "split_queries_by_interval".
-  [cache_split_interval: <duration> | default = 24h0m0s]


### `alertmanager_config`

# The "store" config block has been added. This includes "configdb" which previously
# was the "configdb" root level config block.
+store:
+  [type: <string> | default = "configdb"]
+  [configdb: <configstore_config>]
+  local:
+    [path: <string> | default = ""]


### `storage_config`

index_queries_cache_config:
   # "defaul_validity" has been renamed to "default_validity".
-  [defaul_validity: <duration> | default = 0s]
+  [default_validity: <duration> | default = 0s]


### `chunk_store_config`

chunk_cache_config:
   # "defaul_validity" has been renamed to "default_validity".
-  [defaul_validity: <duration> | default = 0s]
+  [default_validity: <duration> | default = 0s]

write_dedupe_cache_config:
   # "defaul_validity" has been renamed to "default_validity".
-  [defaul_validity: <duration> | default = 0s]
+  [default_validity: <duration> | default = 0s]

 # "min_chunk_age" has been removed in favor of "querier > query_store_after".
-[min_chunk_age: <duration> | default = 0s]


### `configs_config`

-# "uri" has been moved to "database > uri".
-[uri: <string> | default = "postgres://postgres@configs-db.weave.local/configs?sslmode=disable"]

-# "migrationsdir" has been moved to "database > migrations_dir".
-[migrationsdir: <string> | default = ""]

-# "passwordfile" has been moved to "database > password_file".
-[passwordfile: <string> | default = ""]

+database:
+  [uri: <string> | default = "postgres://postgres@configs-db.weave.local/configs?sslmode=disable"]
+  [migrations_dir: <string> | default = ""]
+  [password_file: <string> | default = ""]
```

## Cortex 0.6.1 / 2020-02-05

* [BUGFIX] Fixed parsing of the WAL configuration when specified in the YAML config file. #2071

## Cortex 0.6.0 / 2020-01-28

Note that the ruler flags need to be changed in this upgrade. You're moving from a single node ruler to something that might need to be sharded.
Further, if you're using the configs service, we've upgraded the migration library and this requires some manual intervention. See full instructions below to upgrade your PostgreSQL.

* [CHANGE] The frontend component now does not cache results if it finds a `Cache-Control` header and if one of its values is `no-store`. #1974
* [CHANGE] Flags changed with transition to upstream Prometheus rules manager:
  * `-ruler.client-timeout` is now `ruler.configs.client-timeout` in order to match `ruler.configs.url`.
  * `-ruler.group-timeout`has been removed.
  * `-ruler.num-workers` has been removed.
  * `-ruler.rule-path` has been added to specify where the prometheus rule manager will sync rule files.
  * `-ruler.storage.type` has beem added to specify the rule store backend type, currently only the configdb.
  * `-ruler.poll-interval` has been added to specify the interval in which to poll new rule groups.
  * `-ruler.evaluation-interval` default value has changed from `15s` to `1m` to match the default evaluation interval in Prometheus.
  * Ruler sharding requires a ring which can be configured via the ring flags prefixed by `ruler.ring.`. #1987
* [CHANGE] Use relative links from /ring page to make it work when used behind reverse proxy. #1896
* [CHANGE] Deprecated `-distributor.limiter-reload-period` flag. #1766
* [CHANGE] Ingesters now write only normalised tokens to the ring, although they can still read denormalised tokens used by other ingesters. `-ingester.normalise-tokens` is now deprecated, and ignored. If you want to switch back to using denormalised tokens, you need to downgrade to Cortex 0.4.0. Previous versions don't handle claiming tokens from normalised ingesters correctly. #1809
* [CHANGE] Overrides mechanism has been renamed to "runtime config", and is now separate from limits. Runtime config is simply a file that is reloaded by Cortex every couple of seconds. Limits and now also multi KV use this mechanism.<br />New arguments were introduced: `-runtime-config.file` (defaults to empty) and `-runtime-config.reload-period` (defaults to 10 seconds), which replace previously used `-limits.per-user-override-config` and `-limits.per-user-override-period` options. Old options are still used if `-runtime-config.file` is not specified. This change is also reflected in YAML configuration, where old `limits.per_tenant_override_config` and `limits.per_tenant_override_period` fields are replaced with `runtime_config.file` and `runtime_config.period` respectively. #1749
* [CHANGE] Cortex now rejects data with duplicate labels. Previously, such data was accepted, with duplicate labels removed with only one value left. #1964
* [CHANGE] Changed the default value for `-distributor.ha-tracker.prefix` from `collectors/` to `ha-tracker/` in order to not clash with other keys (ie. ring) stored in the same key-value store. #1940
* [FEATURE] Experimental: Write-Ahead-Log added in ingesters for more data reliability against ingester crashes. #1103
  * `--ingester.wal-enabled`: Setting this to `true` enables writing to WAL during ingestion.
  * `--ingester.wal-dir`: Directory where the WAL data should be stored and/or recovered from.
  * `--ingester.checkpoint-enabled`: Set this to `true` to enable checkpointing of in-memory chunks to disk.
  * `--ingester.checkpoint-duration`: This is the interval at which checkpoints should be created.
  * `--ingester.recover-from-wal`: Set this to `true` to recover data from an existing WAL.
  * For more information, please checkout the ["Ingesters with WAL" guide](https://cortexmetrics.io/docs/guides/ingesters-with-wal/).
* [FEATURE] The distributor can now drop labels from samples (similar to the removal of the replica label for HA ingestion) per user via the `distributor.drop-label` flag. #1726
* [FEATURE] Added flag `debug.mutex-profile-fraction` to enable mutex profiling #1969
* [FEATURE] Added `global` ingestion rate limiter strategy. Deprecated `-distributor.limiter-reload-period` flag. #1766
* [FEATURE] Added support for Microsoft Azure blob storage to be used for storing chunk data. #1913
* [FEATURE] Added readiness probe endpoint`/ready` to queriers. #1934
* [FEATURE] Added "multi" KV store that can interact with two other KV stores, primary one for all reads and writes, and secondary one, which only receives writes. Primary/secondary store can be modified in runtime via runtime-config mechanism (previously "overrides"). #1749
* [FEATURE] Added support to store ring tokens to a file and read it back on startup, instead of generating/fetching the tokens to/from the ring. This feature can be enabled with the flag `-ingester.tokens-file-path`. #1750
* [FEATURE] Experimental TSDB: Added `/series` API endpoint support with TSDB blocks storage. #1830
* [FEATURE] Experimental TSDB: Added TSDB blocks `compactor` component, which iterates over users blocks stored in the bucket and compact them according to the configured block ranges. #1942
* [ENHANCEMENT] metric `cortex_ingester_flush_reasons` gets a new `reason` value: `Spread`, when `-ingester.spread-flushes` option is enabled. #1978
* [ENHANCEMENT] Added `password` and `enable_tls` options to redis cache configuration. Enables usage of Microsoft Azure Cache for Redis service. #1923
* [ENHANCEMENT] Upgraded Kubernetes API version for deployments from `extensions/v1beta1` to `apps/v1`. #1941
* [ENHANCEMENT] Experimental TSDB: Open existing TSDB on startup to prevent ingester from becoming ready before it can accept writes. The max concurrency is set via `--experimental.tsdb.max-tsdb-opening-concurrency-on-startup`. #1917
* [ENHANCEMENT] Experimental TSDB: Querier now exports aggregate metrics from Thanos bucket store and in memory index cache (many metrics to list, but all have `cortex_querier_bucket_store_` or `cortex_querier_blocks_index_cache_` prefix). #1996
* [ENHANCEMENT] Experimental TSDB: Improved multi-tenant bucket store. #1991
  * Allowed to configure the blocks sync interval via `-experimental.tsdb.bucket-store.sync-interval` (0 disables the sync)
  * Limited the number of tenants concurrently synched by `-experimental.tsdb.bucket-store.block-sync-concurrency`
  * Renamed `cortex_querier_sync_seconds` metric to `cortex_querier_blocks_sync_seconds`
  * Track `cortex_querier_blocks_sync_seconds` metric for the initial sync too
* [BUGFIX] Fixed unnecessary CAS operations done by the HA tracker when the jitter is enabled. #1861
* [BUGFIX] Fixed ingesters getting stuck in a LEAVING state after coming up from an ungraceful exit. #1921
* [BUGFIX] Reduce memory usage when ingester Push() errors. #1922
* [BUGFIX] Table Manager: Fixed calculation of expected tables and creation of tables from next active schema considering grace period. #1976
* [BUGFIX] Experimental TSDB: Fixed ingesters consistency during hand-over when using experimental TSDB blocks storage. #1854 #1818
* [BUGFIX] Experimental TSDB: Fixed metrics when using experimental TSDB blocks storage. #1981 #1982 #1990 #1983
* [BUGFIX] Experimental memberlist: Use the advertised address when sending packets to other peers of the Gossip memberlist. #1857
* [BUGFIX] Experimental TSDB: Fixed incorrect query results introduced in #2604 caused by a buffer incorrectly reused while iterating samples. #2697

### Upgrading PostgreSQL (if you're using configs service)

Reference: <https://github.com/golang-migrate/migrate/tree/master/database/postgres#upgrading-from-v1>

1. Install the migrate package cli tool: <https://github.com/golang-migrate/migrate/tree/master/cmd/migrate#installation>
2. Drop the `schema_migrations` table: `DROP TABLE schema_migrations;`.
2. Run the migrate command:

```bash
migrate  -path <absolute_path_to_cortex>/cmd/cortex/migrations -database postgres://localhost:5432/database force 2
```

### Known issues

- The `cortex_prometheus_rule_group_last_evaluation_timestamp_seconds` metric, tracked by the ruler, is not unregistered for rule groups not being used anymore. This issue will be fixed in the next Cortex release (see [2033](https://github.com/cortexproject/cortex/issues/2033)).

- Write-Ahead-Log (WAL) does not have automatic repair of corrupt checkpoint or WAL segments, which is possible if ingester crashes abruptly or the underlying disk corrupts. Currently the only way to resolve this is to manually delete the affected checkpoint and/or WAL segments. Automatic repair will be added in the future releases.

## Cortex 0.4.0 / 2019-12-02

* [CHANGE] The frontend component has been refactored to be easier to re-use. When upgrading the frontend, cache entries will be discarded and re-created with the new protobuf schema. #1734
* [CHANGE] Removed direct DB/API access from the ruler. `-ruler.configs.url` has been now deprecated. #1579
* [CHANGE] Removed `Delta` encoding. Any old chunks with `Delta` encoding cannot be read anymore. If `ingester.chunk-encoding` is set to `Delta` the ingester will fail to start. #1706
* [CHANGE] Setting `-ingester.max-transfer-retries` to 0 now disables hand-over when ingester is shutting down. Previously, zero meant infinite number of attempts. #1771
* [CHANGE] `dynamo` has been removed as a valid storage name to make it consistent for all components. `aws` and `aws-dynamo` remain as valid storage names.
* [CHANGE/FEATURE] The frontend split and cache intervals can now be configured using the respective flag `--querier.split-queries-by-interval` and `--frontend.cache-split-interval`.
  * If `--querier.split-queries-by-interval` is not provided request splitting is disabled by default.
  * __`--querier.split-queries-by-day` is still accepted for backward compatibility but has been deprecated. You should now use `--querier.split-queries-by-interval`. We recommend a to use a multiple of 24 hours.__
* [FEATURE] Global limit on the max series per user and metric #1760
  * `-ingester.max-global-series-per-user`
  * `-ingester.max-global-series-per-metric`
  * Requires `-distributor.replication-factor` and `-distributor.shard-by-all-labels` set for the ingesters too
* [FEATURE] Flush chunks with stale markers early with `ingester.max-stale-chunk-idle`. #1759
* [FEATURE] EXPERIMENTAL: Added new KV Store backend based on memberlist library. Components can gossip about tokens and ingester states, instead of using Consul or Etcd. #1721
* [FEATURE] EXPERIMENTAL: Use TSDB in the ingesters & flush blocks to S3/GCS ala Thanos. This will let us use an Object Store more efficiently and reduce costs. #1695
* [FEATURE] Allow Query Frontend to log slow queries with `frontend.log-queries-longer-than`. #1744
* [FEATURE] Add HTTP handler to trigger ingester flush & shutdown - used when running as a stateful set with the WAL enabled.  #1746
* [FEATURE] EXPERIMENTAL: Added GCS support to TSDB blocks storage. #1772
* [ENHANCEMENT] Reduce memory allocations in the write path. #1706
* [ENHANCEMENT] Consul client now follows recommended practices for blocking queries wrt returned Index value. #1708
* [ENHANCEMENT] Consul client can optionally rate-limit itself during Watch (used e.g. by ring watchers) and WatchPrefix (used by HA feature) operations. Rate limiting is disabled by default. New flags added: `--consul.watch-rate-limit`, and `--consul.watch-burst-size`. #1708
* [ENHANCEMENT] Added jitter to HA deduping heartbeats, configure using `distributor.ha-tracker.update-timeout-jitter-max` #1534
* [ENHANCEMENT] Add ability to flush chunks with stale markers early. #1759
* [BUGFIX] Stop reporting successful actions as 500 errors in KV store metrics. #1798
* [BUGFIX] Fix bug where duplicate labels can be returned through metadata APIs. #1790
* [BUGFIX] Fix reading of old, v3 chunk data. #1779
* [BUGFIX] Now support IAM roles in service accounts in AWS EKS. #1803
* [BUGFIX] Fixed duplicated series returned when querying both ingesters and store with the experimental TSDB blocks storage. #1778

In this release we updated the following dependencies:

- gRPC v1.25.0  (resulted in a drop of 30% CPU usage when compression is on)
- jaeger-client v2.20.0
- aws-sdk-go to v1.25.22

## Cortex 0.3.0 / 2019-10-11

This release adds support for Redis as an alternative to Memcached, and also includes many optimisations which reduce CPU and memory usage.

* [CHANGE] Gauge metrics were renamed to drop the `_total` suffix. #1685
  * In Alertmanager, `alertmanager_configs_total` is now `alertmanager_configs`
  * In Ruler, `scheduler_configs_total` is now `scheduler_configs`
  * `scheduler_groups_total` is now `scheduler_groups`.
* [CHANGE] `--alertmanager.configs.auto-slack-root` flag was dropped as auto Slack root is not supported anymore. #1597
* [CHANGE] In table-manager, default DynamoDB capacity was reduced from 3,000 units to 1,000 units. We recommend you do not run with the defaults: find out what figures are needed for your environment and set that via `-dynamodb.periodic-table.write-throughput` and `-dynamodb.chunk-table.write-throughput`.
* [FEATURE] Add Redis support for caching #1612
* [FEATURE] Allow spreading chunk writes across multiple S3 buckets #1625
* [FEATURE] Added `/shutdown` endpoint for ingester to shutdown all operations of the ingester. #1746
* [ENHANCEMENT] Upgraded Prometheus to 2.12.0 and Alertmanager to 0.19.0. #1597
* [ENHANCEMENT] Cortex is now built with Go 1.13 #1675, #1676, #1679
* [ENHANCEMENT] Many optimisations, mostly impacting ingester and querier: #1574, #1624, #1638, #1644, #1649, #1654, #1702

Full list of changes: <https://github.com/cortexproject/cortex/compare/v0.2.0...v0.3.0>

## Cortex 0.2.0 / 2019-09-05

This release has several exciting features, the most notable of them being setting `-ingester.spread-flushes` to potentially reduce your storage space by upto 50%.

* [CHANGE] Flags changed due to changes upstream in Prometheus Alertmanager #929:
  * `alertmanager.mesh.listen-address` is now `cluster.listen-address`
  * `alertmanager.mesh.peer.host` and `alertmanager.mesh.peer.service` can be replaced by `cluster.peer`
  * `alertmanager.mesh.hardware-address`, `alertmanager.mesh.nickname`, `alertmanager.mesh.password`, and `alertmanager.mesh.peer.refresh-interval` all disappear.
* [CHANGE] --claim-on-rollout flag deprecated; feature is now always on #1566
* [CHANGE] Retention period must now be a multiple of periodic table duration #1564
* [CHANGE] The value for the name label for the chunks memcache in all `cortex_cache_` metrics is now `chunksmemcache` (before it was `memcache`) #1569
* [FEATURE] Makes the ingester flush each timeseries at a specific point in the max-chunk-age cycle with `-ingester.spread-flushes`. This means multiple replicas of a chunk are very likely to contain the same contents which cuts chunk storage space by up to 66%. #1578
* [FEATURE] Make minimum number of chunk samples configurable per user #1620
* [FEATURE] Honor HTTPS for custom S3 URLs #1603
* [FEATURE] You can now point the query-frontend at a normal Prometheus for parallelisation and caching #1441
* [FEATURE] You can now specify `http_config` on alert receivers #929
* [FEATURE] Add option to use jump hashing to load balance requests to memcached #1554
* [FEATURE] Add status page for HA tracker to distributors #1546
* [FEATURE] The distributor ring page is now easier to read with alternate rows grayed out #1621

## Cortex 0.1.0 / 2019-08-07

* [CHANGE] HA Tracker flags were renamed to provide more clarity #1465
  * `distributor.accept-ha-labels` is now `distributor.ha-tracker.enable`
  * `distributor.accept-ha-samples` is now `distributor.ha-tracker.enable-for-all-users`
  * `ha-tracker.replica` is now `distributor.ha-tracker.replica`
  * `ha-tracker.cluster` is now `distributor.ha-tracker.cluster`
* [FEATURE] You can specify "heap ballast" to reduce Go GC Churn #1489
* [BUGFIX] HA Tracker no longer always makes a request to Consul/Etcd when a request is not from the active replica #1516
* [BUGFIX] Queries are now correctly cancelled by the query-frontend #1508
