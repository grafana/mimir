# Changelog

## main / unreleased

### Grafana Mimir

* [CHANGE] Store-gateway: Remove experimental `-blocks-storage.bucket-store.max-concurrent-reject-over-limit` flag. #3706
* [FEATURE] Store-gateway: streaming of series. The store-gateway can now stream results back to the querier instead of buffering them. This is expected to greatly reduce peak memory consumption while keeping latency the same. You can enable this feature by setting `-blocks-storage.bucket-store.batch-series-size` to a value in the high thousands (5000-10000). This is still an experimental feature and is subject to a changing API and instability. #3540 #3546 #3587 #3606 #3611 #3620 #3645 #3355 #3697 #3666 #3687 #3728 #3739 #3751 #3779
* [ENHANCEMENT] Added new metric `thanos_shipper_last_successful_upload_time`: Unix timestamp (in seconds) of the last successful TSDB block uploaded to the bucket. #3627
* [ENHANCEMENT] Ruler: Added `-ruler.alertmanager-client.tls-enabled` configuration for alertmanager client. #3432 #3597
* [ENHANCEMENT] Activity tracker logs now have `component=activity-tracker` label. #3556
* [ENHANCEMENT] Distributor: remove labels with empty values #2439
* [ENHANCEMENT] Query-frontend: track query HTTP requests in the Activity Tracker. #3561
* [ENHANCEMENT] Store-gateway: Add experimental alternate implementation of index-header reader that does not use memory mapped files. The index-header reader is expected to improve stability of the store-gateway. You can enable this implementation with the flag `-blocks-storage.bucket-store.index-header.stream-reader-enabled`. #3639 #3691 #3703 #3742 #3785 #3787 #3797
* [ENHANCEMENT] Query-scheduler: add `cortex_query_scheduler_cancelled_requests_total` metric to track the number of requests that are already cancelled when dequeued. #3696
* [ENHANCEMENT] Store-gateway: add `cortex_bucket_store_partitioner_extended_ranges_total` metric to keep track of the ranges that the partitioner decided to overextend and merge in order to save API call to the object storage. #3769
* [ENHANCEMENT] Compactor: Auto-forget unhealthy compactors after ten failed ring heartbeats. #3771
* [BUGFIX] Log the names of services that are not yet running rather than `unsupported value type` when calling `/ready` and some services are not running. #3625
* [BUGFIX] Alertmanager: Fix template spurious deletion with relative data dir. #3604
* [BUGFIX] Security: update prometheus/exporter-toolkit for CVE-2022-46146. #3675
* [BUGFIX] Debian package: Fix post-install, environment file path and user creation. #3720
* [BUGFIX] memberlist: Fix panic during Mimir startup when Mimir receives gossip message before it's ready. #3746
* [BUGFIX] Store-gateway: fix `cortex_bucket_store_partitioner_requested_bytes_total` metric to not double count overlapping ranges. #3769
* [BUGFIX] Update `github.com/thanos-io/objstore` to address issue with Multipart PUT on s3-compatible Object Storage. #3802 #3821
* [BUGFIX] Distributor, Query-scheduler: Make sure ring metrics include a `cortex_` prefix as expected by dashboards. #3809

### Mixin

* [ENHANCEMENT] Alerts: Added `MimirIngesterInstanceHasNoTenants` alert that fires when an ingester replica is not receiving write requests for any tenant. #3681
* [ENHANCEMENT] Alerts: Extended `MimirAllocatingTooMuchMemory` to check read-write deployment containers. #3710
* [BUGFIX] Alerts: Fixed `MimirIngesterRestarts` alert when Mimir is deployed in read-write mode. #3716
* [BUGFIX] Alerts: Fixed `MimirIngesterHasNotShippedBlocks` and `MimirIngesterHasNotShippedBlocksSinceStart` alerts for when Mimir is deployed in read-write or monolithic modes and updated them to use new `thanos_shipper_last_successful_upload_time` metric. #3627
* [BUGFIX] Alerts: Fixed `MimirMemoryMapAreasTooHigh` alert when Mimir is deployed in read-write mode. #3626
* [BUGFIX] Alerts: Fixed `MimirCompactorSkippedBlocksWithOutOfOrderChunks` matching on non-existent label. #3628
* [BUGFIX] Dashboards: Fix `Rollout Progress` dashboard incorrectly using Gateway metrics when Gateway was not enabled. #3709
* [BUGFIX] Tenants dashboard: Make it compatible with all deployment types. #3754
* [BUGFIX] Alerts: Fixed `MimirCompactorHasNotUploadedBlocks` to not fire if compactor has nothing to do. #3793

### Jsonnet

* [CHANGE] Replaced the deprecated `policy/v1beta1` with `policy/v1` when configuring a PodDisruptionBudget for read-write deployment mode. #3811
* [ENHANCEMENT] Update `rollout-operator` to `v0.2.0`. #3624
* [ENHANCEMENT] Add `user_24M` and `user_32M` classes to operations config. #3367
* [BUGFIX] Apply ingesters and store-gateways per-zone CLI flags overrides to read-write deployment mode too. #3766
* [BUGFIX] Apply overrides-exporter CLI flags to mimir-backend when running Mimir in read-write deployment mode. #3790

### Mimirtool

### Documentation

* [BUGFIX] Querier: Remove assertion that the `-querier.max-concurrent` flag must also be set for the query-frontend. #3678
* [ENHANCEMENT] Update migration from cortex documentation. #3662

### Tools

## 2.5.0

### Grafana Mimir

* [CHANGE] Flag `-azure.msi-resource` is now ignored, and will be removed in Mimir 2.7. This setting is now made automatically by Azure. #2682
* [CHANGE] Experimental flag `-blocks-storage.tsdb.out-of-order-capacity-min` has been removed. #3261
* [CHANGE] Distributor: Wrap errors from pushing to ingesters with useful context, for example clarifying timeouts. #3307
* [CHANGE] The default value of `-server.http-write-timeout` has changed from 30s to 2m. #3346
* [CHANGE] Reduce period of health checks in connection pools for querier->store-gateway, ruler->ruler, and alertmanager->alertmanager clients to 10s. This reduces the time to fail a gRPC call when the remote stops responding. #3168
* [CHANGE] Hide TSDB block ranges period config from doc and mark it experimental. #3518
* [FEATURE] Alertmanager: added Discord support. #3309
* [ENHANCEMENT] Added `-server.tls-min-version` and `-server.tls-cipher-suites` flags to configure cipher suites and min TLS version supported by HTTP and gRPC servers. #2898
* [ENHANCEMENT] Distributor: Add age filter to forwarding functionality, to not forward samples which are older than defined duration. If such samples are not ingested, `cortex_discarded_samples_total{reason="forwarded-sample-too-old"}` is increased. #3049 #3113
* [ENHANCEMENT] Store-gateway: Reduce memory allocation when generating ids in index cache. #3179
* [ENHANCEMENT] Query-frontend: truncate queries based on the configured creation grace period (`--validation.create-grace-period`) to avoid querying too far into the future. #3172
* [ENHANCEMENT] Ingester: Reduce activity tracker memory allocation. #3203
* [ENHANCEMENT] Query-frontend: Log more detailed information in the case of a failed query. #3190
* [ENHANCEMENT] Added `-usage-stats.installation-mode` configuration to track the installation mode via the anonymous usage statistics. #3244
* [ENHANCEMENT] Compactor: Add new `cortex_compactor_block_max_time_delta_seconds` histogram for detecting if compaction of blocks is lagging behind. #3240 #3429
* [ENHANCEMENT] Ingester: reduced the memory footprint of active series custom trackers. #2568
* [ENHANCEMENT] Distributor: Include `X-Scope-OrgId` header in requests forwarded to configured forwarding endpoint. #3283 #3385
* [ENHANCEMENT] Alertmanager: reduced memory utilization in Mimir clusters with a large number of tenants. #3309
* [ENHANCEMENT] Add experimental flag `-shutdown-delay` to allow components to wait after receiving SIGTERM and before stopping. In this time the component returns 503 from /ready endpoint. #3298
* [ENHANCEMENT] Go: update to go 1.19.3. #3371
* [ENHANCEMENT] Alerts: added `RulerRemoteEvaluationFailing` alert, firing when communication between ruler and frontend fails in remote operational mode. #3177 #3389
* [ENHANCEMENT] Clarify which S3 signature versions are supported in the error "unsupported signature version". #3376
* [ENHANCEMENT] Store-gateway: improved index header reading performance. #3393 #3397 #3436
* [ENHANCEMENT] Store-gateway: improved performance of series matching. #3391
* [ENHANCEMENT] Move the validation of incoming series before the distributor's forwarding functionality, so that we don't forward invalid series. #3386 #3458
* [ENHANCEMENT] S3 bucket configuration now validates that the endpoint does not have the bucket name prefix. #3414
* [ENHANCEMENT] Query-frontend: added "fetched index bytes" to query statistics, so that the statistics contain the total bytes read by store-gateways from TSDB block indexes. #3206
* [ENHANCEMENT] Distributor: push wrapper should only receive unforwarded samples. #2980
* [ENHANCEMENT] Added the `/api/v1/status/config` API to maintain API compatibility with prometheus. #3596
* [BUGFIX] Flusher: Add `Overrides` as a dependency to prevent panics when starting with `-target=flusher`. #3151
* [BUGFIX] Updated `golang.org/x/text` dependency to fix CVE-2022-32149. #3285
* [BUGFIX] Query-frontend: properly close gRPC streams to the query-scheduler to stop memory and goroutines leak. #3302
* [BUGFIX] Ruler: persist evaluation delay configured in the rulegroup. #3392
* [BUGFIX] Ring status pages: show 100% ownership as "100%", not "1e+02%". #3435
* [BUGFIX] Fix panics in OTLP ingest path when parse errors exist. #3538

### Mixin

* [CHANGE] Alerts: Change `MimirSchedulerQueriesStuck` `for` time to 7 minutes to account for the time it takes for HPA to scale up. #3223
* [CHANGE] Dashboards: Removed the `Querier > Stages` panel from the `Mimir / Queries` dashboard. #3311
* [CHANGE] Configuration: The format of the `autoscaling` section of the configuration has changed to support more components. #3378
  * Instead of specific config variables for each component, they are listed in a dictionary. For example, `autoscaling.querier_enabled` becomes `autoscaling.querier.enabled`.
* [FEATURE] Dashboards: Added "Mimir / Overview resources" dashboard, providing an high level view over a Mimir cluster resources utilization. #3481
* [FEATURE] Dashboards: Added "Mimir / Overview networking" dashboard, providing an high level view over a Mimir cluster network bandwidth, inflight requests and TCP connections. #3487
* [FEATURE] Compile baremetal mixin along k8s mixin. #3162 #3514
* [ENHANCEMENT] Alerts: Add MimirRingMembersMismatch firing when a component does not have the expected number of running jobs. #2404
* [ENHANCEMENT] Dashboards: Add optional row about the Distributor's metric forwarding feature to the `Mimir / Writes` dashboard. #3182 #3394 #3394 #3461
* [ENHANCEMENT] Dashboards: Remove the "Instance Mapper" row from the "Alertmanager Resources Dashboard". This is a Grafana Cloud specific service and not relevant for external users. #3152
* [ENHANCEMENT] Dashboards: Add "remote read", "metadata", and "exemplar" queries to "Mimir / Overview" dashboard. #3245
* [ENHANCEMENT] Dashboards: Use non-red colors for non-error series in the "Mimir / Overview" dashboard. #3246
* [ENHANCEMENT] Dashboards: Add support to multi-zone deployments for the experimental read-write deployment mode. #3256
* [ENHANCEMENT] Dashboards: If enabled, add new row to the `Mimir / Writes` for distributor autoscaling metrics. #3378
* [ENHANCEMENT] Dashboards: Add read path insights row to the "Mimir / Tenants" dashboard. #3326
* [ENHANCEMENT] Alerts: Add runbook urls for alerts. #3452
* [ENHANCEMENT] Configuration: Make it possible to configure namespace label, job label, and job prefix. #3482
* [ENHANCEMENT] Dashboards: improved resources and networking dashboards to work with read-write deployment mode too. #3497 #3504 #3519 #3531
* [ENHANCEMENT] Alerts: Added "MimirDistributorForwardingErrorRate" alert, which fires on high error rates in the distributor’s forwarding feature. #3200
* [ENHANCEMENT] Improve phrasing in Overview dashboard. #3488
* [BUGFIX] Dashboards: Fix legend showing `persistentvolumeclaim` when using `deployment_type=baremetal` for `Disk space utilization` panels. #3173 #3184
* [BUGFIX] Alerts: Fixed `MimirGossipMembersMismatch` alert when Mimir is deployed in read-write mode. #3489
* [BUGFIX] Dashboards: Remove "Inflight requests" from object store panels because the panel is not tracking the inflight requests to object storage. #3521

### Jsonnet

* [CHANGE] Replaced the deprecated `policy/v1beta1` with `policy/v1` when configuring a PodDisruptionBudget. #3284
* [CHANGE] [Common storage configuration](https://grafana.com/docs/mimir/v2.3.x/operators-guide/configure/configure-object-storage-backend/#common-configuration) is now used to configure object storage in all components. This is a breaking change in terms of Jsonnet manifests and also a CLI flag update for components that use object storage, so it will require a rollout of those components. The changes include: #3257
  * `blocks_storage_backend` was renamed to `storage_backend` and is now used as the common storage backend for all components.
    * So were the related `blocks_storage_azure_account_(name|key)` and `blocks_storage_s3_endpoint` configurations.
  * `storage_s3_endpoint` is now rendered by default using the `aws_region` configuration instead of a hardcoded `us-east-1`.
  * `ruler_client_type` and `alertmanager_client_type` were renamed to `ruler_storage_backend` and `alertmanager_storage_backend` respectively, and their corresponding CLI flags won't be rendered unless explicitly set to a value different from the one in `storage_backend` (like `local`).
  * `alertmanager_s3_bucket_name`, `alertmanager_gcs_bucket_name` and `alertmanager_azure_container_name` have been removed, and replaced by a single `alertmanager_storage_bucket_name` configuration used for all object storages.
  * `genericBlocksStorageConfig` configuration object was removed, and so any extensions to it will be now ignored. Use `blockStorageConfig` instead.
  * `rulerClientConfig` and `alertmanagerStorageClientConfig` configuration objects were renamed to `rulerStorageConfig` and `alertmanagerStorageConfig` respectively, and so any extensions to their previous names will be now ignored. Use the new names instead.
  * The CLI flags `*.s3.region` are no longer rendered as they are optional and the region can be inferred by Mimir by performing an initial API call to the endpoint.
  * The migration to this change should usually consist of:
    * Renaming `blocks_storage_backend` key to `storage_backend`.
    * For Azure/S3:
      * Renaming `blocks_storage_(azure|s3)_*` configurations to `storage_(azure|s3)_*`.
      * If `ruler_storage_(azure|s3)_*` and `alertmanager_storage_(azure|s3)_*` keys were different from the `block_storage_*` ones, they should be now provided using CLI flags, see [configuration reference](https://grafana.com/docs/mimir/v2.3.x/operators-guide/configure/reference-configuration-parameters/) for more details.
    * Removing `ruler_client_type` and `alertmanager_client_type` if their value match the `storage_backend`, or renaming them to their new names otherwise.
    * Reviewing any possible extensions to `genericBlocksStorageConfig`, `rulerClientConfig` and `alertmanagerStorageClientConfig` and moving them to the corresponding new options.
    * Renaming the alertmanager's bucket name configuration from provider-specific to the new `alertmanager_storage_bucket_name` key.
* [CHANGE] The `overrides-exporter.libsonnet` file is now always imported. The overrides-exporter can be enabled in jsonnet setting the following: #3379
  ```jsonnet
  {
    _config+:: {
      overrides_exporter_enabled: true,
    }
  }
  ```
* [FEATURE] Added support for experimental read-write deployment mode. Enabling the read-write deployment mode on a existing Mimir cluster is a destructive operation, because the cluster will be re-created. If you're creating a new Mimir cluster, you can deploy it in read-write mode adding the following configuration: #3379 #3475 #3405
  ```jsonnet
  {
    _config+:: {
      deployment_mode: 'read-write',

      // See operations/mimir/read-write-deployment.libsonnet for more configuration options.
      mimir_write_replicas: 3,
      mimir_read_replicas: 2,
      mimir_backend_replicas: 3,
    }
  }
  ```
* [ENHANCEMENT] Add autoscaling support to the `mimir-read` component when running the read-write-deployment model. #3419
* [ENHANCEMENT] Added `$._config.usageStatsConfig` to track the installation mode via the anonymous usage statistics. #3294
* [ENHANCEMENT] The query-tee node port (`$._config.query_tee_node_port`) is now optional. #3272
* [ENHANCEMENT] Add support for autoscaling distributors. #3378
* [ENHANCEMENT] Make auto-scaling logic ensure integer KEDA thresholds. #3512
* [BUGFIX] Fixed query-scheduler ring configuration for dedicated ruler's queries and query-frontends. #3237 #3239
* [BUGFIX] Jsonnet: Fix auto-scaling so that ruler-querier CPU threshold is a string-encoded integer millicores value. #3520

### Mimirtool

* [FEATURE] Added `mimirtool alertmanager verify` command to validate configuration without uploading. #3440
* [ENHANCEMENT] Added `mimirtool rules delete-namespace` command to delete all of the rule groups in a namespace including the namespace itself. #3136
* [ENHANCEMENT] Refactor `mimirtool analyze prometheus`: add concurrency and resiliency #3349
  * Add `--concurrency` flag. Default: number of logical CPUs
* [BUGFIX] `--log.level=debug` now correctly prints the response from the remote endpoint when a request fails. #3180

### Documentation

* [ENHANCEMENT] Documented how to configure HA deduplication using Consul in a Mimir Helm deployment. #2972
* [ENHANCEMENT] Improve `MimirQuerierAutoscalerNotActive` runbook. #3186
* [ENHANCEMENT] Improve `MimirSchedulerQueriesStuck` runbook to reflect debug steps with querier auto-scaling enabled. #3223
* [ENHANCEMENT] Use imperative for docs titles. #3178 #3332 #3343
* [ENHANCEMENT] Docs: mention gRPC compression in "Production tips". #3201
* [ENHANCEMENT] Update ADOPTERS.md. #3224 #3225
* [ENHANCEMENT] Add a note for jsonnet deploying. #3213
* [ENHANCEMENT] out-of-order runbook update with use case. #3253
* [ENHANCEMENT] Fixed TSDB retention mentioned in the "Recover source blocks from ingesters" runbook. #3280
* [ENHANCEMENT] Run Grafana Mimir in production using the Helm chart. #3072
* [ENHANCEMENT] Use common configuration in the tutorial. #3282
* [ENHANCEMENT] Updated detailed steps for migrating blocks from Thanos to Mimir. #3290
* [ENHANCEMENT] Add scheme to DNS service discovery docs. #3450
* [BUGFIX] Remove reference to file that no longer exists in contributing guide. #3404
* [BUGFIX] Fix some minor typos in the contributing guide and on the runbooks page. #3418
* [BUGFIX] Fix small typos in API reference. #3526
* [BUGFIX] Fixed TSDB retention mentioned in the "Recover source blocks from ingesters" runbook. #3278
* [BUGFIX] Fixed configuration example in the "Configuring the Grafana Mimir query-frontend to work with Prometheus" guide. #3374

### Tools

* [FEATURE] Add `copyblocks` tool, to copy Mimir blocks between two GCS buckets. #3264
* [ENHANCEMENT] copyblocks: copy no-compact global markers and optimize min time filter check. #3268
* [ENHANCEMENT] Mimir rules GitHub action: Added the ability to change default value of `label` when running `prepare` command. #3236
* [BUGFIX] Mimir rules Github action: Fix single line output. #3421

## 2.4.0

### Grafana Mimir

* [CHANGE] Distributor: change the default value of `-distributor.remote-timeout` to `2s` from `20s` and `-distributor.forwarding.request-timeout` to `2s` from `10s` to improve distributor resource usage when ingesters crash. #2728 #2912
* [CHANGE] Anonymous usage statistics tracking: added the `-ingester.ring.store` value. #2981
* [CHANGE] Series metadata `HELP` that is longer than `-validation.max-metadata-length` is now truncated silently, instead of being dropped with a 400 status code. #2993
* [CHANGE] Ingester: changed default setting for `-ingester.ring.readiness-check-ring-health` from `true` to `false`. #2953
* [CHANGE] Anonymous usage statistics tracking has been enabled by default, to help Mimir maintainers make better decisions to support the open source community. #2939 #3034
* [CHANGE] Anonymous usage statistics tracking: added the minimum and maximum value of `-ingester.out-of-order-time-window`. #2940
* [CHANGE] The default hash ring heartbeat period for distributors, ingesters, rulers and compactors has been increased from `5s` to `15s`. Now the default heartbeat period for all Mimir hash rings is `15s`. #3033
* [CHANGE] Reduce the default TSDB head compaction concurrency (`-blocks-storage.tsdb.head-compaction-concurrency`) from 5 to 1, in order to reduce CPU spikes. #3093
* [CHANGE] Ruler: the ruler's [remote evaluation mode](https://grafana.com/docs/mimir/latest/operators-guide/architecture/components/ruler/#remote) (`-ruler.query-frontend.address`) is now stable. #3109
* [CHANGE] Limits: removed the deprecated YAML configuration option `active_series_custom_trackers_config`. Please use `active_series_custom_trackers` instead. #3110
* [CHANGE] Ingester: removed the deprecated configuration option `-ingester.ring.join-after`. #3111
* [CHANGE] Querier: removed the deprecated configuration option `-querier.shuffle-sharding-ingesters-lookback-period`. The value of `-querier.query-ingesters-within` is now used internally for shuffle sharding lookback, while you can use `-querier.shuffle-sharding-ingesters-enabled` to enable or disable shuffle sharding on the read path. #3111
* [CHANGE] Memberlist: cluster label verification feature (`-memberlist.cluster-label` and `-memberlist.cluster-label-verification-disabled`) is now marked as stable. #3108
* [CHANGE] Distributor: only single per-tenant forwarding endpoint can be configured now. Support for per-rule endpoint has been removed. #3095
* [FEATURE] Query-scheduler: added an experimental ring-based service discovery support for the query-scheduler. Refer to [query-scheduler configuration](https://grafana.com/docs/mimir/next/operators-guide/architecture/components/query-scheduler/#configuration) for more information. #2957
* [FEATURE] Introduced the experimental endpoint `/api/v1/user_limits` exposed by all components that load runtime configuration. This endpoint exposes realtime limits for the authenticated tenant, in JSON format. #2864 #3017
* [FEATURE] Query-scheduler: added the experimental configuration option `-query-scheduler.max-used-instances` to restrict the number of query-schedulers effectively used regardless how many replicas are running. This feature can be useful when using the experimental read-write deployment mode. #3005
* [ENHANCEMENT] Go: updated to go 1.19.2. #2637 #3127 #3129
* [ENHANCEMENT] Runtime config: don't unmarshal runtime configuration files if they haven't changed. This can save a bit of CPU and memory on every component using runtime config. #2954
* [ENHANCEMENT] Query-frontend: Add `cortex_frontend_query_result_cache_skipped_total` and `cortex_frontend_query_result_cache_attempted_total` metrics to track the reason why query results are not cached. #2855
* [ENHANCEMENT] Distributor: pool more connections per host when forwarding request. Mark requests as idempotent so they can be retried under some conditions. #2968
* [ENHANCEMENT] Distributor: failure to send request to forwarding target now also increments `cortex_distributor_forward_errors_total`, with `status_code="failed"`. #2968
* [ENHANCEMENT] Distributor: added support forwarding push requests via gRPC, using `httpgrpc` messages from weaveworks/common library. #2996
* [ENHANCEMENT] Query-frontend / Querier: increase internal backoff period used to retry connections to query-frontend / query-scheduler. #3011
* [ENHANCEMENT] Querier: do not log "error processing requests from scheduler" when the query-scheduler is shutting down. #3012
* [ENHANCEMENT] Query-frontend: query sharding process is now time-bounded and it is cancelled if the request is aborted. #3028
* [ENHANCEMENT] Query-frontend: improved Prometheus response JSON encoding performance. #2450
* [ENHANCEMENT] TLS: added configuration parameters to configure the client's TLS cipher suites and minimum version. The following new CLI flags have been added: #3070
  * `-alertmanager.alertmanager-client.tls-cipher-suites`
  * `-alertmanager.alertmanager-client.tls-min-version`
  * `-alertmanager.sharding-ring.etcd.tls-cipher-suites`
  * `-alertmanager.sharding-ring.etcd.tls-min-version`
  * `-compactor.ring.etcd.tls-cipher-suites`
  * `-compactor.ring.etcd.tls-min-version`
  * `-distributor.forwarding.grpc-client.tls-cipher-suites`
  * `-distributor.forwarding.grpc-client.tls-min-version`
  * `-distributor.ha-tracker.etcd.tls-cipher-suites`
  * `-distributor.ha-tracker.etcd.tls-min-version`
  * `-distributor.ring.etcd.tls-cipher-suites`
  * `-distributor.ring.etcd.tls-min-version`
  * `-ingester.client.tls-cipher-suites`
  * `-ingester.client.tls-min-version`
  * `-ingester.ring.etcd.tls-cipher-suites`
  * `-ingester.ring.etcd.tls-min-version`
  * `-memberlist.tls-cipher-suites`
  * `-memberlist.tls-min-version`
  * `-querier.frontend-client.tls-cipher-suites`
  * `-querier.frontend-client.tls-min-version`
  * `-querier.store-gateway-client.tls-cipher-suites`
  * `-querier.store-gateway-client.tls-min-version`
  * `-query-frontend.grpc-client-config.tls-cipher-suites`
  * `-query-frontend.grpc-client-config.tls-min-version`
  * `-query-scheduler.grpc-client-config.tls-cipher-suites`
  * `-query-scheduler.grpc-client-config.tls-min-version`
  * `-query-scheduler.ring.etcd.tls-cipher-suites`
  * `-query-scheduler.ring.etcd.tls-min-version`
  * `-ruler.alertmanager-client.tls-cipher-suites`
  * `-ruler.alertmanager-client.tls-min-version`
  * `-ruler.client.tls-cipher-suites`
  * `-ruler.client.tls-min-version`
  * `-ruler.query-frontend.grpc-client-config.tls-cipher-suites`
  * `-ruler.query-frontend.grpc-client-config.tls-min-version`
  * `-ruler.ring.etcd.tls-cipher-suites`
  * `-ruler.ring.etcd.tls-min-version`
  * `-store-gateway.sharding-ring.etcd.tls-cipher-suites`
  * `-store-gateway.sharding-ring.etcd.tls-min-version`
* [ENHANCEMENT] Store-gateway: Add `-blocks-storage.bucket-store.max-concurrent-reject-over-limit` option to allow requests that exceed the max number of inflight object storage requests to be rejected. #2999
* [ENHANCEMENT] Query-frontend: allow setting a separate limit on the total (before splitting/sharding) query length of range queries with the new experimental `-query-frontend.max-total-query-length` flag, which defaults to `-store.max-query-length` if unset or set to 0. #3058
* [ENHANCEMENT] Query-frontend: Lower TTL for cache entries overlapping the out-of-order samples ingestion window (re-using `-ingester.out-of-order-allowance` from ingesters). #2935
* [ENHANCEMENT] Ruler: added support to forcefully disable recording and/or alerting rules evaluation. The following new configuration options have been introduced, which can be overridden on a per-tenant basis in the runtime configuration: #3088
  * `-ruler.recording-rules-evaluation-enabled`
  * `-ruler.alerting-rules-evaluation-enabled`
* [ENHANCEMENT] Distributor: Improved error messages reported when the distributor fails to remote write to ingesters. #3055
* [ENHANCEMENT] Improved tracing spans tracked by distributors, ingesters and store-gateways. #2879 #3099 #3089
* [ENHANCEMENT] Ingester: improved the performance of label value cardinality endpoint. #3044
* [ENHANCEMENT] Ruler: use backoff retry on remote evaluation #3098
* [ENHANCEMENT] Query-frontend: Include multiple tenant IDs in query logs when present instead of dropping them. #3125
* [ENHANCEMENT] Query-frontend: truncate queries based on the configured blocks retention period (`-compactor.blocks-retention-period`) to avoid querying past this period. #3134
* [ENHANCEMENT] Alertmanager: reduced memory utilization in Mimir clusters with a large number of tenants. #3143
* [ENHANCEMENT] Store-gateway: added extra span logging to improve observability. #3131
* [ENHANCEMENT] Compactor: cleaning up different tenants' old blocks and updating bucket indexes is now more independent. This prevents a single tenant from delaying cleanup for other tenants. #2631
* [ENHANCEMENT] Distributor: request rate, ingestion rate, and inflight requests limits are now enforced before reading and parsing the body of the request. This makes the distributor more resilient against a burst of requests over those limit. #2419
* [BUGFIX] Querier: Fix 400 response while handling streaming remote read. #2963
* [BUGFIX] Fix a bug causing query-frontend, query-scheduler, and querier not failing if one of their internal components fail. #2978
* [BUGFIX] Querier: re-balance the querier worker connections when a query-frontend or query-scheduler is terminated. #3005
* [BUGFIX] Distributor: Now returns the quorum error from ingesters. For example, with replication_factor=3, two HTTP 400 errors and one HTTP 500 error, now the distributor will always return HTTP 400. Previously the behaviour was to return the error which the distributor first received. #2979
* [BUGFIX] Ruler: fix panic when ruler.external_url is explicitly set to an empty string ("") in YAML. #2915
* [BUGFIX] Alertmanager: Fix support for the Telegram API URL in the global settings. #3097
* [BUGFIX] Alertmanager: Fix parsing of label matchers without label value in the API used to retrieve alerts. #3097
* [BUGFIX] Ruler: Fix not restoring alert state for rule groups when other ruler replicas shut down. #3156
* [BUGFIX] Updated `golang.org/x/net` dependency to fix CVE-2022-27664. #3124
* [BUGFIX] Fix distributor from returning a `500` status code when a `400` was received from the ingester. #3211
* [BUGFIX] Fix incorrect OS value set in Mimir v2.3.* RPM packages. #3221

### Mixin

* [CHANGE] Alerts: MimirQuerierAutoscalerNotActive is now critical and fires after 1h instead of 15m. #2958
* [FEATURE] Dashboards: Added "Mimir / Overview" dashboards, providing an high level view over a Mimir cluster. #3122 #3147 #3155
* [ENHANCEMENT] Dashboards: Updated the "Writes" and "Rollout progress" dashboards to account for samples ingested via the new OTLP ingestion endpoint. #2919 #2938
* [ENHANCEMENT] Dashboards: Include per-tenant request rate in "Tenants" dashboard. #2874
* [ENHANCEMENT] Dashboards: Include inflight object store requests in "Reads" dashboard. #2914
* [ENHANCEMENT] Dashboards: Make queries used to find job, cluster and namespace for dropdown menus configurable. #2893
* [ENHANCEMENT] Dashboards: Include rate of label and series queries in "Reads" dashboard. #3065 #3074
* [ENHANCEMENT] Dashboards: Fix legend showing on per-pod panels. #2944
* [ENHANCEMENT] Dashboards: Use the "req/s" unit on panels showing the requests rate. #3118
* [ENHANCEMENT] Dashboards: Use a consistent color across dashboards for the error rate. #3154

### Jsonnet

* [FEATURE] Added support for query-scheduler ring-based service discovery. #3128
* [ENHANCEMENT] Querier autoscaling is now slower on scale downs: scale down 10% every 1m instead of 100%. #2962
* [BUGFIX] Memberlist: `gossip_member_label` is now set for ruler-queriers. #3141

### Mimirtool

* [ENHANCEMENT] mimirtool analyze: Store the query errors instead of exit during the analysis. #3052
* [BUGFIX] mimir-tool remote-read: fix returns where some conditions [return nil error even if there is error](https://github.com/grafana/cortex-tools/issues/260). #3053

### Documentation

* [ENHANCEMENT] Added documentation on how to configure storage retention. #2970
* [ENHANCEMENT] Improved gRPC clients config documentation. #3020
* [ENHANCEMENT] Added documentation on how to manage alerting and recording rules. #2983
* [ENHANCEMENT] Improved `MimirSchedulerQueriesStuck` runbook. #3006
* [ENHANCEMENT] Added "Cluster label verification" section to memberlist documentation. #3096
* [ENHANCEMENT] Mention compression in multi-zone replication documentation. #3107
* [BUGFIX] Fixed configuration option names in "Enabling zone-awareness via the Grafana Mimir Jsonnet". #3018
* [BUGFIX] Fixed `mimirtool analyze` parameters documentation. #3094
* [BUGFIX] Fixed YAML configuraton in the "Manage the configuration of Grafana Mimir with Helm" guide. #3042
* [BUGFIX] Fixed Alertmanager capacity planning documentation. #3132

### Tools

- [BUGFIX] trafficdump: Fixed panic occurring when `-success-only=true` and the captured request failed. #2863

## 2.3.1

### Grafana Mimir
* [BUGFIX] Query-frontend: query sharding took exponential time to map binary expressions. #3027
* [BUGFIX] Distributor: Stop panics on OTLP endpoint when a single metric has multiple timeseries. #3040

## 2.3.0

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
* [CHANGE] The tenant ID `__mimir_cluster` is reserved by Mimir and not allowed to store metrics. #2643
* [CHANGE] Purger: removed the purger component and moved its API endpoints `/purger/delete_tenant` and `/purger/delete_tenant_status` to the compactor at `/compactor/delete_tenant` and `/compactor/delete_tenant_status`. The new endpoints on the compactor are stable. #2644
* [CHANGE] Memberlist: Change the leave timeout duration (`-memberlist.leave-timeout duration`) from 5s to 20s and connection timeout (`-memberlist.packet-dial-timeout`) from 5s to 2s. This makes leave timeout 10x the connection timeout, so that we can communicate the leave to at least 1 node, if the first 9 we try to contact times out. #2669
* [CHANGE] Alertmanager: return status code `412 Precondition Failed` and log info message when alertmanager isn't configured for a tenant. #2635
* [CHANGE] Distributor: if forwarding rules are used to forward samples, exemplars are now removed from the request. #2710 #2725
* [CHANGE] Limits: change the default value of `max_global_series_per_metric` limit to `0` (disabled). Setting this limit by default does not provide much benefit because series are sharded by all labels. #2714
* [CHANGE] Ingester: experimental `-blocks-storage.tsdb.new-chunk-disk-mapper` has been removed, new chunk disk mapper is now always used, and is no longer marked experimental. Default value of `-blocks-storage.tsdb.head-chunks-write-queue-size` has changed to 1000000, this enables async chunk queue by default, which leads to improved latency on the write path when new chunks are created in ingesters. #2762
* [CHANGE] Ingester: removed deprecated `-blocks-storage.tsdb.isolation-enabled` option. TSDB-level isolation is now always disabled in Mimir. #2782
* [CHANGE] Compactor: `-compactor.partial-block-deletion-delay` must either be set to 0 (to disable partial blocks deletion) or a value higher than `4h`. #2787
* [CHANGE] Query-frontend: CLI flag `-query-frontend.align-querier-with-step` has been deprecated. Please use `-query-frontend.align-queries-with-step` instead. #2840
* [FEATURE] Compactor: Adds the ability to delete partial blocks after a configurable delay. This option can be configured per tenant. #2285
  - `-compactor.partial-block-deletion-delay`, as a duration string, allows you to set the delay since a partial block has been modified before marking it for deletion. A value of `0`, the default, disables this feature.
  - The metric `cortex_compactor_blocks_marked_for_deletion_total` has a new value for the `reason` label `reason="partial"`, when a block deletion marker is triggered by the partial block deletion delay.
* [FEATURE] Querier: enabled support for queries with negative offsets, which are not cached in the query results cache. #2429
* [FEATURE] EXPERIMENTAL: OpenTelemetry Metrics ingestion path on `/otlp/v1/metrics`. #695 #2436 #2461
* [FEATURE] Querier: Added support for tenant federation to metric metadata endpoint. #2467
* [FEATURE] Query-frontend: introduced experimental support to split instant queries by time. The instant query splitting can be enabled setting `-query-frontend.split-instant-queries-by-interval`. #2469 #2564 #2565 #2570 #2571 #2572 #2573 #2574 #2575 #2576 #2581 #2582 #2601 #2632 #2633 #2634 #2641 #2642 #2766
* [FEATURE] Introduced an experimental anonymous usage statistics tracking (disabled by default), to help Mimir maintainers make better decisions to support the open source community. The tracking system anonymously collects non-sensitive, non-personally identifiable information about the running Mimir cluster, and is disabled by default. #2643 #2662 #2685 #2732 #2733 #2735
* [FEATURE] Introduced an experimental deployment mode called read-write and running a fully featured Mimir cluster with three components: write, read and backend. The read-write deployment mode is a trade-off between the monolithic mode (only one component, no isolation) and the microservices mode (many components, high isolation). #2754 #2838
* [ENHANCEMENT] Distributor: Decreased distributor tests execution time. #2562
* [ENHANCEMENT] Alertmanager: Allow the HTTP `proxy_url` configuration option in the receiver's configuration. #2317
* [ENHANCEMENT] ring: optimize shuffle-shard computation when lookback is used, and all instances have registered timestamp within the lookback window. In that case we can immediately return origial ring, because we would select all instances anyway. #2309
* [ENHANCEMENT] Memberlist: added experimental memberlist cluster label support via `-memberlist.cluster-label` and `-memberlist.cluster-label-verification-disabled` CLI flags (and their respective YAML config options). #2354
* [ENHANCEMENT] Object storage can now be configured for all components using the `common` YAML config option key (or `-common.storage.*` CLI flags). #2330 #2347
* [ENHANCEMENT] Go: updated to go 1.18.4. #2400
* [ENHANCEMENT] Store-gateway, listblocks: list of blocks now includes stats from `meta.json` file: number of series, samples and chunks. #2425
* [ENHANCEMENT] Added more buckets to `cortex_ingester_client_request_duration_seconds` histogram metric, to correctly track requests taking longer than 1s (up until 16s). #2445
* [ENHANCEMENT] Azure client: Improve memory usage for large object storage downloads. #2408
* [ENHANCEMENT] Distributor: Add `-distributor.instance-limits.max-inflight-push-requests-bytes`. This limit protects the distributor against multiple large requests that together may cause an OOM, but are only a few, so do not trigger the `max-inflight-push-requests` limit. #2413
* [ENHANCEMENT] Distributor: Drop exemplars in distributor for tenants where exemplars are disabled. #2504
* [ENHANCEMENT] Runtime Config: Allow operator to specify multiple comma-separated yaml files in `-runtime-config.file` that will be merged in left to right order. #2583
* [ENHANCEMENT] Query sharding: shard binary operations only if it doesn't lead to non-shardable vector selectors in one of the operands. #2696
* [ENHANCEMENT] Add packaging for both debian based deb file and redhat based rpm file using FPM. #1803
* [ENHANCEMENT] Distributor: Add `cortex_distributor_query_ingester_chunks_deduped_total` and `cortex_distributor_query_ingester_chunks_total` metrics for determining how effective ingester chunk deduplication at query time is. #2713
* [ENHANCEMENT] Upgrade Docker base images to `alpine:3.16.2`. #2729
* [ENHANCEMENT] Ruler: Add `<prometheus-http-prefix>/api/v1/status/buildinfo` endpoint. #2724
* [ENHANCEMENT] Querier: Ensure all queries pulled from query-frontend or query-scheduler are immediately executed. The maximum workers concurrency in each querier is configured by `-querier.max-concurrent`. #2598
* [ENHANCEMENT] Distributor: Add `cortex_distributor_received_requests_total` and `cortex_distributor_requests_in_total` metrics to provide visiblity into appropriate per-tenant request limits. #2770
* [ENHANCEMENT] Distributor: Add single forwarding remote-write endpoint for a tenant (`forwarding_endpoint`), instead of using per-rule endpoints. This takes precendence over per-rule endpoints. #2801
* [ENHANCEMENT] Added `err-mimir-distributor-max-write-message-size` to the errors catalog. #2470
* [ENHANCEMENT] Add sanity check at startup to ensure the configured filesystem directories don't overlap for different components. #2828 #2947
* [BUGFIX] TSDB: Fixed a bug on the experimental out-of-order implementation that led to wrong query results. #2701
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
* [BUGFIX] Perform HA-deduplication before forwarding samples according to forwarding rules in the distributor. #2603 #2709
* [BUGFIX] Fix reporting of tracing spans from PromQL engine. #2707
* [BUGFIX] Apply relabel and drop_label rules before forwarding rules in the distributor. #2703
* [BUGFIX] Distributor: Register `cortex_discarded_requests_total` metric, which previously was not registered and therefore not exported. #2712
* [BUGFIX] Ruler: fix not restoring alerts' state at startup. #2648
* [BUGFIX] Ingester: Fix disk filling up after restarting ingesters with out-of-order support disabled while it was enabled before. #2799
* [BUGFIX] Memberlist: retry joining memberlist cluster on startup when no nodes are resolved. #2837
* [BUGFIX] Query-frontend: fix incorrect mapping of http status codes 413 to 500 when request is too large. #2819
* [BUGFIX] Alertmanager: revert upstream alertmananger to v0.24.0 to fix panic when unmarshalling email headers #2924 #2925

### Mixin

* [CHANGE] Dashboards: "Slow Queries" dashboard no longer works with versions older than Grafana 9.0. #2223
* [CHANGE] Alerts: use RSS memory instead of working set memory in the `MimirAllocatingTooMuchMemory` alert for ingesters. #2480
* [CHANGE] Dashboards: remove the "Cache - Latency (old)" panel from the "Mimir / Queries" dashboard. #2796
* [FEATURE] Dashboards: added support to experimental read-write deployment mode. #2780
* [ENHANCEMENT] Dashboards: added missed rule evaluations to the "Evaluations per second" panel in the "Mimir / Ruler" dashboard. #2314
* [ENHANCEMENT] Dashboards: add k8s resource requests to CPU and memory panels. #2346
* [ENHANCEMENT] Dashboards: add RSS memory utilization panel for ingesters, store-gateways and compactors. #2479
* [ENHANCEMENT] Dashboards: allow to configure graph tooltip. #2647
* [ENHANCEMENT] Alerts: MimirFrontendQueriesStuck and MimirSchedulerQueriesStuck alerts are more reliable now as they consider all the intermediate samples in the minute prior to the evaluation. #2630
* [ENHANCEMENT] Alerts: added `RolloutOperatorNotReconciling` alert, firing if the optional rollout-operator is not successfully reconciling. #2700
* [ENHANCEMENT] Dashboards: added support to query-tee in front of ruler-query-frontend in the "Remote ruler reads" dashboard. #2761
* [ENHANCEMENT] Dashboards: Introduce support for baremetal deployment, setting `deployment_type: 'baremetal'` in the mixin `_config`. #2657
* [ENHANCEMENT] Dashboards: use timeseries panel to show exemplars. #2800
* [BUGFIX] Dashboards: fixed unit of latency panels in the "Mimir / Ruler" dashboard. #2312
* [BUGFIX] Dashboards: fixed "Intervals per query" panel in the "Mimir / Queries" dashboard. #2308
* [BUGFIX] Dashboards: Make "Slow Queries" dashboard works with Grafana 9.0. #2223
* [BUGFIX] Dashboards: add missing API routes to Ruler dashboard. #2412
* [BUGFIX] Dashboards: stop setting 'interval' in dashboards; it should be set on your datasource. #2802

### Jsonnet

* [CHANGE] query-scheduler is enabled by default. We advise to deploy the query-scheduler to improve the scalability of the query-frontend. #2431
* [CHANGE] Replaced anti-affinity rules with pod topology spread constraints for distributor, query-frontend, querier and ruler. #2517
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
* [CHANGE] Change `max_global_series_per_metric` to 0 in all plans, and as a default value. #2669
* [FEATURE] Memberlist: added support for experimental memberlist cluster label, through the jsonnet configuration options `memberlist_cluster_label` and `memberlist_cluster_label_verification_disabled`. #2349
* [FEATURE] Added ruler-querier autoscaling support. It requires [KEDA](https://keda.sh) installed in the Kubernetes cluster. Ruler-querier autoscaler can be enabled and configure through the following options in the jsonnet config: #2545
  * `autoscaling_ruler_querier_enabled`: `true` to enable autoscaling.
  * `autoscaling_ruler_querier_min_replicas`: minimum number of ruler-querier replicas.
  * `autoscaling_ruler_querier_max_replicas`: maximum number of ruler-querier replicas.
  * `autoscaling_prometheus_url`: Prometheus base URL from which to scrape Mimir metrics (e.g. `http://prometheus.default:9090/prometheus`).
* [ENHANCEMENT] Memberlist now uses DNS service-discovery by default. #2549
* [ENHANCEMENT] Upgrade memcached image tag to `memcached:1.6.16-alpine`. #2740
* [ENHANCEMENT] Added `$._config.configmaps` and `$._config.runtime_config_files` to make it easy to add new configmaps or runtime config file to all components. #2748

### Mimirtool

* [ENHANCEMENT] Added `mimirtool backfill` command to upload Prometheus blocks using API available in the compactor. #1822
* [ENHANCEMENT] mimirtool bucket-validation: Verify existing objects can be overwritten by subsequent uploads. #2491
* [ENHANCEMENT] mimirtool config convert: Now supports migrating to the current version of Mimir. #2629
* [BUGFIX] mimirtool analyze: Fix dashboard JSON unmarshalling errors by using custom parsing. #2386
* [BUGFIX] Version checking no longer prompts for updating when already on latest version. #2723

### Mimir Continuous Test

* [ENHANCEMENT] Added basic authentication and bearer token support for when Mimir is behind a gateway authenticating the calls. #2717

### Query-tee

* [CHANGE] Renamed CLI flag `-server.service-port` to `-server.http-service-port`. #2683
* [CHANGE] Renamed metric `cortex_querytee_request_duration_seconds` to `cortex_querytee_backend_request_duration_seconds`. Metric `cortex_querytee_request_duration_seconds` is now reported without label `backend`. #2683
* [ENHANCEMENT] Added HTTP over gRPC support to `query-tee` to allow testing gRPC requests to Mimir instances. #2683

### Documentation

* [ENHANCEMENT] Referenced `mimirtool` commands in the HTTP API documentation. #2516
* [ENHANCEMENT] Improved DNS service discovery documentation. #2513

### Tools

* [ENHANCEMENT] `markblocks` now processes multiple blocks concurrently. #2677

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
* [ENHANCEMENT] Faster memcached cache requests. #2720
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

## [Cortex 1.10.0 CHANGELOG](https://github.com/grafana/mimir/blob/a13959db5d38ff65c2b7ef52c56331d2f4dbc00c/CHANGELOG.md#cortex-1100--2021-08-03)
