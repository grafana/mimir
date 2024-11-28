# Changelog

## main / unreleased

### Grafana Mimir

* [CHANGE] Alertmanager: the following metrics are not exported for a given `user` when the metric value is zero: #9359
  * `cortex_alertmanager_alerts_received_total`
  * `cortex_alertmanager_alerts_invalid_total`
  * `cortex_alertmanager_partial_state_merges_total`
  * `cortex_alertmanager_partial_state_merges_failed_total`
  * `cortex_alertmanager_state_replication_total`
  * `cortex_alertmanager_state_replication_failed_total`
  * `cortex_alertmanager_alerts`
  * `cortex_alertmanager_silences`
* [CHANGE] Distributor: Drop experimental `-distributor.direct-otlp-translation-enabled` flag, since direct OTLP translation is well tested at this point. #9647
* [CHANGE] Ingester: Change `-initial-delay` for circuit breakers to begin when the first request is received, rather than at breaker activation. #9842
* [CHANGE] Query-frontend: apply query pruning before query sharding instead of after. #9913
* [CHANGE] Ingester: remove experimental flags `-ingest-storage.kafka.ongoing-records-per-fetch` and `-ingest-storage.kafka.startup-records-per-fetch`. They are removed in favour of `-ingest-storage.kafka.max-buffered-bytes`. #9906
* [CHANGE] Ingester: Replace `cortex_discarded_samples_total` label from `sample-out-of-bounds` to `sample-timestamp-too-old`. #9885
* [CHANGE] Ruler: the `/prometheus/config/v1/rules` does not return an error anymore if a rule group is missing in the object storage after been successfully returned by listing the storage, because it could have been deleted in the meanwhile. #9936
* [CHANGE] Querier: The `.` pattern in regular expressions in PromQL matches newline characters. With this change regular expressions like `.*` match strings that include `\n`. To maintain the old behaviour, you will have to change regular expressions by replacing all `.` patterns with `[^\n]`, e.g. `foo[^\n]*`. This upgrades PromQL compatibility from Prometheus 2.0 to 3.0. #9844
* [CHANGE] Querier: Lookback and range selectors are left open and right closed (previously left closed and right closed). This change affects queries when the evaluation time perfectly aligns with the sample timestamps. For example assume querying a timeseries with evenly spaced samples exactly 1 minute apart. Previously, a range query with `5m` would usually return 5 samples, or 6 samples if the query evaluation aligns perfectly with a scrape. Now, queries like this will always return 5 samples. This upgrades PromQL compatibility from Prometheus 2.0 to 3.0. #9844
* [CHANGE] Querier: promql(native histograms): Introduce exponential interpolation. #9844
* [FEATURE] Querier: add experimental streaming PromQL engine, enabled with `-querier.query-engine=mimir`. #9367 #9368 #9398 #9399 #9403 #9417 #9418 #9419 #9420 #9482 #9504 #9505 #9507 #9518 #9531 #9532 #9533 #9553 #9558 #9588 #9589 #9639 #9641 #9642 #9651 #9664 #9681 #9717 #9719 #9724 #9874 #9998 #10007 #10010
* [FEATURE] Distributor: Add support for `lz4` OTLP compression. #9763
* [FEATURE] Query-frontend: added experimental configuration options `query-frontend.cache-errors` and `query-frontend.results-cache-ttl-for-errors` to allow non-transient responses to be cached. When set to `true` error responses from hitting limits or bad data are cached for a short TTL. #9028
* [FEATURE] Query-frontend: add middleware to control access to specific PromQL experimental functions on a per-tenant basis. #9798
* [FEATURE] gRPC: Support S2 compression. #9322
  * `-alertmanager.alertmanager-client.grpc-compression=s2`
  * `-ingester.client.grpc-compression=s2`
  * `-querier.frontend-client.grpc-compression=s2`
  * `-querier.scheduler-client.grpc-compression=s2`
  * `-query-frontend.grpc-client-config.grpc-compression=s2`
  * `-query-scheduler.grpc-client-config.grpc-compression=s2`
  * `-ruler.client.grpc-compression=s2`
  * `-ruler.query-frontend.grpc-client-config.grpc-compression=s2`
* [FEATURE] Alertmanager: limit added for maximum size of the Grafana state (`-alertmanager.max-grafana-state-size-bytes`). #9475
* [FEATURE] Alertmanager: limit added for maximum size of the Grafana configuration (`-alertmanager.max-config-size-bytes`). #9402
* [FEATURE] Ingester: Experimental support for ingesting out-of-order native histograms. This is disabled by default and can be enabled by setting `-ingester.ooo-native-histograms-ingestion-enabled` to `true`. #7175
* [FEATURE] Distributor: Added `-api.skip-label-count-validation-header-enabled` option to allow skipping label count validation on the HTTP write path based on `X-Mimir-SkipLabelCountValidation` header being `true` or not. #9576
* [FEATURE] Ruler: Add experimental support for caching the contents of rule groups. This is disabled by default and can be enabled by setting `-ruler-storage.cache.rule-group-enabled`. #9595 #10024
* [FEATURE] PromQL: Add experimental `info` function. Experimental functions are disabled by default, but can be enabled setting `-querier.promql-experimental-functions-enabled=true` in the query-frontend and querier. #9879
* [FEATURE] Distributor: Support promotion of OTel resource attributes to labels. #8271
* [FEATURE] Querier: Add experimental `double_exponential_smoothing` PromQL function. Experimental functions are disabled by default, but can be enabled by setting `-querier.promql-experimental-functions-enabled=true` in the query-frontend and querier. #9844
* [ENHANCEMENT] Query Frontend: Return server-side `bytes_processed` statistics following Server-Timing format. #9645 #9985
* [ENHANCEMENT] mimirtool: Adds bearer token support for mimirtool's analyze ruler/prometheus commands. #9587
* [ENHANCEMENT] Ruler: Support `exclude_alerts` parameter in `<prometheus-http-prefix>/api/v1/rules` endpoint. #9300
* [ENHANCEMENT] Distributor: add a metric to track tenants who are sending newlines in their label values called `cortex_distributor_label_values_with_newlines_total`. #9400
* [ENHANCEMENT] Ingester: improve performance of reading the WAL. #9508
* [ENHANCEMENT] Query-scheduler: improve the errors and traces emitted by query-schedulers when communicating with queriers. #9519
* [ENHANCEMENT] Compactor: uploaded blocks cannot be bigger than max configured compactor time range, and cannot cross the boundary for given time range. #9524
* [ENHANCEMENT] The distributor now validates that received label values only contain allowed characters. #9185
* [ENHANCEMENT] Add SASL plain authentication support to Kafka client used by the experimental ingest storage. Configure SASL credentials via the following settings: #9584
  * `-ingest-storage.kafka.sasl-password`
  * `-ingest-storage.kafka.sasl-username`
* [ENHANCEMENT] memberlist: TCP transport write path is now non-blocking, and is configurable by new flags: #9594
  * `-memberlist.max-concurrent-writes`
  * `-memberlist.acquire-writer-timeout`
* [ENHANCEMENT] memberlist: Notifications can now be processed once per interval specified by `-memberlist.notify-interval` to reduce notify storm CPU activity in large clusters. #9594
* [ENHANCEMENT] Query-scheduler: Remove the experimental `query-scheduler.prioritize-query-components` flag. Request queues always prioritize query component dequeuing above tenant fairness. #9703
* [ENHANCEMENT] Ingester: Emit traces for block syncing, to join up block-upload traces. #9656
* [ENHANCEMENT] Querier: Enable the optional querying of additional storage queryables. #9712
* [ENHANCEMENT] Ingester: Disable the push circuit breaker when ingester is in read-only mode. #9760
* [ENHANCEMENT] Ingester: Reduced lock contention in the `PostingsForMatchers` cache. #9773
* [ENHANCEMENT] Storage: Allow HTTP client settings to be tuned for GCS and Azure backends via an `http` block or corresponding CLI flags. This was already supported by the S3 backend. #9778
* [ENHANCEMENT] Ruler: Support `group_limit` and `group_next_token` parameters in the `<prometheus-http-prefix>/api/v1/rules` endpoint. #9563
* [ENHANCEMENT] Ingester: improved lock contention affecting read and write latencies during TSDB head compaction. #9822
* [ENHANCEMENT] Distributor: when a label value fails validation due to invalid UTF-8 characters, don't include the invalid characters in the returned error. #9828
* [ENHANCEMENT] Ingester: when experimental ingest storage is enabled, do not buffer records in the Kafka client when fetch concurrency is in use. #9838 #9850
* [ENHANCEMENT] Compactor: refresh deletion marks when updating the bucket index concurrently. This speeds up updating the bucket index by up to 16 times when there is a lot of blocks churn (thousands of blocks churning every cleanup cycle). #9881
* [ENHANCEMENT] PromQL: make `sort_by_label` stable. #9879
* [ENHANCEMENT] Distributor: Initialize ha_tracker cache before ha_tracker and distributor reach running state and begin serving writes. #9826 #9976
* [ENHANCEMENT] Ingester: `-ingest-storage.kafka.max-buffered-bytes` to limit the memory for buffered records when using concurrent fetching. #9892
* [ENHANCEMENT] Querier: improve performance and memory consumption of queries that select many series. #9914
* [ENHANCEMENT] Ruler: Support OAuth2 and proxies in Alertmanager client #9945
* [ENHANCEMENT] Ingester: Build 24h blocks for older OOO. #9844, #10033
* [ENHANCEMENT] Distributor: allow a different limit for info series (series ending in `_info`) label count, via `-validation.max-label-names-per-info-series`. #10028
* [BUGFIX] Fix issue where functions such as `rate()` over native histograms could return incorrect values if a float stale marker was present in the selected range. #9508
* [BUGFIX] Fix issue where negation of native histograms (eg. `-some_native_histogram_series`) did nothing. #9508
* [BUGFIX] Fix issue where `metric might not be a counter, name does not end in _total/_sum/_count/_bucket` annotation would be emitted even if `rate` or `increase` did not have enough samples to compute a result. #9508
* [BUGFIX] Fix issue where sharded queries could return annotations with incorrect or confusing position information. #9536
* [BUGFIX] Fix issue where downstream consumers may not generate correct cache keys for experimental error caching. #9644
* [BUGFIX] Fix issue where active series requests error when encountering a stale posting. #9580
* [BUGFIX] Fix pooling buffer reuse logic when `-distributor.max-request-pool-buffer-size` is set. #9666
* [BUGFIX] Fix issue when using the experimental `-ruler.max-independent-rule-evaluation-concurrency` feature, where the ruler could panic as it updates a running ruleset or shutdowns. #9726
* [BUGFIX] Always return unknown hint for first sample in non-gauge native histograms chunk to avoid incorrect counter reset hints when merging chunks from different sources. #10033
* [BUGFIX] Ensure native histograms counter reset hints are corrected when merging results from different sources. #9909
* [BUGFIX] Ingester: Fix race condition in per-tenant TSDB creation. #9708
* [BUGFIX] Ingester: Fix race condition in exemplar adding. #9765
* [BUGFIX] Ingester: Fix race condition in native histogram appending. #9765
* [BUGFIX] Ingester: Fix bug in concurrent fetching where a failure to list topics on startup would cause to use an invalid topic ID (0x00000000000000000000000000000000). #9883
* [BUGFIX] Ingester: Fix data loss bug in the experimental ingest storage when a Kafka Fetch is split into multiple requests and some of them return an error. #9963 #9964
* [BUGFIX] PromQL: `round` now removes the metric name again. #9879
* [BUGFIX] Query-Frontend: fix `QueryFrontendCodec` module initialization to set lookback delta from `-querier.lookback-delta`. #9984
* [BUGFIX] OTLP: Support integer exemplar value type. #9844
* [BUGFIX] Querier: Correct the behaviour of binary operators between native histograms and floats. #9844
* [BUGFIX] Querier: Fix stddev+stdvar aggregations to always ignore native histograms. #9844
* [BUGFIX] Querier: Fix stddev+stdvar aggregations to treat Infinity consistently. #9844
* [BUGFIX] Ingester: Chunks could have one unnecessary zero byte at the end. #9844

### Mixin

* [CHANGE] Remove backwards compatibility for `thanos_memcached_` prefixed metrics in dashboards and alerts removed in 2.12. #9674 #9758
* [CHANGE] Reworked the alert `MimirIngesterStuckProcessingRecordsFromKafka` to also work when concurrent fetching is enabled. #9855
* [ENHANCEMENT] Unify ingester autoscaling panels on 'Mimir / Writes' dashboard to work for both ingest-storage and non-ingest-storage autoscaling. #9617
* [ENHANCEMENT] Alerts: Enable configuring job prefix for alerts to prevent clashes with metrics from Loki/Tempo. #9659
* [ENHANCEMENT] Dashboards: visualize the age of source blocks in the "Mimir / Compactor" dashboard. #9697
* [ENHANCEMENT] Dashboards: Include block compaction level on queried blocks in 'Mimir / Queries' dashboard. #9706
* [ENHANCEMENT] Alerts: add `MimirIngesterMissedRecordsFromKafka` to detect gaps in consumed records in the ingester when using the experimental Kafka-based storage. #9921 #9972
* [ENHANCEMENT] Dashboards: Add more panels to 'Mimir / Writes' for concurrent ingestion and fetching when using ingest storage. #10021
* [BUGFIX] Dashboards: Fix autoscaling metrics joins when series churn. #9412 #9450 #9432
* [BUGFIX] Alerts: Fix autoscaling metrics joins in `MimirAutoscalerNotActive` when series churn. #9412
* [BUGFIX] Alerts: Exclude failed cache "add" operations from alerting since failures are expected in normal operation. #9658
* [BUGFIX] Alerts: Exclude read-only replicas from `IngesterInstanceHasNoTenants` alert. #9843
* [BUGFIX] Alerts: Use resident set memory for the `EtcdAllocatingTooMuchMemory` alert so that ephemeral file cache memory doesn't cause the alert to misfire. #9997

### Jsonnet

* [CHANGE] Remove support to set Redis as a cache backend from jsonnet. #9677
* [CHANGE] Rollout-operator now defaults to storing scaling operation metadata in a Kubernetes ConfigMap. This avoids recursively invoking the admission webhook in some Kubernetes environments. #9699
* [CHANGE] Update rollout-operator version to 0.20.0. #9995
* [CHANGE] Remove the `track_sizes` feature for Memcached pods since it is unused. #10032
* [FEATURE] Add support to deploy distributors in multi availability zones. #9548
* [FEATURE] Add configuration settings to set the number of Memcached replicas for each type of cache (`memcached_frontend_replicas`, `memcached_index_queries_replicas`, `memcached_chunks_replicas`, `memcached_metadata_replicas`). #9679
* [ENHANCEMENT] Add `ingest_storage_ingester_autoscaling_triggers` option to specify multiple triggers in ScaledObject created for ingest-store ingester autoscaling. #9422
* [ENHANCEMENT] Add `ingest_storage_ingester_autoscaling_scale_up_stabilization_window_seconds` and `ingest_storage_ingester_autoscaling_scale_down_stabilization_window_seconds` config options to make stabilization window for ingester autoscaling when using ingest-storage configurable. #9445
* [ENHANCEMENT] Make label-selector in ReplicaTemplate/ingester-zone-a object configurable when using ingest-storage. #9480
* [ENHANCEMENT] Add `querier_only_args` option to specify CLI flags that apply only to queriers but not ruler-queriers. #9503
* [ENHANCEMENT] Validate the Kafka client ID configured when ingest storage is enabled. #9573
* [ENHANCEMENT] Configure pod anti-affinity and tolerations to run etcd pods multi-AZ when `_config.multi_zone_etcd_enabled` is set to `true`. #9725

### Mimirtool

### Mimir Continuous Test

### Query-tee

* [FEATURE] Added `-proxy.compare-skip-samples-before` to skip samples before the given time when comparing responses. The time can be in RFC3339 format (or) RFC3339 without the timezone and seconds (or) date only. #9515
* [ENHANCEMENT] Added human-readable timestamps to comparison failure messages. #9665

### Documentation

### Tools

* [FEATURE] `splitblocks`: add new tool to split blocks larger than a specified duration into multiple blocks. #9517, #9779
* [ENHANCEMENT] `copyblocks`: Added `--skip-no-compact-block-duration-check`, which defaults to `false`, to simplify targeting blocks that are not awaiting compaction. #9439
* [ENHANCEMENT] `kafkatool`: add SASL plain authentication support. The following new CLI flags have been added: #9584
  * `--kafka-sasl-username`
  * `--kafka-sasl-password`
* [ENHANCEMENT] `kafkatool`: add `dump print` command to print the content of write requests from a dump. #9942
* [ENHANCEMENT] Updated `KubePersistentVolumeFillingUp` runbook, including a sample command to debug the distroless image. #9802

## 2.14.1

### Grafana Mimir

* [BUGFIX] Update objstore library to resolve issues observed for some S3-compatible object stores, which respond to `StatObject` with `Range` incorrectly. #9625

## 2.14.0

### Grafana Mimir

* [CHANGE] Update minimal supported version of Go to 1.22. #9134
* [CHANGE] Store-gateway / querier: enable streaming chunks from store-gateways to queriers by default. #6646
* [CHANGE] Querier: honor the start/end time range specified in the read hints when executing a remote read request. #8431
* [CHANGE] Querier: return only samples within the queried start/end time range when executing a remote read request using "SAMPLES" mode. Previously, samples outside of the range could have been returned. Samples outside of the queried time range may still be returned when executing a remote read request using "STREAMED_XOR_CHUNKS" mode. #8463
* [CHANGE] Querier: Set minimum for `-querier.max-concurrent` to four to prevent queue starvation with querier-worker queue prioritization algorithm; values below the minimum four are ignored and set to the minimum. #9054
* [CHANGE] Store-gateway: enabled `-blocks-storage.bucket-store.max-concurrent-queue-timeout` by default with a timeout of 5 seconds. #8496
* [CHANGE] Store-gateway: enabled `-blocks-storage.bucket-store.index-header.lazy-loading-concurrency-queue-timeout` by default with a timeout of 5 seconds . #8667
* [CHANGE] Distributor: Incoming OTLP requests were previously size-limited by using limit from `-distributor.max-recv-msg-size` option. We have added option `-distributor.max-otlp-request-size` for limiting OTLP requests, with default value of 100 MiB. #8574
* [CHANGE] Distributor: remove metric `cortex_distributor_sample_delay_seconds`. #8698
* [CHANGE] Query-frontend: Remove deprecated `frontend.align_queries_with_step` YAML configuration. The configuration option has been moved to per-tenant and default `limits` since Mimir 2.12. #8733 #8735
* [CHANGE] Store-gateway: Change default of `-blocks-storage.bucket-store.max-concurrent` to 200. #8768
* [CHANGE] Added new metric `cortex_compactor_disk_out_of_space_errors_total` which counts how many times a compaction failed due to the compactor being out of disk, alert if there is a single increase. #8237 #8278
* [CHANGE] Store-gateway: Remove experimental parameter `-blocks-storage.bucket-store.series-selection-strategy`. The default strategy is now `worst-case`. #8702
* [CHANGE] Store-gateway: Rename `-blocks-storage.bucket-store.series-selection-strategies.worst-case-series-preference` to `-blocks-storage.bucket-store.series-fetch-preference` and promote to stable. #8702
* [CHANGE] Querier, store-gateway: remove deprecated `-querier.prefer-streaming-chunks-from-store-gateways=true`. Streaming from store-gateways is now always enabled. #8696
* [CHANGE] Ingester: remove deprecated `-ingester.return-only-grpc-errors`. #8699 #8828
* [CHANGE] Distributor, ruler: remove deprecated `-ingester.client.report-grpc-codes-in-instrumentation-label-enabled`. #8700
* [CHANGE] Ingester client: experimental support for client-side circuit breakers, their configuration options (`-ingester.client.circuit-breaker.*`) and metrics (`cortex_ingester_client_circuit_breaker_results_total`, `cortex_ingester_client_circuit_breaker_transitions_total`) were removed. #8802
* [CHANGE] Ingester: circuit breakers do not open in case of per-instance limit errors anymore. Opening can be triggered only in case of push and pull requests exceeding the configured duration. #8854
* [CHANGE] Query-frontend: Return `413 Request Entity Too Large` if a response shard for an `/active_series` request is too large. #8861
* [CHANGE] Distributor: Promote replying with `Retry-After` header on retryable errors to stable and set `-distributor.retry-after-header.enabled=true` by default. #8694
* [CHANGE] Distributor: Replace `-distributor.retry-after-header.max-backoff-exponent` and `-distributor.retry-after-header.base-seconds` with `-distributor.retry-after-header.min-backoff` and `-distributor.retry-after-header.max-backoff` for easier configuration. #8694
* [CHANGE] Ingester: increase the default inactivity timeout of active series (`-ingester.active-series-metrics-idle-timeout`) from `10m` to `20m`. #8975
* [CHANGE] Distributor: Remove `-distributor.enable-otlp-metadata-storage` flag, which was deprecated in version 2.12. #9069
* [CHANGE] Ruler: Removed `-ruler.drain-notification-queue-on-shutdown` option, which is now enabled by default. #9115
* [CHANGE] Querier: allow wrapping errors with context errors only when the former actually correspond to `context.Canceled` and `context.DeadlineExceeded`. #9175
* [CHANGE] Query-scheduler: Remove the experimental `-query-scheduler.use-multi-algorithm-query-queue` flag. The new multi-algorithm tree queue is always used for the scheduler. #9210
* [CHANGE] Distributor: reject incoming requests until the distributor service has started. #9317
* [CHANGE] Ingester, Distributor: Remove deprecated `-ingester.limit-inflight-requests-using-grpc-method-limiter` and `-distributor.limit-inflight-requests-using-grpc-method-limiter`. The feature was deprecated and enabled by default in Mimir 2.12. #9407
* [CHANGE] Querier: Remove deprecated `-querier.max-query-into-future`. The feature was deprecated in Mimir 2.12. #9407
* [CHANGE] Cache: Deprecate experimental support for Redis as a cache backend. The support is set to be removed in the next major release. #9453
* [FEATURE] Alertmanager: Added `-alertmanager.log-parsing-label-matchers` to control logging when parsing label matchers. This flag is intended to be used with `-alertmanager.utf8-strict-mode-enabled` to validate UTF-8 strict mode is working as intended. The default value is `false`. #9173
* [FEATURE] Alertmanager: Added `-alertmanager.utf8-migration-logging-enabled` to enable logging of tenant configurations that are incompatible with UTF-8 strict mode. The default value is `false`. #9174
* [FEATURE] Querier: add experimental streaming PromQL engine, enabled with `-querier.query-engine=mimir`. #8422 #8430 #8454 #8455 #8360 #8490 #8508 #8577 #8660 #8671 #8677 #8747 #8850 #8872 #8838 #8911 #8909 #8923 #8924 #8925 #8932 #8933 #8934 #8962 #8986 #8993 #8995 #9008 #9017 #9018 #9019 #9120 #9121 #9136 #9139 #9140 #9145 #9191 #9192 #9194 #9196 #9201 #9212 #9225 #9260 #9272 #9277 #9278 #9280 #9281 #9342 #9343 #9371 #9859 #9858
* [FEATURE] Experimental Kafka-based ingest storage. #6888 #6894 #6929 #6940 #6951 #6974 #6982 #7029 #7030 #7091 #7142 #7147 #7148 #7153 #7160 #7193 #7349 #7376 #7388 #7391 #7393 #7394 #7402 #7404 #7423 #7424 #7437 #7486 #7503 #7508 #7540 #7621 #7682 #7685 #7694 #7695 #7696 #7697 #7701 #7733 #7734 #7741 #7752 #7838 #7851 #7871 #7877 #7880 #7882 #7887 #7891 #7925 #7955 #7967 #8031 #8063 #8077 #8088 #8135 #8176 #8184 #8194 #8216 #8217 #8222 #8233 #8503 #8542 #8579 #8657 #8686 #8688 #8703 #8706 #8708 #8738 #8750 #8778 #8808 #8809 #8841 #8842 #8845 #8853 #8886 #8988
  * What it is:
    * When the new ingest storage architecture is enabled, distributors write incoming write requests to a Kafka-compatible backend, and the ingesters asynchronously replay ingested data from Kafka. In this architecture, the write and read path are de-coupled through a Kafka-compatible backend. The write path and Kafka load is a function of the incoming write traffic, the read path load is a function of received queries. Whatever the load on the read path, it doesn't affect the write path.
  * New configuration options:
    * `-ingest-storage.enabled`
    * `-ingest-storage.kafka.*`: configures Kafka-compatible backend and how clients interact with it.
    * `-ingest-storage.ingestion-partition-tenant-shard-size`: configures the per-tenant shuffle-sharding shard size used by partitions ring.
    * `-ingest-storage.read-consistency`: configures the default read consistency.
    * `-ingest-storage.migration.distributor-send-to-ingesters-enabled`: enabled tee-ing writes to classic ingesters and Kafka, used during a live migration to the new ingest storage architecture.
    * `-ingester.partition-ring.*`: configures partitions ring backend.
* [FEATURE] Querier: added support for `limitk()` and `limit_ratio()` experimental PromQL functions. Experimental functions are disabled by default, but can be enabled setting `-querier.promql-experimental-functions-enabled=true` in the query-frontend and querier. #8632
* [FEATURE] Querier: experimental support for `X-Mimir-Chunk-Info-Logger` header that triggers logging information about TSDB chunks loaded from ingesters and store-gateways in the querier. The header should contain the comma separated list of labels for which their value will be included in the logs. #8599
* [FEATURE] Query frontend: added new query pruning middleware to enable pruning dead code (eg. expressions that cannot produce any results) and simplifying expressions (eg. expressions that can be evaluated immediately) in queries. #9086
* [FEATURE] Ruler: added experimental configuration, `-ruler.rule-evaluation-write-enabled`, to disable writing the result of rule evaluation to ingesters. This feature can be used for testing purposes. #9060
* [FEATURE] Ingester: added experimental configuration `ingester.ignore-ooo-exemplars`. When set to `true` out of order exemplars are no longer reported to the remote write client. #9151
* [ENHANCEMENT] Compactor: Add `cortex_compactor_compaction_job_duration_seconds` and `cortex_compactor_compaction_job_blocks` histogram metrics to track duration of individual compaction jobs and number of blocks per job. #8371
* [ENHANCEMENT] Rules: Added per namespace max rules per rule group limit. The maximum number of rules per rule groups for all namespaces continues to be configured by `-ruler.max-rules-per-rule-group`, but now, this can be superseded by the new `-ruler.max-rules-per-rule-group-by-namespace` option on a per namespace basis. This new limit can be overridden using the overrides mechanism to be applied per-tenant. #8378
* [ENHANCEMENT] Rules: Added per namespace max rule groups per tenant limit. The maximum number of rule groups per rule tenant for all namespaces continues to be configured by `-ruler.max-rule-groups-per-tenant`, but now, this can be superseded by the new `-ruler.max-rule-groups-per-tenant-by-namespace` option on a per namespace basis. This new limit can be overridden using the overrides mechanism to be applied per-tenant. #8425
* [ENHANCEMENT] Ruler: Added support to protect rules namespaces from modification. The `-ruler.protected-namespaces` flag can be used to specify namespaces that are protected from rule modifications. The header `X-Mimir-Ruler-Override-Namespace-Protection` can be used to override the protection. #8444
* [ENHANCEMENT] Query-frontend: be able to block remote read queries via the per tenant runtime override `blocked_queries`. #8372 #8415
* [ENHANCEMENT] Query-frontend: added `remote_read` to `op` supported label values for the `cortex_query_frontend_queries_total` metric. #8412
* [ENHANCEMENT] Query-frontend: log the overall length and start, end time offset from current time for remote read requests. The start and end times are calculated as the miminum and maximum times of the individual queries in the remote read request. #8404
* [ENHANCEMENT] Storage Provider: Added option `-<prefix>.s3.dualstack-enabled` that allows disabling S3 client from resolving AWS S3 endpoint into dual-stack IPv4/IPv6 endpoint. Defaults to true. #8405
* [ENHANCEMENT] HA Tracker: Added reporting of most recent elected replica change via `cortex_ha_tracker_last_election_timestamp_seconds` gauge, logging, and a new column in the HA Tracker status page. #8507
* [ENHANCEMENT] Use sd_notify to send events to systemd at start and stop of mimir services. Default systemd mimir.service config now wait for those events with a configurable timeout `TimeoutStartSec` default is 3 min to handle long start time (ex. store-gateway). #8220 #8555 #8658
* [ENHANCEMENT] Alertmanager: Reloading config and templates no longer needs to hit the disk. #4967
* [ENHANCEMENT] Compactor: Added experimental `-compactor.in-memory-tenant-meta-cache-size` option to set size of in-memory cache (in number of items) for parsed meta.json files. This can help when a tenant has many meta.json files and their parsing before each compaction cycle is using a lot of CPU time. #8544
* [ENHANCEMENT] Distributor: Interrupt OTLP write request translation when context is canceled or has timed out. #8524
* [ENHANCEMENT] Ingester, store-gateway: optimised regular expression matching for patterns like `1.*|2.*|3.*|...|1000.*`. #8632
* [ENHANCEMENT] Query-frontend: Add `header_cache_control` to query stats. #8590
* [ENHANCEMENT] Query-scheduler: Introduce `query-scheduler.use-multi-algorithm-query-queue`, which allows use of an experimental queue structure, with no change in external queue behavior. #7873
* [ENHANCEMENT] Query-scheduler: Improve CPU/memory performance of experimental query-scheduler. #8871
* [ENHANCEMENT] Expose a new `s3.trace.enabled` configuration option to enable detailed logging of operations against S3-compatible object stores. #8690
* [ENHANCEMENT] memberlist: locally-generated messages (e.g. ring updates) are sent to gossip network before forwarded messages. Introduced `-memberlist.broadcast-timeout-for-local-updates-on-shutdown` option to modify how long to wait until queue with locally-generated messages is empty when shutting down. Previously this was hard-coded to 10s, and wait included all messages (locally-generated and forwarded). Now it defaults to 10s, 0 means no timeout. Increasing this value may help to avoid problem when ring updates on shutdown are not propagated to other nodes, and ring entry is left in a wrong state. #8761
* [ENHANCEMENT] Querier: allow using both raw numbers of seconds and duration literals in queries where previously only one or the other was permitted. For example, `predict_linear` now accepts a duration literal (eg. `predict_linear(..., 4h)`), and range vector selectors now accept a number of seconds (eg. `rate(metric[2])`). #8780
* [ENHANCEMENT] Ruler: Add `ruler.max-independent-rule-evaluation-concurrency` to allow independent rules of a tenant to be run concurrently. You can control the amount of concurrency per tenant is controlled via the `-ruler.max-independent-rule-evaluation-concurrency-per-tenan` as a limit. Use a `-ruler.max-independent-rule-evaluation-concurrency` value of `0` can be used to disable the feature for all tenants. By default, this feature is disabled. A rule is eligible for concurrency as long as it doesn't depend on any other rules, doesn't have any other rules that depend on it, and has a total rule group runtime that exceeds 50% of its interval by default. The threshold can can be adjusted with `-ruler.independent-rule-evaluation-concurrency-min-duration-percentage`. #8146 #8858 #8880 #8884
  * This work introduces the following metrics:
    * `cortex_ruler_independent_rule_evaluation_concurrency_slots_in_use`
    * `cortex_ruler_independent_rule_evaluation_concurrency_attempts_started_total`
    * `cortex_ruler_independent_rule_evaluation_concurrency_attempts_incomplete_total`
    * `cortex_ruler_independent_rule_evaluation_concurrency_attempts_completed_total`
* [ENHANCEMENT] Expose a new `s3.session-token` configuration option to enable using temporary security credentials. #8952
* [ENHANCEMENT] Add HA deduplication features to the `mimir-microservices-mode` development environment. #9012
* [ENHANCEMENT] Remove experimental `-query-frontend.additional-query-queue-dimensions-enabled` and `-query-scheduler.additional-query-queue-dimensions-enabled`. Mimir now always includes "query components" as a queue dimension. #8984 #9135
* [ENHANCEMENT] Add a new ingester endpoint to prepare instances to downscale. #8956
* [ENHANCEMENT] Query-scheduler: Add `query-scheduler.prioritize-query-components` which, when enabled, will primarily prioritize dequeuing fairly across queue components, and secondarily prioritize dequeuing fairly across tenants. When disabled, tenant fairness is primarily prioritized. `query-scheduler.use-multi-algorithm-query-queue` must be enabled in order to use this flag. #9016 #9071
* [ENHANCEMENT] Update runtime configuration to read gzip-compressed files with `.gz` extension. #9074
* [ENHANCEMENT] Ingester: add `cortex_lifecycler_read_only` metric which is set to 1 when ingester's lifecycler is set to read-only mode. #9095
* [ENHANCEMENT] Add a new field, `encode_time_seconds` to query stats log messages, to record the amount of time it takes the query-frontend to encode a response. This does not include any serialization time for downstream components. #9062
* [ENHANCEMENT] OTLP: If the flag `-distributor.otel-created-timestamp-zero-ingestion-enabled` is true, OTel start timestamps are converted to Prometheus zero samples to mark series start. #9131
* [ENHANCEMENT] Querier: attach logs emitted during query consistency check to trace span for query. #9213
* [ENHANCEMENT] Query-scheduler: Experimental `-query-scheduler.prioritize-query-components` flag enables the querier-worker queue priority algorithm to take precedence over tenant rotation when dequeuing requests. #9220
* [ENHANCEMENT] Add application credential arguments for Openstack Swift storage backend. #9181
* [ENHANCEMENT] Make MemberlistKV module targetable (can be run through `-target=memberlist-kv`). #9940
* [BUGFIX] Ruler: add support for draining any outstanding alert notifications before shutting down. This can be enabled with the `-ruler.drain-notification-queue-on-shutdown=true` CLI flag. #8346
* [BUGFIX] Query-frontend: fix `-querier.max-query-lookback` enforcement when `-compactor.blocks-retention-period` is not set, and viceversa. #8388
* [BUGFIX] Ingester: fix sporadic `not found` error causing an internal server error if label names are queried with matchers during head compaction. #8391
* [BUGFIX] Ingester, store-gateway: fix case insensitive regular expressions not matching correctly some Unicode characters. #8391
* [BUGFIX] Query-frontend: "query stats" log now includes the actual `status_code` when the request fails due to an error occurring in the query-frontend itself. #8407
* [BUGFIX] Store-gateway: fixed a case where, on a quick subsequent restart, the previous lazy-loaded index header snapshot was overwritten by a partially loaded one. #8281
* [BUGFIX] Ingester: fixed timestamp reported in the "the sample has been rejected because its timestamp is too old" error when the write request contains only histograms. #8462
* [BUGFIX] Store-gateway: store sparse index headers atomically to disk. #8485
* [BUGFIX] Query scheduler: fix a panic in request queueing. #8451
* [BUGFIX] Querier: fix issue where "context canceled" is logged for trace spans for requests to store-gateways that return no series when chunks streaming is enabled. #8510
* [BUGFIX] Alertmanager: Fix per-tenant silence limits not reloaded during runtime. #8456
* [BUGFIX] Alertmanager: Fixes a number of bugs in silences which could cause an existing silence to be deleted/expired when updating the silence failed. This could happen when the replacing silence was invalid or exceeded limits. #8525
* [BUGFIX] Alertmanager: Fix help message for utf-8-strict-mode. #8572
* [BUGFIX] Query-frontend: Ensure that internal errors result in an HTTP 500 response code instead of 422. #8595 #8666
* [BUGFIX] Configuration: Multi line envs variables are flatten during injection to be compatible with YAML syntax
* [BUGFIX] Querier: fix issue where queries can return incorrect results if a single store-gateway returns overlapping chunks for a series. #8827
* [BUGFIX] HA Tracker: store correct timestamp for last received request from elected replica. #8821
* [BUGFIX] Querier: do not return `grpc: the client connection is closing` errors as HTTP `499`. #8865 #8888
* [BUGFIX] Compactor: fix a race condition between different compactor replicas that may cause a deleted block to be still referenced as non-deleted in the bucket index. #8905
* [BUGFIX] Querier: fix issue where some native histogram-related warnings were not emitted when `rate()` was used over native histograms. #8918
* [BUGFIX] Ruler: map invalid org-id errors to 400 status code. #8935
* [BUGFIX] Querier: Fix invalid query results when multiple chunks are being merged. #8992
* [BUGFIX] Query-frontend: return annotations generated during evaluation of sharded queries. #9138
* [BUGFIX] Querier: Support optional start and end times on `/prometheus/api/v1/labels`, `/prometheus/api/v1/label/<label>/values`, and `/prometheus/api/v1/series` when `max_query_into_future: 0`. #9129
* [BUGFIX] Alertmanager: Fix config validation gap around unreferenced templates. #9207
* [BUGFIX] Alertmanager: Fix goroutine leak when stored config fails to apply and there is no existing tenant alertmanager #9211
* [BUGFIX] Querier: fix issue where both recently compacted blocks and their source blocks can be skipped during querying if store-gateways are restarting. #9224
* [BUGFIX] Alertmanager: fix receiver firewall to detect `0.0.0.0` and IPv6 interface-local multicast address as local addresses. #9308

### Mixin

* [CHANGE] Dashboards: set default auto-refresh rate to 5m. #8758
* [ENHANCEMENT] Dashboards: allow switching between using classic or native histograms in dashboards.
  * Overview dashboard: status, read/write latency and queries/ingestion per sec panels, `cortex_request_duration_seconds` metric. #7674 #8502 #8791
  * Writes dashboard: `cortex_request_duration_seconds` metric. #8757 #8791
  * Reads dashboard: `cortex_request_duration_seconds` metric. #8752
  * Rollout progress dashboard: `cortex_request_duration_seconds` metric. #8779
  * Alertmanager dashboard: `cortex_request_duration_seconds` metric. #8792
  * Ruler dashboard: `cortex_request_duration_seconds` metric. #8795
  * Queries dashboard: `cortex_request_duration_seconds` metric. #8800
  * Remote ruler reads dashboard: `cortex_request_duration_seconds` metric. #8801
* [ENHANCEMENT] Alerts: `MimirRunningIngesterReceiveDelayTooHigh` alert has been tuned to be more reactive to high receive delay. #8538
* [ENHANCEMENT] Dashboards: improve end-to-end latency and strong read consistency panels when experimental ingest storage is enabled. #8543 #8830
* [ENHANCEMENT] Dashboards: Add panels for monitoring ingester autoscaling when not using ingest-storage. These panels are disabled by default, but can be enabled using the `autoscaling.ingester.enabled: true` config option. #8484
* [ENHANCEMENT] Dashboards: Add panels for monitoring store-gateway autoscaling. These panels are disabled by default, but can be enabled using the `autoscaling.store_gateway.enabled: true` config option. #8824
* [ENHANCEMENT] Dashboards: add panels to show writes to experimental ingest storage backend in the "Mimir / Ruler" dashboard, when `_config.show_ingest_storage_panels` is enabled. #8732
* [ENHANCEMENT] Dashboards: show all series in tooltips on time series dashboard panels. #8748
* [ENHANCEMENT] Dashboards: add compactor autoscaling panels to "Mimir / Compactor" dashboard. The panels are disabled by default, but can be enabled setting `_config.autoscaling.compactor.enabled` to `true`. #8777
* [ENHANCEMENT] Alerts: added `MimirKafkaClientBufferedProduceBytesTooHigh` alert. #8763
* [ENHANCEMENT] Dashboards: added "Kafka produced records / sec" panel to "Mimir / Writes" dashboard. #8763
* [ENHANCEMENT] Alerts: added `MimirStrongConsistencyOffsetNotPropagatedToIngesters` alert, and rename `MimirIngesterFailsEnforceStrongConsistencyOnReadPath` alert to `MimirStrongConsistencyEnforcementFailed`. #8831
* [ENHANCEMENT] Dashboards: remove "All" option for namespace dropdown in dashboards. #8829
* [ENHANCEMENT] Dashboards: add Kafka end-to-end latency outliers panel in the "Mimir / Writes" dashboard. #8948
* [ENHANCEMENT] Dashboards: add "Out-of-order samples appended" panel to "Mimir / Tenants" dashboard. #8939
* [ENHANCEMENT] Alerts: `RequestErrors` and `RulerRemoteEvaluationFailing` have been enriched with a native histogram version. #9004
* [ENHANCEMENT] Dashboards: add 'Read path' selector to 'Mimir / Queries' dashboard. #8878
* [ENHANCEMENT] Dashboards: add annotation indicating active series are being reloaded to 'Mimir / Tenants' dashboard. #9257
* [ENHANCEMENT] Dashboards: limit results on the 'Failed evaluations rate' panel of the 'Mimir / Tenants' dashboard to 50 to avoid crashing the page when there are many failing groups. #9262
* [FEATURE] Alerts: add `MimirGossipMembersEndpointsOutOfSync` alert. #9347
* [BUGFIX] Dashboards: fix "current replicas" in autoscaling panels when HPA is not active. #8566
* [BUGFIX] Alerts: do not fire `MimirRingMembersMismatch` during the migration to experimental ingest storage. #8727
* [BUGFIX] Dashboards: avoid over-counting of ingesters metrics when migrating to experimental ingest storage. #9170
* [BUGFIX] Dashboards: fix `job_prefix` not utilized in `jobSelector`. #9155

### Jsonnet

* [CHANGE] Changed the following config options when the experimental ingest storage is enabled: #8874
  * `ingest_storage_ingester_autoscaling_min_replicas` changed to `ingest_storage_ingester_autoscaling_min_replicas_per_zone`
  * `ingest_storage_ingester_autoscaling_max_replicas` changed to `ingest_storage_ingester_autoscaling_max_replicas_per_zone`
* [CHANGE] Changed the overrides configmap generation to remove any field with `null` value. #9116
* [CHANGE] `$.replicaTemplate` function now takes replicas and labelSelector parameter. #9248
* [CHANGE] Renamed `ingest_storage_ingester_autoscaling_replica_template_custom_resource_definition_enabled` to `replica_template_custom_resource_definition_enabled`. #9248
* [FEATURE] Add support for automatically deleting compactor, store-gateway, ingester and read-write mode backend PVCs when the corresponding StatefulSet is scaled down. #8382 #8736
* [FEATURE] Automatically set GOMAXPROCS on ingesters. #9273
* [ENHANCEMENT] Added the following config options to set the number of partition ingester replicas when migrating to experimental ingest storage. #8517
  * `ingest_storage_migration_partition_ingester_zone_a_replicas`
  * `ingest_storage_migration_partition_ingester_zone_b_replicas`
  * `ingest_storage_migration_partition_ingester_zone_c_replicas`
* [ENHANCEMENT] Distributor: increase `-distributor.remote-timeout` when the experimental ingest storage is enabled. #8518
* [ENHANCEMENT] Memcached: Update to Memcached 1.6.28 and memcached-exporter 0.14.4. #8557
* [ENHANCEMENT] Rollout-operator: Allow the rollout-operator to be used as Kubernetes statefulset webhook to enable `no-downscale` and `prepare-downscale` annotations to be used on ingesters or store-gateways. #8743
* [ENHANCEMENT] Do not deploy ingester-zone-c when experimental ingest storage is enabled and `ingest_storage_ingester_zones` is configured to `2`. #8776
* [ENHANCEMENT] Added the config option `ingest_storage_migration_classic_ingesters_no_scale_down_delay` to disable the downscale delay on classic ingesters when migrating to experimental ingest storage. #8775 #8873
* [ENHANCEMENT] Configure experimental ingest storage on query-frontend too when enabled. #8843
* [ENHANCEMENT] Allow to override Kafka client ID on a per-component basis. #9026
* [ENHANCEMENT] Rollout-operator's access to ReplicaTemplate is now configured via config option `rollout_operator_replica_template_access_enabled`. #9252
* [ENHANCEMENT] Added support for new way of downscaling ingesters, using rollout-operator's resource-mirroring feature and read-only mode of ingesters. This can be enabled by using `ingester_automated_downscale_v2_enabled` config option. This is mutually exclusive with both `ingester_automated_downscale_enabled` (previous downscale mode) and `ingest_storage_ingester_autoscaling_enabled` (autoscaling for ingest-storage).
* [ENHANCEMENT] Update rollout-operator to `v0.19.1`. #9388
* [BUGFIX] Added missing node affinity matchers to write component. #8910

### Mimirtool

* [CHANGE] Disable colored output on mimirtool when the output is not to a terminal. #9423
* [CHANGE] Add `--force-color` flag to be able to enable colored output when the output is not to a terminal. #9423
* [CHANGE] Analyze Rules: Count recording rules used in rules group as used. #6133
* [CHANGE] Remove deprecated `--rule-files` flag in favor of CLI arguments for the following commands: #8701
  * `mimirtool rules load`
  * `mimirtool rules sync`
  * `mimirtool rules diff`
  * `mimirtool rules check`
  * `mimirtool rules prepare`
* [ENHANCEMENT] Remote read and backfill now supports the experimental native histograms. #9156

### Mimir Continuous Test

* [CHANGE] Use test metrics that do not pass through 0 to make identifying incorrect results easier. #8630
* [CHANGE] Allowed authentication to Mimir using both Tenant ID and basic/bearer auth. #9038
* [FEATURE] Experimental support for the `-tests.send-chunks-debugging-header` boolean flag to send the `X-Mimir-Chunk-Info-Logger: series_id` header with queries. #8599
* [ENHANCEMENT] Include human-friendly timestamps in diffs logged when a test fails. #8630
* [ENHANCEMENT] Add histograms to measure latency of read and write requests. #8583
* [ENHANCEMENT] Log successful test runs in addition to failed test runs. #8817
* [ENHANCEMENT] Series emitted by continuous-test now distribute more uniformly across ingesters. #9218 #9243
* [ENHANCEMENT] Configure `User-Agent` header for the Mimir client via `-tests.client.user-agent`. #9338
* [BUGFIX] Initialize test result metrics to 0 at startup so that alerts can correctly identify the first failure after startup. #8630

### Query-tee

* [CHANGE] If a preferred backend is configured, then query-tee always returns its response, regardless of the response status code. Previously, query-tee would only return the response from the preferred backend if it did not have a 5xx status code. #8634
* [ENHANCEMENT] Emit trace spans from query-tee. #8419
* [ENHANCEMENT] Log trace ID (if present) with all log messages written while processing a request. #8419
* [ENHANCEMENT] Log user agent when processing a request. #8419
* [ENHANCEMENT] Add `time` parameter to proxied instant queries if it is not included in the incoming request. This is optional but enabled by default, and can be disabled with `-proxy.add-missing-time-parameter-to-instant-queries=false`. #8419
* [ENHANCEMENT] Add support for sending only a proportion of requests to all backends, with the remainder only sent to the preferred backend. The default behaviour is to send all requests to all backends. This can be configured with `-proxy.secondary-backends-request-proportion`. #8532
* [ENHANCEMENT] Check annotations emitted by both backends are the same when comparing responses from two backends. #8660
* [ENHANCEMENT] Compare native histograms in query results when comparing results between two backends. #8724
* [ENHANCEMENT] Don't consider responses to be different during response comparison if both backends' responses contain different series, but all samples are within the recent sample window. #8749 #8894
* [ENHANCEMENT] When the expected and actual response for a matrix series is different, the full set of samples for that series from both backends will now be logged. #8947
* [ENHANCEMENT] Wait up to `-server.graceful-shutdown-timeout` for inflight requests to finish when shutting down, rather than immediately terminating inflight requests on shutdown. #8985
* [ENHANCEMENT] Optionally consider equivalent error messages the same when comparing responses. Enabled by default, disable with `-proxy.require-exact-error-match=true`. #9143 #9350 #9366
* [BUGFIX] Ensure any errors encountered while forwarding a request to a backend (eg. DNS resolution failures) are logged. #8419
* [BUGFIX] The comparison of the results should not fail when either side contains extra samples from within SkipRecentSamples duration. #8920
* [BUGFIX] When `-proxy.compare-skip-recent-samples` is enabled, compare sample timestamps with the time the query requests were made, rather than the time at which the comparison is occurring. #9416

### Documentation

* [ENHANCEMENT] Specify in which component the configuration flags `-compactor.blocks-retention-period`, `-querier.max-query-lookback`, `-query-frontend.max-total-query-length`, `-query-frontend.max-query-expression-size-bytes` are applied and that they are applied to remote read as well. #8433
* [ENHANCEMENT] Provide more detailed recommendations on how to migrate from classic to native histograms. #8864
* [ENHANCEMENT] Clarify that `{namespace}` and `{groupName}` path segments in the ruler config API should be URL-escaped. #8969
* [ENHANCEMENT] Include stalled compactor network drive information in runbooks. #9297
* [ENHANCEMENT] Document `/ingester/prepare-partition-downscale` and `/ingester/prepare-instance-ring-downscale` endpoints. #9132
* [ENHANCEMENT] Describe read-only mode of ingesters in component documentation. #9132

### Tools

* [CHANGE] `wal-reader`: Renamed `-series-entries` to `-print-series`. Renamed `-print-series-with-samples` to `-print-samples`. #8568
* [FEATURE] `query-bucket-index`: add new tool to query a bucket index file and print the blocks that would be used for a given query time range. #8818
* [FEATURE] `kafkatool`: add new CLI tool to operate Kafka. Supported commands: #9000
  * `brokers list-leaders-by-partition`
  * `consumer-group commit-offset`
  * `consumer-group copy-offset`
  * `consumer-group list-offsets`
  * `create-partitions`
* [ENHANCEMENT] `wal-reader`: References to unknown series from Samples, Exemplars, histogram or tombstones records are now always logged. #8568
* [ENHANCEMENT] `tsdb-series`: added `-stats` option to print min/max time of chunks, total number of samples and DPM for each series. #8420
* [ENHANCEMENT] `tsdb-print-chunk`: print counter reset information for native histograms. #8812
* [ENHANCEMENT] `grpcurl-query-ingesters`: print counter reset information for native histograms. #8820
* [ENHANCEMENT] `grpcurl-query-ingesters`: concurrently query ingesters. #9102
* [ENHANCEMENT] `grpcurl-query-ingesters`: sort series and chunks in output. #9180
* [ENHANCEMENT] `grpcurl-query-ingesters`: print full chunk timestamps, not just time component. #9180
* [ENHANCEMENT] `tsdb-series`: Added `-json` option to generate JSON output for easier post-processing. #8844
* [ENHANCEMENT] `tsdb-series`: Added `-min-time` and `-max-time` options to filter samples that are used for computing data-points per minute. #8844
* [ENHANCEMENT] `mimir-rules-action`: Added new input to support matching target namespaces by regex. #9244
* [ENHANCEMENT] `mimir-rules-action`: Added new inputs to support ignoring namespaces and ignoring namespaces by regex. #9258 #9324
* [BUGFIX] `copyblocks`, `undelete-blocks`, `copyprefix`: use a multipart upload to server-side copy objects greater than 5GiB in size on S3. #9357

## 2.13.0

### Grafana Mimir

* [CHANGE] Build: `grafana/mimir` docker image is now based on `gcr.io/distroless/static-debian12` image. Alpine-based docker image is still available as `grafana/mimir-alpine`, until Mimir 2.15. #8204 #8235
* [CHANGE] Ingester: `/ingester/flush` endpoint is now only allowed to execute only while the ingester is in `Running` state. The 503 status code is returned if the endpoint is called while the ingester is not in `Running` state. #7486
* [CHANGE] Distributor: Include label name in `err-mimir-label-value-too-long` error message: #7740
* [CHANGE] Ingester: enabled 1 out 10 errors log sampling by default. All the discarded samples will still be tracked by the `cortex_discarded_samples_total` metric. The feature can be configured via `-ingester.error-sample-rate` (0 to log all errors). #7807
* [CHANGE] Query-frontend: Query results caching and experimental query blocking now utilize the PromQL string-formatted query format rather than the unvalidated query as submitted to the frontend. #7742
  * Query results caching should be more stable as all equivalent queries receive the same cache key, but there may be cache churn on first deploy with the updated format
  * Query blocking can no longer be circumvented with an equivalent query in a different format; see [Configure queries to block](https://grafana.com/docs/mimir/latest/configure/configure-blocked-queries/)
* [CHANGE] Query-frontend: stop using `-validation.create-grace-period` to clamp how far into the future a query can span. #8075
* [CHANGE] Clamp [`GOMAXPROCS`](https://pkg.go.dev/runtime#GOMAXPROCS) to [`runtime.NumCPU`](https://pkg.go.dev/runtime#NumCPU). #8201
* [CHANGE] Anonymous usage statistics tracking: add CPU usage percentage tracking. #8282
* [CHANGE] Added new metric `cortex_compactor_disk_out_of_space_errors_total` which counts how many times a compaction failed due to the compactor being out of disk. #8237
* [CHANGE] Anonymous usage statistics tracking: report active series in addition to in-memory series. #8279
* [CHANGE] Ruler: `evaluation_delay` field in the rule group configuration has been deprecated. Please use `query_offset` instead (it has the same exact meaning and behaviour). #8295
* [CHANGE] General: remove `-log.buffered`. The configuration option has been enabled by default and deprecated since Mimir 2.11. #8395
* [CHANGE] Ruler: promote tenant federation from experimental to stable. #8400
* [CHANGE] Ruler: promote `-ruler.recording-rules-evaluation-enabled` and `-ruler.alerting-rules-evaluation-enabled` from experimental to stable. #8400
* [CHANGE] General: promote `-tenant-federation.max-tenants` from experimental to stable. #8400
* [FEATURE] Continuous-test: now runable as a module with `mimir -target=continuous-test`. #7747
* [FEATURE] Store-gateway: Allow specific tenants to be enabled or disabled via `-store-gateway.enabled-tenants` or `-store-gateway.disabled-tenants` CLI flags or their corresponding YAML settings. #7653
* [FEATURE] New `-<prefix>.s3.bucket-lookup-type` flag configures lookup style type, used to access bucket in s3 compatible providers. #7684
* [FEATURE] Querier: add experimental streaming PromQL engine, enabled with `-querier.promql-engine=mimir`. #7693 #7898 #7899 #8023 #8058 #8096 #8121 #8197 #8230 #8247 #8270 #8276 #8277 #8291 #8303 #8340 #8256 #8348
* [FEATURE] New `/ingester/unregister-on-shutdown` HTTP endpoint allows dynamic access to ingesters' `-ingester.ring.unregister-on-shutdown` configuration. #7739
* [FEATURE] Server: added experimental [PROXY protocol support](https://www.haproxy.org/download/2.3/doc/proxy-protocol.txt). The PROXY protocol support can be enabled via `-server.proxy-protocol-enabled=true`. When enabled, the support is added both to HTTP and gRPC listening ports. #7698
* [FEATURE] Query-frontend, querier: new experimental `/cardinality/active_native_histogram_metrics` API to get active native histogram metric names with statistics about active native histogram buckets. #7982 #7986 #8008
* [FEATURE] Alertmanager: Added `-alertmanager.max-silences-count` and `-alertmanager.max-silence-size-bytes` to set limits on per tenant silences. Disabled by default. #8241 #8249
* [FEATURE] Ingester: add experimental support for the server-side circuit breakers when writing to and reading from ingesters. This can be enabled using `-ingester.push-circuit-breaker.enabled` and `-ingester.read-circuit-breaker.enabled` options. Further `-ingester.push-circuit-breaker.*` and `-ingester.read-circuit-breaker.*` options for configuring circuit-breaker are available. Added metrics `cortex_ingester_circuit_breaker_results_total`,  `cortex_ingester_circuit_breaker_transitions_total`, `cortex_ingester_circuit_breaker_current_state` and `cortex_ingester_circuit_breaker_request_timeouts_total`. #8180 #8285 #8315 #8446
* [FEATURE] Distributor, ingester: add new setting `-validation.past-grace-period` to limit how old (based on the wall clock minus OOO window) the ingested samples can be. The default 0 value disables this limit. #8262
* [ENHANCEMENT] Distributor: add metrics `cortex_distributor_samples_per_request` and `cortex_distributor_exemplars_per_request` to track samples/exemplars per request. #8265
* [ENHANCEMENT] Reduced memory allocations in functions used to propagate contextual information between gRPC calls. #7529
* [ENHANCEMENT] Distributor: add experimental limit for exemplars per series per request, enabled with `-distributor.max-exemplars-per-series-per-request`, the number of discarded exemplars are tracked with `cortex_discarded_exemplars_total{reason="too_many_exemplars_per_series_per_request"}` #7989 #8010
* [ENHANCEMENT] Store-gateway: merge series from different blocks concurrently. #7456
* [ENHANCEMENT] Store-gateway: Add `stage="wait_max_concurrent"` to `cortex_bucket_store_series_request_stage_duration_seconds` which records how long the query had to wait for its turn for `-blocks-storage.bucket-store.max-concurrent`. #7609
* [ENHANCEMENT] Querier: add `cortex_querier_federation_upstream_query_wait_duration_seconds` to observe time from when a querier picks up a cross-tenant query to when work begins on its single-tenant counterparts. #7209
* [ENHANCEMENT] Compactor: Add `cortex_compactor_block_compaction_delay_seconds` metric to track how long it takes to compact blocks since the blocks are created. #7635
* [ENHANCEMENT] Store-gateway: add `outcome` label to `cortex_bucket_stores_gate_duration_seconds` histogram metric. Possible values for the `outcome` label are: `rejected_canceled`, `rejected_deadline_exceeded`, `rejected_other`, and `permitted`. #7784
* [ENHANCEMENT] Query-frontend: use zero-allocation experimental decoder for active series queries via `-query-frontend.use-active-series-decoder`. #7665
* [ENHANCEMENT] Go: updated to 1.22.2. #7802
* [ENHANCEMENT] Query-frontend: support `limit` parameter on `/prometheus/api/v1/label/{name}/values` and `/prometheus/api/v1/labels` endpoints. #7722
* [ENHANCEMENT] Expose TLS configuration for the S3 backend client. #7959
* [ENHANCEMENT] Rules: Support expansion of native histogram values when using rule templates #7974
* [ENHANCEMENT] Rules: Add metric `cortex_prometheus_rule_group_last_restore_duration_seconds` which measures how long it takes to restore rule groups using the `ALERTS_FOR_STATE` series #7974
* [ENHANCEMENT] OTLP: Improve remote write format translation performance by using label set hashes for metric identifiers instead of string based ones. #8012
* [ENHANCEMENT] Querying: Remove OpEmptyMatch from regex concatenations. #8012
* [ENHANCEMENT] Store-gateway: add `-blocks-storage.bucket-store.max-concurrent-queue-timeout`. When set, queries at the store-gateway's query gate will not wait longer than that to execute. If a query reaches the wait timeout, then the querier will retry the blocks on a different store-gateway. If all store-gateways are unavailable, then the query will fail with `err-mimir-store-consistency-check-failed`. #7777 #8149
* [ENHANCEMENT] Store-gateway: add `-blocks-storage.bucket-store.index-header.lazy-loading-concurrency-queue-timeout`. When set, loads of index-headers at the store-gateway's index-header lazy load gate will not wait longer than that to execute. If a load reaches the wait timeout, then the querier will retry the blocks on a different store-gateway. If all store-gateways are unavailable, then the query will fail with `err-mimir-store-consistency-check-failed`. #8138
* [ENHANCEMENT] Ingester: Optimize querying with regexp matchers. #8106
* [ENHANCEMENT] Distributor: Introduce `-distributor.max-request-pool-buffer-size` to allow configuring the maximum size of the request pool buffers. #8082
* [ENHANCEMENT] Store-gateway: improve performance when streaming chunks to queriers is enabled (`-querier.prefer-streaming-chunks-from-store-gateways=true`) and the query selects fewer than `-blocks-storage.bucket-store.batch-series-size` series (defaults to 5000 series). #8039
* [ENHANCEMENT] Ingester: active series are now updated along with owned series. They decrease when series change ownership between ingesters. This helps provide a more accurate total of active series when ingesters are added. This is only enabled when `-ingester.track-ingester-owned-series` or `-ingester.use-ingester-owned-series-for-limits` are enabled. #8084
* [ENHANCEMENT] Query-frontend: include route name in query stats log lines. #8191
* [ENHANCEMENT] OTLP: Speed up conversion from OTel to Mimir format by about 8% and reduce memory consumption by about 30%. Can be disabled via `-distributor.direct-otlp-translation-enabled=false` #7957
* [ENHANCEMENT] Ingester/Querier: Optimise regexps with long lists of alternates. #8221, #8234
* [ENHANCEMENT] Ingester: Include more detail in tracing of queries. #8242
* [ENHANCEMENT] Distributor: add `insight=true` to remote-write and OTLP write handlers when the HTTP response status code is 4xx. #8294
* [ENHANCEMENT] Ingester: reduce locked time while matching postings for a label, improving the write latency and compaction speed. #8327
* [ENHANCEMENT] Ingester: reduce the amount of locks taken during the Head compaction's garbage-collection process, improving the write latency and compaction speed. #8327
* [ENHANCEMENT] Query-frontend: log the start, end time and matchers for remote read requests to the query stats logs. #8326 #8370 #8373
* [BUGFIX] Distributor: prometheus retry on 5xx and 429 errors, while otlp collector only retry on 429, 502, 503 and 504, mapping other 5xx errors to the retryable ones in otlp endpoint. #8324 #8339
* [BUGFIX] Distributor: make OTLP endpoint return marshalled proto bytes as response body for 4xx/5xx errors. #8227
* [BUGFIX] Rules: improve error handling when querier is local to the ruler. #7567
* [BUGFIX] Querier, store-gateway: Protect against panics raised during snappy encoding. #7520
* [BUGFIX] Ingester: Prevent timely compaction of empty blocks. #7624
* [BUGFIX] Querier: Don't cache context.Canceled errors for bucket index. #7620
* [BUGFIX] Store-gateway: account for `"other"` time in LabelValues and LabelNames requests. #7622
* [BUGFIX] Query-frontend: Don't panic when using the `-query-frontend.downstream-url` flag. #7651
* [BUGFIX] Ingester: when receiving multiple exemplars for a native histogram via remote write, sort them and only report an error if all are older than the latest exemplar as this could be a partial update. #7640 #7948 #8014
* [BUGFIX] Ingester: don't retain blocks if they finish exactly on the boundary of the retention window. #7656
* [BUGFIX] Bug-fixes and improvements to experimental native histograms. #7744 #7813
* [BUGFIX] Querier: return an error when a query uses `label_join` with an invalid destination label name. #7744
* [BUGFIX] Compactor: correct outstanding job estimation in metrics and `compaction-planner` tool when block labels differ. #7745
* [BUGFIX] Ingester: turn native histogram validation errors in TSDB into soft ingester errors that result in returning 4xx to the end-user instead of 5xx. In the case of TSDB validation errors, the counter `cortex_discarded_samples_total` will be increased with the `reason` label set to `"invalid-native-histogram"`. #7736 #7773
* [BUGFIX] Do not wrap error message with `sampled 1/<frequency>` if it's not actually sampled. #7784
* [BUGFIX] Store-gateway: do not track cortex_querier_blocks_consistency_checks_failed_total metric if query has been canceled or interrued due to any error not related to blocks consistency check failed. #7752
* [BUGFIX] Ingester: ignore instances with no tokens when calculating local limits to prevent discards during ingester scale-up #7881
* [BUGFIX] Ingester: do not reuse exemplars slice in the write request if there are more than 10 exemplars per series. This should help to reduce the in-use memory in case of few requests with a very large number of exemplars. #7936
* [BUGFIX] Distributor: fix down scaling of native histograms in the distributor when timeseries unmarshal cache is in use. #7947
* [BUGFIX] Distributor: fix cardinality API to return more accurate number of in-memory series when number of zones is larger than replication factor. #7984
* [BUGFIX] All: fix config validation for non-ingester modules, when ingester's ring is configured with spread-minimizing token generation strategy. #7990
* [BUGFIX] Ingester: copy LabelValues strings out of mapped memory to avoid a segmentation fault if the region becomes unmapped before the result is marshaled. #8003
* [BUGFIX] OTLP: Don't generate target_info unless at least one identifying label is defined. #8012
* [BUGFIX] OTLP: Don't generate target_info unless there are metrics. #8012
* [BUGFIX] Query-frontend: Experimental query queue splitting: fix issue where offset and range selector duration were not considered when predicting query component. #7742
* [BUGFIX] Querying: Empty matrix results were incorrectly returning `null` instead of `[]`. #8029
* [BUGFIX] All: don't increment `thanos_objstore_bucket_operation_failures_total` metric for cancelled requests. #8072
* [BUGFIX] Query-frontend: fix empty metric name matcher not being applied under certain conditions. #8076
* [BUGFIX] Querying: Fix regex matching of multibyte runes with dot operator. #8089
* [BUGFIX] Querying: matrix results returned from instant queries were not sorted by series. #8113
* [BUGFIX] Query scheduler: Fix a crash in result marshaling. #8140
* [BUGFIX] Store-gateway: Allow long-running index scans to be interrupted. #8154
* [BUGFIX] Query-frontend: fix splitting of queries using `@ start()` and `@end()` modifiers on a subquery. Previously the `start()` and `end()` would be evaluated using the start end end of the split query instead of the original query. #8162
* [BUGFIX] Distributor: Don't discard time series with invalid exemplars, just drop affected exemplars. #8224
* [BUGFIX] Ingester: fixed in-memory series count when replaying a corrupted WAL. #8295
* [BUGFIX] Ingester: fix context cancellation handling when a query is busy looking up series in the TSDB index and `-blocks-storage.tsdb.head-postings-for-matchers-cache*` or `-blocks-storage.tsdb.block-postings-for-matchers-cache*` are in use. #8337
* [BUGFIX] Querier: fix edge case where bucket indexes are sometimes cached forever instead of with the expected TTL. #8343
* [BUGFIX] OTLP handler: fix errors returned by OTLP handler when used via httpgrpc tunneling. #8363
* [BUGFIX] Update `github.com/hashicorp/go-retryablehttp` to address [CVE-2024-6104](https://github.com/advisories/GHSA-v6v8-xj6m-xwqh). #8539
* [BUGFIX] Alertmanager: Fixes a number of bugs in silences which could cause an existing silence to be deleted/expired when updating the silence failed. This could happen when the replacing silence was invalid or exceeded limits. #8525
* [BUGFIX] Alertmanager: Fix per-tenant silence limits not reloaded during runtime. #8456
* [BUGFIX] Alertmanager: Fix help message for utf-8-strict-mode. #8572
* [BUGFIX] Upgrade golang to 1.22.5 to address [CVE-2024-24791](https://nvd.nist.gov/vuln/detail/CVE-2024-24791). #8600

### Mixin

* [CHANGE] Alerts: Removed obsolete `MimirQueriesIncorrect` alert that used test-exporter metrics. Test-exporter support was however removed in Mimir 2.0 release. #7774
* [CHANGE] Alerts: Change threshold for `MimirBucketIndexNotUpdated` alert to fire before queries begin to fail due to bucket index age. #7879
* [FEATURE] Dashboards: added 'Remote ruler reads networking' dashboard. #7751
* [FEATURE] Alerts: Add `MimirIngesterStuckProcessingRecordsFromKafka` alert. #8147
* [ENHANCEMENT] Alerts: allow configuring alerts range interval via `_config.base_alerts_range_interval_minutes`. #7591
* [ENHANCEMENT] Dashboards: Add panels for monitoring distributor and ingester when using ingest-storage. These panels are disabled by default, but can be enabled using `show_ingest_storage_panels: true` config option. Similarly existing panels used when distributors and ingesters use gRPC for forwarding requests can be disabled by setting `show_grpc_ingestion_panels: false`. #7670 #7699
* [ENHANCEMENT] Alerts: add the following alerts when using ingest-storage: #7699 #7702 #7867
  * `MimirIngesterLastConsumedOffsetCommitFailed`
  * `MimirIngesterFailedToReadRecordsFromKafka`
  * `MimirIngesterKafkaFetchErrorsRateTooHigh`
  * `MimirStartingIngesterKafkaReceiveDelayIncreasing`
  * `MimirRunningIngesterReceiveDelayTooHigh`
  * `MimirIngesterFailsToProcessRecordsFromKafka`
  * `MimirIngesterFailsEnforceStrongConsistencyOnReadPath`
* [ENHANCEMENT] Dashboards: add in-flight queries scaling metric panel for ruler-querier. #7749
* [ENHANCEMENT] Dashboards: renamed rows in the "Remote ruler reads" and "Remote ruler reads resources" dashboards to match the actual component names. #7750
* [ENHANCEMENT] Dashboards: allow switching between using classic of native histograms in dashboards. #7627
  * Overview dashboard, Status panel, `cortex_request_duration_seconds` metric.
* [ENHANCEMENT] Alerts: exclude `529` and `598` status codes from failure codes in `MimirRequestsError`. #7889
* [ENHANCEMENT] Dashboards: renamed "TCP Connections" panel to "Ingress TCP Connections" in the networking dashboards. #8092
* [ENHANCEMENT] Dashboards: update the use of deprecated "table (old)" panels to "table". #8181
* [ENHANCEMENT] Dashboards: added a `component` variable to "Slow queries" dashboard to allow checking the slow queries of the remote ruler evaluation query path. #8309
* [BUGFIX] Dashboards: fix regular expression for matching read-path gRPC ingester methods to include querying of exemplars, label-related queries, or active series queries. #7676
* [BUGFIX] Dashboards: fix user id abbreviations and column heads for Top Tenants dashboard. #7724
* [BUGFIX] Dashboards: fix incorrect query used for "queue length" panel on "Ruler" dashboard. #8006
* [BUGFIX] Dashboards: fix disk space utilization panels when running with a recent version of kube-state-metrics. #8212

### Jsonnet

* [CHANGE] Memcached: Change default read timeout for chunks and index caches to `750ms` from `450ms`. #7778
* [CHANGE] Fine-tuned `terminationGracePeriodSeconds` for the following components: #7364
  * Querier: changed from `30` to `180`
  * Query-scheduler: changed from `30` to `180`
* [CHANGE] Change TCP port exposed by `mimir-continuous-test` deployment to match with updated defaults of its container image (see changes below). #7958
* [FEATURE] Add support to deploy Mimir with experimental ingest storage enabled. #8028 #8222
* [ENHANCEMENT] Compactor: add `$._config.cortex_compactor_concurrent_rollout_enabled` option (disabled by default) that makes use of rollout-operator to speed up the rollout of compactors. #7783 #7878
* [ENHANCEMENT] Shuffle-sharding: add `$._config.shuffle_sharding.ingest_storage_partitions_enabled` and `$._config.shuffle_sharding.ingester_partitions_shard_size` options, that allow configuring partitions shard size in ingest-storage mode. #7804
* [ENHANCEMENT] Update rollout-operator to `v0.17.0`. #8399
* [ENHANCEMENT] Add `_config.autoscaling_querier_predictive_scaling_enabled` to scale querier based on inflight queries 7 days ago. #7775
* [ENHANCEMENT] Add support to autoscale ruler-querier replicas based on in-flight queries too (in addition to CPU and memory based scaling). #8060 #8188
* [ENHANCEMENT] Distributor: improved distributor HPA scaling metric to only take in account ready pods. This requires the metric `kube_pod_status_ready` to be available in the data source used by KEDA to query scaling metrics (configured via `_config.autoscaling_prometheus_url`). #8251
* [BUGFIX] Guard against missing samples in KEDA queries. #7691 #10013
* [BUGFIX] Alertmanager: Set -server.http-idle-timeout to avoid EOF errors in ruler. #8192

### Mimirtool

* [CHANGE] Deprecated `--rule-files` flag in favor of CLI arguments. #7756
* [FEATURE] mimirtool: Add `runtime-config verify` sub-command, for verifying Mimir runtime config files. #8123
* [ENHANCEMENT] `mimirtool promql format`: Format PromQL query with Prometheus' string or pretty-print formatter. #7742
* [ENHANCEMENT] Add `mimir-http-prefix` configuration to set the Mimir URL prefix when using legacy routes. #8069
* [ENHANCEMENT] Add option `--output-dir` to `mimirtool rules get` and `mimirtool rules print` to allow persisting rule groups to a file for edit and re-upload. #8142
* [BUGFIX] Fix panic in `loadgen` subcommand. #7629
* [BUGFIX] `mimirtool rules prepare`: do not add aggregation label to `on()` clause if already present in `group_left()` or `group_right()`. #7839
* [BUGFIX] Analyze Grafana: fix parsing queries with variables. #8062
* [BUGFIX] `mimirtool rules sync`: detect a change when the `query_offset` or the deprecated `evaluation_delay` configuration changes. #8297

### Mimir Continuous Test

* [CHANGE] `mimir-continuous-test` has been deprecated and replaced by a Mimir module that can be run as a target from the `mimir` binary using `mimir -target=continuous-test`. #7753
* [CHANGE] `-server.metrics-port` flag is no longer available for use in the module run of mimir-continuous-test, including the grafana/mimir-continuous-test Docker image which uses the new module. Configuring this port is still possible in the binary, which is deprecated. #7747
* [CHANGE] Allowed authenticatication to Mimir using both Tenant ID and basic/bearer auth #7619.
* [BUGFIX] Set `User-Agent` header for all requests sent from the testing client. #7607

### Query-tee

* [ENHANCEMENT] Log queries that take longer than `proxy.log-slow-query-response-threshold` when compared to other backends. #7346
* [ENHANCEMENT] Add two new metrics for measuring the relative duration between backends: #7782 #8013 #8330
  * `cortex_querytee_backend_response_relative_duration_seconds`
  * `cortex_querytee_backend_response_relative_duration_proportional`

### Documentation

* [CHANGE] Note that the _Play with Grafana Mimir_ tutorial directory path changed after the release of the video. #8319
* [ENHANCEMENT] Clarify Compactor and its storage volume when configured under Kubernetes. #7675
* [ENHANCEMENT] Add OTLP route to _Mimir routes by path_ runbooks section. #8074
* [ENHANCEMENT] Document option server.log-source-ips-full. #8268

### Tools

* [ENHANCEMENT] ulidtime: add option to show random part of ULID, timestamp in milliseconds and header. #7615
* [ENHANCEMENT] copyblocks: add a flag to configure part-size for multipart uploads in s3 client-side copying. #8292
* [ENHANCEMENT] copyblocks: enable pprof HTTP endpoints. #8292

## 2.12.0

### Grafana Mimir

* [CHANGE] Alertmanager: Deprecates the `v1` API. All `v1` API endpoints now respond with a JSON deprecation notice and a status code of `410`. All endpoints have a `v2` equivalent. The list of endpoints is: #7103
  * `<alertmanager-web.external-url>/api/v1/alerts`
  * `<alertmanager-web.external-url>/api/v1/receivers`
  * `<alertmanager-web.external-url>/api/v1/silence/{id}`
  * `<alertmanager-web.external-url>/api/v1/silences`
  * `<alertmanager-web.external-url>/api/v1/status`
* [CHANGE] Ingester: Increase default value of `-blocks-storage.tsdb.head-postings-for-matchers-cache-max-bytes` and `-blocks-storage.tsdb.block-postings-for-matchers-cache-max-bytes` to 100 MiB (previous default value was 10 MiB). #6764
* [CHANGE] Validate tenant IDs according to [documented behavior](https://grafana.com/docs/mimir/latest/configure/about-tenant-ids/) even when tenant federation is not enabled. Note that this will cause some previously accepted tenant IDs to be rejected such as those longer than 150 bytes or containing `|` characters. #6959
* [CHANGE] Ruler: don't use backoff retry on remote evaluation in case of `4xx` errors. #7004
* [CHANGE] Server: responses with HTTP 4xx status codes are now treated as errors and used in `status_code` label of request duration metric. #7045
* [CHANGE] Memberlist: change default for `-memberlist.stream-timeout` from `10s` to `2s`. #7076
* [CHANGE] Memcached: remove legacy `thanos_cache_memcached_*` and `thanos_memcached_*` prefixed metrics. Instead, Memcached and Redis cache clients now emit `thanos_cache_*` prefixed metrics with a `backend` label. #7076
* [CHANGE] Ruler: the following metrics, exposed when the ruler is configured to discover Alertmanager instances via service discovery, have been renamed: #7057
  * `prometheus_sd_failed_configs` renamed to `cortex_prometheus_sd_failed_configs`
  * `prometheus_sd_discovered_targets` renamed to `cortex_prometheus_sd_discovered_targets`
  * `prometheus_sd_received_updates_total` renamed to `cortex_prometheus_sd_received_updates_total`
  * `prometheus_sd_updates_delayed_total` renamed to `cortex_prometheus_sd_updates_delayed_total`
  * `prometheus_sd_updates_total` renamed to `cortex_prometheus_sd_updates_total`
  * `prometheus_sd_refresh_failures_total` renamed to `cortex_prometheus_sd_refresh_failures_total`
  * `prometheus_sd_refresh_duration_seconds` renamed to `cortex_prometheus_sd_refresh_duration_seconds`
* [CHANGE] Query-frontend: the default value for `-query-frontend.not-running-timeout` has been changed from 0 (disabled) to 2s. The configuration option has also been moved from "experimental" to "advanced". #7127
* [CHANGE] Store-gateway: to reduce disk contention on HDDs the default value for `blocks-storage.bucket-store.tenant-sync-concurrency` has been changed from `10` to `1` and the default value for `blocks-storage.bucket-store.block-sync-concurrency` has been changed from `20` to `4`. #7136
* [CHANGE] Store-gateway: Remove deprecated CLI flags `-blocks-storage.bucket-store.index-header-lazy-loading-enabled` and `-blocks-storage.bucket-store.index-header-lazy-loading-idle-timeout` and their corresponding YAML settings. Instead, use `-blocks-storage.bucket-store.index-header.lazy-loading-enabled` and `-blocks-storage.bucket-store.index-header.lazy-loading-idle-timeout`. #7521
* [CHANGE] Store-gateway: Mark experimental CLI flag `-blocks-storage.bucket-store.index-header.lazy-loading-concurrency` and its corresponding YAML settings as advanced. #7521
* [CHANGE] Store-gateway: Remove experimental CLI flag `-blocks-storage.bucket-store.index-header.sparse-persistence-enabled` since this is now the default behavior. #7535
* [CHANGE] All: set `-server.report-grpc-codes-in-instrumentation-label-enabled` to `true` by default, which enables reporting gRPC status codes as `status_code` labels in the `cortex_request_duration_seconds` metric. #7144
* [CHANGE] Distributor: report gRPC status codes as `status_code` labels in the `cortex_ingester_client_request_duration_seconds` metric by default. #7144
* [CHANGE] Distributor: CLI flag `-ingester.client.report-grpc-codes-in-instrumentation-label-enabled` has been deprecated, and its default value is set to `true`. #7144
* [CHANGE] Ingester: CLI flag `-ingester.return-only-grpc-errors` has been deprecated, and its default value is set to `true`. To ensure backwards compatibility, during a migration from a version prior to 2.11.0 to 2.12 or later, `-ingester.return-only-grpc-errors` should be set to `false`. Once all the components are migrated, the flag can be removed.   #7151
* [CHANGE] Ingester: the following CLI flags have been moved from "experimental" to "advanced": #7169
  * `-ingester.ring.token-generation-strategy`
  * `-ingester.ring.spread-minimizing-zones`
  * `-ingester.ring.spread-minimizing-join-ring-in-order`
* [CHANGE] Query-frontend: the default value of the CLI flag `-query-frontend.max-cache-freshness` (and its respective YAML configuration parameter) has been changed from `1m` to `10m`. #7161
* [CHANGE] Distributor: default the optimization `-distributor.write-requests-buffer-pooling-enabled` to `true`. #7165
* [CHANGE] Tracing: Move query information to span attributes instead of span logs. #7046
* [CHANGE] Distributor: the default value of circuit breaker's CLI flag `-ingester.client.circuit-breaker.cooldown-period` has been changed from `1m` to `10s`. #7310
* [CHANGE] Store-gateway: remove `cortex_bucket_store_blocks_loaded_by_duration`. `cortex_bucket_store_series_blocks_queried` is better suited for detecting when compactors are not able to keep up with the number of blocks to compact. #7309
* [CHANGE] Ingester, Distributor: the support for rejecting push requests received via gRPC before reading them into memory, enabled via `-ingester.limit-inflight-requests-using-grpc-method-limiter` and `-distributor.limit-inflight-requests-using-grpc-method-limiter`, is now stable and enabled by default. The configuration options have been deprecated and will be removed in Mimir 2.14. #7360
* [CHANGE] Distributor: Change`-distributor.enable-otlp-metadata-storage` flag's default to true, and deprecate it. The flag will be removed in Mimir 2.14. #7366
* [CHANGE] Store-gateway: Use a shorter TTL for cached items related to temporary blocks. #7407 #7534
* [CHANGE] Standardise exemplar label as "trace_id". #7475
* [CHANGE] The configuration option `-querier.max-query-into-future` has been deprecated and will be removed in Mimir 2.14. #7496
* [CHANGE] Distributor: the metric `cortex_distributor_sample_delay_seconds` has been deprecated and will be removed in Mimir 2.14. #7516
* [CHANGE] Query-frontend: The deprecated YAML setting `frontend.cache_unaligned_requests` has been moved to `limits.cache_unaligned_requests`. #7519
* [CHANGE] Querier: the CLI flag `-querier.minimize-ingester-requests` has been moved from "experimental" to "advanced". #7638
* [CHANGE] Ingester: allow only POST method on `/ingester/shutdown`, as previously it was too easy to accidentally trigger through GET requests. At the same time, add an option to keep the existing behavior by introducing an `-api.get-request-for-ingester-shutdown-enabled` flag. This flag will be removed in Mimir 2.15. #7707
* [FEATURE] Introduce `-server.log-source-ips-full` option to log all IPs from `Forwarded`, `X-Real-IP`, `X-Forwarded-For` headers. #7250
* [FEATURE] Introduce `-tenant-federation.max-tenants` option to limit the max number of tenants allowed for requests when federation is enabled. #6959
* [FEATURE] Cardinality API: added a new `count_method` parameter which enables counting active label names. #7085
* [FEATURE] Querier / query-frontend: added `-querier.promql-experimental-functions-enabled` CLI flag (and respective YAML config option) to enable experimental PromQL functions. The experimental functions introduced are: `mad_over_time()`, `sort_by_label()` and `sort_by_label_desc()`. #7057
* [FEATURE] Alertmanager API: added `-alertmanager.grafana-alertmanager-compatibility-enabled` CLI flag (and respective YAML config option) to enable an experimental API endpoints that support the migration of the Grafana Alertmanager. #7057
* [FEATURE] Alertmanager: Added `-alertmanager.utf8-strict-mode-enabled` to control support for any UTF-8 character as part of Alertmanager configuration/API matchers and labels. It's default value is set to `false`. #6898
* [FEATURE] Querier: added `histogram_avg()` function support to PromQL. #7293
* [FEATURE] Ingester: added `-blocks-storage.tsdb.timely-head-compaction` flag, which enables more timely head compaction, and defaults to `false`. #7372
* [FEATURE] Compactor: Added `/compactor/tenants` and `/compactor/tenant/{tenant}/planned_jobs` endpoints that provide functionality that was provided by `tools/compaction-planner` -- listing of planned compaction jobs based on tenants' bucket index. #7381
* [FEATURE] Add experimental support for streaming response bodies from queriers to frontends via `-querier.response-streaming-enabled`. This is currently only supported for the `/api/v1/cardinality/active_series` endpoint. #7173
* [FEATURE] Release: Added mimir distroless docker image. #7371
* [FEATURE] Add support for the new grammar of `{"metric_name", "l1"="val"}` to promql and some of the exposition formats. #7475 #7541
* [ENHANCEMENT] Distributor: Add a new metric `cortex_distributor_otlp_requests_total` to track the total number of OTLP requests. #7385
* [ENHANCEMENT] Vault: add lifecycle manager for token used to authenticate to Vault. This ensures the client token is always valid. Includes a gauge (`cortex_vault_token_lease_renewal_active`) to check whether token renewal is active, and the counters `cortex_vault_token_lease_renewal_success_total` and `cortex_vault_auth_success_total` to see the total number of successful lease renewals / authentications. #7337
* [ENHANCEMENT] Store-gateway: add no-compact details column on store-gateway tenants admin UI. #6848
* [ENHANCEMENT] PromQL: ignore small errors for bucketQuantile #6766
* [ENHANCEMENT] Distributor: improve efficiency of some errors #6785
* [ENHANCEMENT] Ruler: exclude vector queries from being tracked in `cortex_ruler_queries_zero_fetched_series_total`. #6544
* [ENHANCEMENT] Ruler: local storage backend now supports reading a rule group via `/config/api/v1/rules/{namespace}/{groupName}` configuration API endpoint. #6632
* [ENHANCEMENT] Query-Frontend and Query-Scheduler: split tenant query request queues by query component with `query-frontend.additional-query-queue-dimensions-enabled` and `query-scheduler.additional-query-queue-dimensions-enabled`. #6772
* [ENHANCEMENT] Distributor: support disabling metric relabel rules per-tenant via the flag `-distributor.metric-relabeling-enabled` or associated YAML. #6970
* [ENHANCEMENT] Distributor: `-distributor.remote-timeout` is now accounted from the first ingester push request being sent. #6972
* [ENHANCEMENT] Storage Provider: `-<prefix>.s3.sts-endpoint` sets a custom endpoint for AWS Security Token Service (AWS STS) in s3 storage provider. #6172
* [ENHANCEMENT] Querier: add `cortex_querier_queries_storage_type_total ` metric that indicates how many queries have executed for a source, ingesters or store-gateways. Add `cortex_querier_query_storegateway_chunks_total` metric to count the number of chunks fetched from a store gateway. #7099,#7145
* [ENHANCEMENT] Query-frontend: add experimental support for sharding active series queries via `-query-frontend.shard-active-series-queries`. #6784
* [ENHANCEMENT] Distributor: set `-distributor.reusable-ingester-push-workers=2000` by default and mark feature as `advanced`. #7128
* [ENHANCEMENT] All: set `-server.grpc.num-workers=100` by default and mark feature as `advanced`. #7131
* [ENHANCEMENT] Distributor: invalid metric name error message gets cleaned up to not include non-ascii strings. #7146
* [ENHANCEMENT] Store-gateway: add `source`, `level`, and `out_or_order` to `cortex_bucket_store_series_blocks_queried` metric that indicates the number of blocks that were queried from store gateways by block metadata. #7112 #7262 #7267
* [ENHANCEMENT] Compactor: After updating bucket-index, compactor now also computes estimated number of compaction jobs based on current bucket-index, and reports the result in `cortex_bucket_index_estimated_compaction_jobs` metric. If computation of jobs fails, `cortex_bucket_index_estimated_compaction_jobs_errors_total` is updated instead. #7299
* [ENHANCEMENT] Mimir: Integrate profiling into tracing instrumentation. #7363
* [ENHANCEMENT] Alertmanager: Adds metric `cortex_alertmanager_notifications_suppressed_total` that counts the total number of notifications suppressed for being silenced, inhibited, outside of active time intervals or within muted time intervals. #7384
* [ENHANCEMENT] Query-scheduler: added more buckets to `cortex_query_scheduler_queue_duration_seconds` histogram metric, in order to better track queries staying in the queue for longer than 10s. #7470
* [ENHANCEMENT] A `type` label is added to `prometheus_tsdb_head_out_of_order_samples_appended_total` metric. #7475
* [ENHANCEMENT] Distributor: Optimize OTLP endpoint. #7475
* [ENHANCEMENT] API: Use github.com/klauspost/compress for faster gzip and deflate compression of API responses. #7475
* [ENHANCEMENT] Ingester: Limiting on owned series (`-ingester.use-ingester-owned-series-for-limits`) now prevents discards in cases where a tenant is sharded across all ingesters (or shuffle sharding is disabled) and the ingester count increases. #7411
* [ENHANCEMENT] Block upload: include converted timestamps in the error message if block is from the future. #7538
* [ENHANCEMENT] Query-frontend: Introduce `-query-frontend.active-series-write-timeout` to allow configuring the server-side write timeout for active series requests. #7553 #7569
* [BUGFIX] Ingester: don't ignore errors encountered while iterating through chunks or samples in response to a query request. #6451
* [BUGFIX] Fix issue where queries can fail or omit OOO samples if OOO head compaction occurs between creating a querier and reading chunks #6766
* [BUGFIX] Fix issue where concatenatingChunkIterator can obscure errors #6766
* [BUGFIX] Fix panic during tsdb Commit #6766
* [BUGFIX] tsdb/head: wlog exemplars after samples #6766
* [BUGFIX] Ruler: fix issue where "failed to remotely evaluate query expression, will retry" messages are logged without context such as the trace ID and do not appear in trace events. #6789
* [BUGFIX] Ruler: do not retry requests to remote querier when server's response exceeds its configured max payload size. #7216
* [BUGFIX] Querier: fix issue where spans in query request traces were not nested correctly. #6893
* [BUGFIX] Fix issue where all incoming HTTP requests have duplicate trace spans. #6920
* [BUGFIX] Querier: do not retry requests to store-gateway when a query gets canceled. #6934
* [BUGFIX] Querier: return 499 status code instead of 500 when a request to remote read endpoint gets canceled. #6934
* [BUGFIX] Querier: fix issue where `-querier.max-fetched-series-per-query` is not applied to `/series` endpoint if the series are loaded from ingesters. #7055
* [BUGFIX] Distributor: fix issue where `-distributor.metric-relabeling-enabled` may cause distributors to panic #7176
* [BUGFIX] Distributor: fix issue where `-distributor.metric-relabeling-enabled` may cause distributors to write unsorted labels and corrupt blocks #7326
* [BUGFIX] Query-frontend: the `cortex_query_frontend_queries_total` report incorrectly reported `op="query"` for any request which wasn't a range query. Now the `op` label value can be one of the following: #7207
  * `query`: instant query
  * `query_range`: range query
  * `cardinality`: cardinality query
  * `label_names_and_values`: label names / values query
  * `active_series`: active series query
  * `other`: any other request
* [BUGFIX] Fix performance regression introduced in Mimir 2.11.0 when uploading blocks to AWS S3. #7240
* [BUGFIX] Query-frontend: fix race condition when sharding active series is enabled (see above) and response is compressed with snappy. #7290
* [BUGFIX] Query-frontend: "query stats" log unsuccessful replies from downstream as "failed". #7296
* [BUGFIX] Packaging: remove reload from systemd file as mimir does not take into account SIGHUP. #7345
* [BUGFIX] Compactor: do not allow out-of-order blocks to prevent timely compaction. #7342
* [BUGFIX] Update `google.golang.org/grpc` to resolve occasional issues with gRPC server closing its side of connection before it was drained by the client. #7380
* [BUGFIX] Query-frontend: abort response streaming for `active_series` requests when the request context is canceled. #7378
* [BUGFIX] Compactor: improve compaction of sporadic blocks. #7329
* [BUGFIX] Ruler: fix regression that caused client errors to be tracked in `cortex_ruler_write_requests_failed_total` metric. #7472
* [BUGFIX] promql: Fix Range selectors with an @ modifier are wrongly scoped in range queries. #7475
* [BUGFIX] Fix metadata API using wrong JSON field names. #7475
* [BUGFIX] Ruler: fix native histogram recording rule result corruption. #7552
* [BUGFIX] Querier: fix HTTP status code translations for remote read requests. Previously, remote-read had conflicting behaviours: when returning samples all internal errors were translated to HTTP 400; when returning chunks all internal errors were translated to HTTP 500. #7487
* [BUGFIX] Query-frontend: Fix memory leak on every request. #7654

### Mixin

* [CHANGE] The `job` label matcher for distributor and gateway have been extended to include any deployment matching `distributor.*` and `cortex-gw.*` respectively. This change allows to match custom and multi-zone distributor and gateway deployments too. #6817
* [ENHANCEMENT] Dashboards: Add panels for alertmanager activity of a tenant #6826
* [ENHANCEMENT] Dashboards: Add graphs to "Slow Queries" dashboard. #6880
* [ENHANCEMENT] Dashboards: Update all deprecated "graph" panels to "timeseries" panels. #6864 #7413 #7457
* [ENHANCEMENT] Dashboards: Make most columns in "Slow Queries" sortable. #7000
* [ENHANCEMENT] Dashboards: Render graph panels at full resolution as opposed to at half resolution. #7027
* [ENHANCEMENT] Dashboards: show query-scheduler queue length on "Reads" and "Remote Ruler Reads" dashboards. #7088
* [ENHANCEMENT] Dashboards: Add estimated number of compaction jobs to "Compactor", "Tenants" and "Top tenants" dashboards. #7449 #7481
* [ENHANCEMENT] Recording rules: add native histogram recording rules to `cortex_request_duration_seconds`. #7528
* [ENHANCEMENT] Dashboards: Add total owned series, and per-ingester in-memory and owned series to "Tenants" dashboard. #7511
* [BUGFIX] Dashboards: drop `step` parameter from targets as it is not supported. #7157
* [BUGFIX] Recording rules: drop rules for metrics removed in 2.0: `cortex_memcache_request_duration_seconds` and `cortex_cache_request_duration_seconds`. #7514

### Jsonnet

* [CHANGE] Distributor: Increase `JAEGER_REPORTER_MAX_QUEUE_SIZE` from the default (100) to 1000, to avoid dropping tracing spans. #7259
* [CHANGE] Querier: Increase `JAEGER_REPORTER_MAX_QUEUE_SIZE` from 1000 to 5000, to avoid dropping tracing spans. #6764
* [CHANGE] rollout-operator: remove default CPU limit. #7066
* [CHANGE] Store-gateway: Increase `JAEGER_REPORTER_MAX_QUEUE_SIZE` from the default (100) to 1000, to avoid dropping tracing spans. #7068
* [CHANGE] Query-frontend, ingester, ruler, backend and write instances: Increase `JAEGER_REPORTER_MAX_QUEUE_SIZE` from the default (100), to avoid dropping tracing spans. #7086
* [CHANGE] Ring: relaxed the hash ring heartbeat period and timeout for distributor, ingester, store-gateway and compactor: #6860
  * `-distributor.ring.heartbeat-period` set to `1m`
  * `-distributor.ring.heartbeat-timeout` set to `4m`
  * `-ingester.ring.heartbeat-period` set to `2m`
  * `-store-gateway.sharding-ring.heartbeat-period` set to `1m`
  * `-store-gateway.sharding-ring.heartbeat-timeout` set to `4m`
  * `-compactor.ring.heartbeat-period` set to `1m`
  * `-compactor.ring.heartbeat-timeout` set to `4m`
* [CHANGE] Ruler-querier: the topology spread constrain max skew is now configured through the configuration option `ruler_querier_topology_spread_max_skew` instead of `querier_topology_spread_max_skew`. #7204
* [CHANGE] Distributor: `-server.grpc.keepalive.max-connection-age` lowered from `2m` to `60s` and configured `-shutdown-delay=90s` and termination grace period to `100` seconds in order to reduce the chances of failed gRPC write requests when distributors gracefully shutdown. #7361
* [FEATURE] Added support for the following root-level settings to configure the list of matchers to apply to node affinity: #6782 #6829
  * `alertmanager_node_affinity_matchers`
  * `compactor_node_affinity_matchers`
  * `continuous_test_node_affinity_matchers`
  * `distributor_node_affinity_matchers`
  * `ingester_node_affinity_matchers`
  * `ingester_zone_a_node_affinity_matchers`
  * `ingester_zone_b_node_affinity_matchers`
  * `ingester_zone_c_node_affinity_matchers`
  * `mimir_backend_node_affinity_matchers`
  * `mimir_backend_zone_a_node_affinity_matchers`
  * `mimir_backend_zone_b_node_affinity_matchers`
  * `mimir_backend_zone_c_node_affinity_matchers`
  * `mimir_read_node_affinity_matchers`
  * `mimir_write_node_affinity_matchers`
  * `mimir_write_zone_a_node_affinity_matchers`
  * `mimir_write_zone_b_node_affinity_matchers`
  * `mimir_write_zone_c_node_affinity_matchers`
  * `overrides_exporter_node_affinity_matchers`
  * `querier_node_affinity_matchers`
  * `query_frontend_node_affinity_matchers`
  * `query_scheduler_node_affinity_matchers`
  * `rollout_operator_node_affinity_matchers`
  * `ruler_node_affinity_matchers`
  * `ruler_node_affinity_matchers`
  * `ruler_querier_node_affinity_matchers`
  * `ruler_query_frontend_node_affinity_matchers`
  * `ruler_query_scheduler_node_affinity_matchers`
  * `store_gateway_node_affinity_matchers`
  * `store_gateway_node_affinity_matchers`
  * `store_gateway_zone_a_node_affinity_matchers`
  * `store_gateway_zone_b_node_affinity_matchers`
  * `store_gateway_zone_c_node_affinity_matchers`
* [FEATURE] Ingester: Allow automated zone-by-zone downscaling, that can be enabled via the `ingester_automated_downscale_enabled` flag. It is disabled by default. #6850
* [ENHANCEMENT] Alerts: Add `MimirStoreGatewayTooManyFailedOperations` warning alert that triggers when Mimir store-gateway report error when interacting with the object storage. #6831
* [ENHANCEMENT] Querier HPA: improved scaling metric and scaling policies, in order to scale up and down more gradually. #6971
* [ENHANCEMENT] Rollout-operator: upgraded to v0.13.0. #7469
* [ENHANCEMENT] Rollout-operator: add tracing configuration to rollout-operator container (when tracing is enabled and configured). #7469
* [ENHANCEMENT] Query-frontend: configured `-shutdown-delay`, `-server.grpc.keepalive.max-connection-age` and termination grace period to reduce the likelihood of queries hitting terminated query-frontends. #7129
* [ENHANCEMENT] Autoscaling: add support for KEDA's `ignoreNullValues` option for Prometheus scaler. #7471
* [BUGFIX] Update memcached-exporter to 0.14.1 due to CVE-2023-39325. #6861

### Mimirtool

* [FEATURE] Add command `migrate-utf8` to migrate Alertmanager configurations for Alertmanager versions 0.27.0 and later. #7383
* [ENHANCEMENT] Add template render command to render locally a template. #7325
* [ENHANCEMENT] Add `--extra-headers` option to `mimirtool rules` command to add extra headers to requests for auth. #7141
* [ENHANCEMENT] Analyze Prometheus: set tenant header. #6737
* [ENHANCEMENT] Add argument `--output-dir` to `mimirtool alertmanager get` where the config and templates will be written to and can be loaded via `mimirtool alertmanager load` #6760
* [BUGFIX] Analyze rule-file: .metricsUsed field wasn't populated. #6953

### Mimir Continuous Test

* [ENHANCEMENT] Include comparison of all expected and actual values when any float sample does not match. #6756

### Query-tee

* [BUGFIX] Fix issue where `Host` HTTP header was not being correctly changed for the proxy targets. #7386
* [ENHANCEMENT] Allow using the value of X-Scope-OrgID for basic auth username in the forwarded request if URL username is set as `__REQUEST_HEADER_X_SCOPE_ORGID__`. #7452

### Documentation

* [CHANGE] No longer mark OTLP distributor endpoint as experimental. #7348
* [ENHANCEMENT] Added runbook for `KubePersistentVolumeFillingUp` alert. #7297
* [ENHANCEMENT] Add Grafana Cloud recommendations to OTLP documentation. #7375
* [BUGFIX] Fixed typo on single zone->zone aware replication Helm page. #7327

### Tools

* [CHANGE] copyblocks: The flags for copyblocks have been changed to align more closely with other tools. #6607
* [CHANGE] undelete-blocks: undelete-blocks-gcs has been removed and replaced with undelete-blocks, which supports recovering deleted blocks in versioned buckets from ABS, GCS, and S3-compatible object storage. #6607
* [FEATURE] copyprefix: Add tool to copy objects between prefixes. Supports ABS, GCS, and S3-compatible object storage. #6607

## 2.11.0

### Grafana Mimir

* [CHANGE] The following deprecated configurations have been removed: #6673 #6779 #6808 #6814
  * `-querier.iterators`
  * `-querier.batch-iterators`
  * `-blocks-storage.bucket-store.max-chunk-pool-bytes`
  * `-blocks-storage.bucket-store.chunk-pool-min-bucket-size-bytes`
  * `-blocks-storage.bucket-store.chunk-pool-max-bucket-size-bytes`
  * `-blocks-storage.bucket-store.bucket-index.enabled`
* [CHANGE] Querier: Split worker GRPC config into separate client configs for the frontend and scheduler to allow TLS to be configured correctly when specifying the `tls_server_name`. The GRPC config specified under `-querier.frontend-client.*` will no longer apply to the scheduler client, and will need to be set explicitly under `-querier.scheduler-client.*`. #6445 #6573
* [CHANGE] Store-gateway: enable sparse index headers by default. Sparse index headers reduce the time to load an index header up to 90%. #6005
* [CHANGE] Store-gateway: lazy-loading concurrency limit default value is now 4. #6004
* [CHANGE] General: enabled `-log.buffered` by default. The `-log.buffered` has been deprecated and will be removed in Mimir 2.13. #6131
* [CHANGE] Ingester: changed default `-blocks-storage.tsdb.series-hash-cache-max-size-bytes` setting from `1GB` to `350MB`. The new default cache size is enough to store the hashes for all series in a ingester, assuming up to 2M in-memory series per ingester and using the default 13h retention period for local TSDB blocks in the ingesters. #6130
* [CHANGE] Query-frontend: removed `cortex_query_frontend_workers_enqueued_requests_total`. Use `cortex_query_frontend_enqueue_duration_seconds_count` instead. #6121
* [CHANGE] Ingester / querier: enable ingester to querier chunks streaming by default and mark it as stable. #6174
* [CHANGE] Ingester / querier: enable ingester query request minimisation by default and mark it as stable. #6174
* [CHANGE] Ingester: changed the default value for the experimental configuration parameter `-blocks-storage.tsdb.early-head-compaction-min-estimated-series-reduction-percentage` from 10 to 15. #6186
* [CHANGE] Ingester: `/ingester/push` HTTP endpoint has been removed. This endpoint was added for testing and troubleshooting, but was never documented or used for anything. #6299
* [CHANGE] Experimental setting `-log.rate-limit-logs-per-second-burst` renamed to `-log.rate-limit-logs-burst-size`. #6230
* [CHANGE] Ingester: by setting the newly introduced experimental CLI flag `-ingester.return-only-grpc-errors` to true, ingester will return only gRPC errors. #6443 #6680 #6723
* [CHANGE] Upgrade Node.js to v20. #6540
* [CHANGE] Querier: `cortex_querier_blocks_consistency_checks_failed_total` is now incremented when a block couldn't be queried from any attempted store-gateway as opposed to incremented after each attempt. Also `cortex_querier_blocks_consistency_checks_total` is incremented once per query as opposed to once per attempt (with 3 attempts). #6590
* [CHANGE] Ingester: Modify utilization based read path limiter to base memory usage on Go heap size. #6584
* [FEATURE] Distributor: added option `-distributor.retry-after-header.enabled` to include the `Retry-After` header in recoverable error responses. #6608
* [FEATURE] Query-frontend: add experimental support for query blocking. Queries are blocked on a per-tenant basis and is configured via the limit `blocked_queries`. #5609
* [FEATURE] Vault: Added support for new Vault authentication methods: `AppRole`, `Kubernetes`, `UserPass` and `Token`. #6143
* [FEATURE] Add experimental endpoint `/api/v1/cardinality/active_series` to return the set of active series for a given selector. #6536 #6619 #6651 #6667 #6717
* [FEATURE] Added `-<prefix>.s3.part-size` flag to configure the S3 minimum file size in bytes used for multipart uploads. #6592
* [FEATURE] Add the experimental `-<prefix>.s3.send-content-md5` flag (defaults to `false`) to configure S3 Put Object requests to send a `Content-MD5` header. Setting this flag is not recommended unless your object storage does not support checksums. #6622
* [FEATURE] Distributor: add an experimental flag `-distributor.reusable-ingester-push-worker` that can be used to pre-allocate a pool of workers to be used to send push requests to the ingesters. #6660
* [FEATURE] Distributor: Support enabling of automatically generated name suffixes for metrics ingested via OTLP, through the flag `-distributor.otel-metric-suffixes-enabled`. #6542
* [FEATURE] Ingester: ingester can now track which of the user's series the ingester actually owns according to the ring, and only consider owned series when checking for user series limit. This helps to avoid hitting the user's series limit when scaling up ingesters or changing user's ingester shard size. Feature is currently experimental, and disabled by default. It can be enabled by setting `-ingester.use-ingester-owned-series-for-limits` (to use owned series for limiting). This is currently limited to multi-zone ingester setup, with replication factor being equal to number of zones. #6718 #7087
* [ENHANCEMENT] Query-frontend: don't treat cancel as an error. #4648
* [ENHANCEMENT] Ingester: exported summary `cortex_ingester_inflight_push_requests_summary` tracking total number of inflight requests in percentile buckets. #5845
* [ENHANCEMENT] Query-scheduler: add `cortex_query_scheduler_enqueue_duration_seconds` metric that records the time taken to enqueue or reject a query request. #5879
* [ENHANCEMENT] Query-frontend: add `cortex_query_frontend_enqueue_duration_seconds` metric that records the time taken to enqueue or reject a query request. When query-scheduler is in use, the metric has the `scheduler_address` label to differentiate the enqueue duration by query-scheduler backend. #5879 #6087 #6120
* [ENHANCEMENT] Store-gateway: add metric `cortex_bucket_store_blocks_loaded_by_duration` for counting the loaded number of blocks based on their duration. #6074  #6129
* [ENHANCEMENT] Expose `/sync/mutex/wait/total:seconds` Go runtime metric as `go_sync_mutex_wait_total_seconds_total` from all components. #5879
* [ENHANCEMENT] Query-scheduler: improve latency with many concurrent queriers. #5880
* [ENHANCEMENT] Ruler: add new per-tenant `cortex_ruler_queries_zero_fetched_series_total` metric to track rules that fetched no series. #5925
* [ENHANCEMENT] Implement support for `limit`, `limit_per_metric` and `metric` parameters for `<Prometheus HTTP prefix>/api/v1/metadata` endpoint. #5890
* [ENHANCEMENT] Distributor: add experimental support for storing metadata when ingesting metrics via OTLP. This makes metrics description and type available when ingesting metrics via OTLP. Enable with `-distributor.enable-otlp-metadata-storage=true`. #5693 #6035 #6254
* [ENHANCEMENT] Ingester: added support for sampling errors, which can be enabled by setting `-ingester.error-sample-rate`. This way each error will be logged once in the configured number of times. All the discarded samples will still be tracked by the `cortex_discarded_samples_total` metric. #5584 #6014
* [ENHANCEMENT] Ruler: Fetch secrets used to configure TLS on the Alertmanager client from Vault when `-vault.enabled` is true. #5239
* [ENHANCEMENT] Query-frontend: added query-sharding support for `group by` aggregation queries. #6024
* [ENHANCEMENT] Fetch secrets used to configure server-side TLS from Vault when `-vault.enabled` is true. #6052.
* [ENHANCEMENT] Packaging: add logrotate config file. #6142
* [ENHANCEMENT] Ingester: add the experimental configuration options `-blocks-storage.tsdb.head-postings-for-matchers-cache-max-bytes` and `-blocks-storage.tsdb.block-postings-for-matchers-cache-max-bytes` to enforce a limit in bytes on the `PostingsForMatchers()` cache used by ingesters (the cache limit is per TSDB head and block basis, not a global one). The experimental configuration options `-blocks-storage.tsdb.head-postings-for-matchers-cache-size` and `-blocks-storage.tsdb.block-postings-for-matchers-cache-size` have been deprecated. #6151
* [ENHANCEMENT] Ingester: use the `PostingsForMatchers()` in-memory cache for label values queries with matchers too. #6151
* [ENHANCEMENT] Ingester / store-gateway: optimized regex matchers. #6168 #6250
* [ENHANCEMENT] Distributor: Include ingester IDs in circuit breaker related metrics and logs. #6206
* [ENHANCEMENT] Querier: improve errors and logging when streaming chunks from ingesters and store-gateways. #6194 #6309
* [ENHANCEMENT] Querier: Add `cortex_querier_federation_exemplar_tenants_queried` and `cortex_querier_federation_tenants_queried` metrics to track the number of tenants queried by multi-tenant queries. #6374 #6409
* [ENHANCEMENT] All: added an experimental `-server.grpc.num-workers` flag that configures the number of long-living workers used to process gRPC requests. This could decrease the CPU usage by reducing the number of stack allocations. #6311
* [ENHANCEMENT] All: improved IPv6 support by using the proper host:port formatting. #6311
* [ENHANCEMENT] Querier: always return error encountered during chunks streaming, rather than `the stream has already been exhausted`. #6345 #6433
* [ENHANCEMENT] Query-frontend: add `instance_enable_ipv6` to support IPv6. #6111
* [ENHANCEMENT] Store-gateway: return same detailed error messages as queriers when chunks or series limits are reached. #6347
* [ENHANCEMENT] Querier: reduce memory consumed for queries that hit store-gateways. #6348
* [ENHANCEMENT] Ruler: include corresponding trace ID with log messages associated with rule evaluation. #6379 #6520
* [ENHANCEMENT] Querier: clarify log messages and span events emitted while querying ingesters, and include both ingester name and address when relevant. #6381
* [ENHANCEMENT] Memcached: introduce new experimental configuration parameters `-<prefix>.memcached.write-buffer-size-bytes` `-<prefix>.memcached.read-buffer-size-bytes` to customise the memcached client write and read buffer size (the buffer is allocated for each memcached connection). #6468
* [ENHANCEMENT] Ingester, Distributor: added experimental support for rejecting push requests received via gRPC before reading them into memory, if ingester or distributor is unable to accept the request. This is activated by using `-ingester.limit-inflight-requests-using-grpc-method-limiter` for ingester, and `-distributor.limit-inflight-requests-using-grpc-method-limiter` for distributor. #5976 #6300
* [ENHANCEMENT] Add capability in store-gateways to accept number of tokens through config. `-store-gateway.sharding-ring.num-tokens`, `default-value=512` #4863
* [ENHANCEMENT] Query-frontend: return warnings generated during query evaluation. #6391
* [ENHANCEMENT] Server: Add the option `-server.http-read-header-timeout` to enable specifying a timeout for reading HTTP request headers. It defaults to 0, in which case reading of headers can take up to `-server.http-read-timeout`, leaving no time for reading body, if there's any. #6517
* [ENHANCEMENT] Add connection-string option, `-<prefix>.azure.connection-string`, for Azure Blob Storage. #6487
* [ENHANCEMENT] Ingester: Add `-ingester.instance-limits.max-inflight-push-requests-bytes`. This limit protects the ingester against requests that together may cause an OOM. #6492
* [ENHANCEMENT] Ingester: add new per-tenant `cortex_ingester_local_limits` metric to expose the calculated local per-tenant limits seen at each ingester. Exports the local per-tenant series limit with label `{limit="max_global_series_per_user"}` #6403
* [ENHANCEMENT] Query-frontend: added "queue_time_seconds" field to "query stats" log. This is total time that query and subqueries spent in the queue, before queriers picked it up. #6537
* [ENHANCEMENT] Server: Add `-server.report-grpc-codes-in-instrumentation-label-enabled` CLI flag to specify whether gRPC status codes should be used in `status_code` label of `cortex_request_duration_seconds` metric. It defaults to false, meaning that successful and erroneous gRPC status codes are represented with `success` and `error` respectively. #6562
* [ENHANCEMENT] Server: Add `-ingester.client.report-grpc-codes-in-instrumentation-label-enabled` CLI flag to specify whether gRPC status codes should be used in `status_code` label of `cortex_ingester_client_request_duration_seconds` metric. It defaults to false, meaning that successful and erroneous gRPC status codes are represented with `2xx` and `error` respectively. #6562
* [ENHANCEMENT] Server: Add `-server.http-log-closed-connections-without-response-enabled` option to log details about connections to HTTP server that were closed before any data was sent back. This can happen if client doesn't manage to send complete HTTP headers before timeout. #6612
* [ENHANCEMENT] Query-frontend: include length of query, time since the earliest and latest points of a query, time since the earliest and latest points of a query, cached/uncached bytes in "query stats" logs. Time parameters (start/end/time) are always formatted as RFC3339 now. #6473 #6477 #6709 #6710
* [ENHANCEMENT] Query-frontend: `-query-frontend.align-queries-with-step` has been moved from a global flag to a per-tenant override. #6714
* [ENHANCEMENT] Distributor: added support for reducing the resolution of native histogram samples upon ingestion if the sample has too many buckets compared to `-validation.max-native-histogram-buckets`. This is enabled by default and can be turned off by setting `-validation.reduce-native-histogram-over-max-buckets` to `false`. #6535
* [ENHANCEMENT] Query-frontend: optionally wait for the frontend to complete startup if requests are received while the frontend is still starting. Disabled by default, set `-query-frontend.not-running-timeout` to a non-zero value to enable. #6621
* [ENHANCEMENT] Distributor: Include source IPs in OTLP push handler logs. #6652
* [ENHANCEMENT] Query-frontend: return clearer error message when a query request is received while shutting down. #6675
* [ENHANCEMENT] Querier: return clearer error message when a query request is cancelled by the caller. #6697
* [ENHANCEMENT] Compactor: Mark corrupted blocks for no-compaction to avoid blocking compactor future runs. #6588
* [ENHANCEMENT] Distributor: Added an experimental configuration option `distributor.ingestion-burst-factor` that overrides the `distributor.ingestion-burst-size` option if set. The `distributor.ingestion-burst-factor` is used to set the underlying ingestion rate limiter token bucket's burst size to a multiple of the per distributor `distributor.ingestion-rate-limit` and the `distributor.ingestion-burst-factor`. This is disabled by default. #6662
* [ENHANCEMENT] Add debug message to track tenants sending queries that are not able to benefit from caches. #6732
* [BUGFIX] Distributor: return server overload error in the event of exceeding the ingestion rate limit. #6549
* [BUGFIX] Ring: Ensure network addresses used for component hash rings are formatted correctly when using IPv6. #6068
* [BUGFIX] Query-scheduler: don't retain connections from queriers that have shut down, leading to gradually increasing enqueue latency over time. #6100 #6145
* [BUGFIX] Ingester: prevent query logic from continuing to execute after queries are canceled. #6085
* [BUGFIX] Ensure correct nesting of children of the `querier.Select` tracing span. #6085
* [BUGFIX] Packaging: fix preremove script preventing upgrades on RHEL based OS. #6067
* [BUGFIX] Querier: return actual error rather than `attempted to read series at index XXX from stream, but the stream has already been exhausted` (or even no error at all) when streaming chunks from ingesters or store-gateways is enabled and an error occurs while streaming chunks. #6346
* [BUGFIX] Querier: reduce log volume when querying ingesters with zone-awareness enabled and one or more instances in a single zone unavailable. #6381
* [BUGFIX] Querier: don't try to query further ingesters if ingester query request minimization is enabled and a query limit is reached as a result of the responses from the initial set of ingesters. #6402
* [BUGFIX] Ingester: Don't cache context cancellation error when querying. #6446
* [BUGFIX] Ingester: don't ignore errors encountered while iterating through chunks or samples in response to a query request. #6469
* [BUGFIX] All: fix issue where traces for some inter-component gRPC calls would incorrectly show the call as failing due to cancellation. #6470
* [BUGFIX] Querier: correctly mark streaming requests to ingesters or store-gateways as successful, not cancelled, in metrics and traces. #6471 #6505
* [BUGFIX] Querier: fix issue where queries fail with "context canceled" error when an ingester or store-gateway fails healthcheck while the query is in progress. #6550
* [BUGFIX] Tracing: When creating an OpenTelemetry tracing span, add it to the context for later retrieval. #6614
* [BUGFIX] Querier: always report query results to query-frontends, even when cancelled, to ensure query-frontends don't wait for results that will otherwise never arrive. #6703
* [BUGFIX] Querier: attempt to query ingesters in PENDING state, to reduce the likelihood that scaling up the number of ingesters in multiple zones simultaneously causes a read outage. #6726 #6727
* [BUGFIX] Querier: don't cancel inflight queries from a query-scheduler if the stream between the querier and query-scheduler is broken. #6728
* [BUGFIX] Store-gateway: Fix double-counting of some duration metrics. #6616
* [BUGFIX] Fixed possible series matcher corruption leading to wrong series being included in query results. #6884

### Mixin

* [CHANGE] Dashboards: enabled reporting gRPC codes as `status_code` label in Mimir dashboards. In case of gRPC calls, the successful `status_code` label on `cortex_request_duration_seconds` and gRPC client request duration metrics has changed from 'success' and '2xx' to 'OK'. #6561
* [CHANGE] Alerts: remove `MimirGossipMembersMismatch` alert and replace it with `MimirGossipMembersTooHigh` and `MimirGossipMembersTooLow` alerts that should have a higher signal-to-noise ratio. #6508
* [ENHANCEMENT] Dashboards: Optionally show rejected requests on Mimir Writes dashboard. Useful when used together with "early request rejection" in ingester and distributor. #6132 #6556
* [ENHANCEMENT] Alerts: added a critical alert for `CompactorSkippedBlocksWithOutOfOrderChunks` when multiple blocks are affected. #6410
* [ENHANCEMENT] Dashboards: Added the min-replicas for autoscaling dashboards. #6528
* [ENHANCEMENT] Dashboards: Show queries per second for the `/api/v1/cardinality/` endpoints on the "Overview" dashboard. #6720
* [BUGFIX] Alerts: fixed issue where `GossipMembersMismatch` warning message referred to per-instance labels that were not produced by the alert query. #6146
* [BUGFIX] Dashboards: Fix autoscaling dashboard panels for KEDA > 2.9. [Requires scraping the KEDA operator for metrics since they moved](https://github.com/kedacore/keda/issues/3972). #6528
* [BUGFIX] Alerts: Fix autoscaling alerts for KEDA > 2.9. [Requires scraping the KEDA operator for metrics since they moved](https://github.com/kedacore/keda/issues/3972). #6528

### Jsonnet

* [CHANGE] Ingester: reduce `-server.grpc-max-concurrent-streams` to 500. #5666
* [CHANGE] Changed default `_config.cluster_domain` from `cluster.local` to `cluster.local.` to reduce the number of DNS lookups made by Mimir. #6389
* [CHANGE] Query-frontend: changed default `_config.autoscaling_query_frontend_cpu_target_utilization` from `1` to `0.75`. #6395
* [CHANGE] Distributor: Increase HPA scale down period such that distributors are slower to scale down after autoscaling up. #6589
* [CHANGE] Store-gateway: Change the default timeout used for index-queries caches from `200ms` to `450ms`. #6786
* [FEATURE] Store-gateway: Allow automated zone-by-zone downscaling, that can be enabled via the `store_gateway_automated_downscale_enabled` flag. It is disabled by default. #6149
* [FEATURE] Ingester: Allow to configure TSDB Head early compaction using the following `_config` parameters: #6181
  * `ingester_tsdb_head_early_compaction_enabled` (disabled by default)
  * `ingester_tsdb_head_early_compaction_reduction_percentage`
  * `ingester_tsdb_head_early_compaction_min_in_memory_series`
* [ENHANCEMENT] Double the amount of rule groups for each user tier. #5897
* [ENHANCEMENT] Set `maxUnavailable` to 0 for `distributor`, `overrides-exporter`, `querier`, `query-frontend`, `query-scheduler` `ruler-querier`, `ruler-query-frontend`, `ruler-query-scheduler` and `consul` deployments, to ensure they don't become completely unavailable during a rollout. #5924
* [ENHANCEMENT] Update rollout-operator to `v0.9.0`. #6022 #6110 #6558 #6681
* [ENHANCEMENT] Update memcached to `memcached:1.6.22-alpine`. #6585
* [ENHANCEMENT] Store-gateway: replaced the following deprecated CLI flags: #6319
  * `-blocks-storage.bucket-store.index-header-lazy-loading-enabled` replaced with `-blocks-storage.bucket-store.index-header.lazy-loading-enabled`
  * `-blocks-storage.bucket-store.index-header-lazy-loading-idle-timeout` replaced with `-blocks-storage.bucket-store.index-header.lazy-loading-idle-timeout`
* [ENHANCEMENT] Store-gateway: Allow selective enablement of store-gateway automated scaling on a per-zone basis. #6302
* [BUGFIX] Autoscaling: KEDA > 2.9 removed the ability to set metricName in the trigger metadata. To help discern which metric is used by the HPA, we set the trigger name to what was the metricName. This is available as the `scaler` label on `keda_*` metrics. #6528

### Mimirtool

* [ENHANCEMENT] Analyze Grafana: Improve support for variables in range. #6657
* [BUGFIX] Fix out of bounds error on export with large timespans and/or series count. #5700
* [BUGFIX] Fix the issue where `--read-timeout` was applied to the entire `mimirtool analyze grafana` invocation rather than to individual Grafana API calls. #5915
* [BUGFIX] Fix incorrect remote-read path joining for `mimirtool remote-read` commands on Windows. #6011
* [BUGFIX] Fix template files full path being sent in `mimirtool alertmanager load` command. #6138
* [BUGFIX] Analyze rule-file: .metricsUsed field wasn't populated. #6953

### Mimir Continuous Test

### Query-tee

### Documentation

* [ENHANCEMENT] Document the concept of native histograms and how to send them to Mimir, migration path. #5956 #6488 #6539 #6752
* [ENHANCEMENT] Document native histograms query and visualization. #6231

### Tools

* [CHANGE] tsdb-index: Rename tool to tsdb-series. #6317
* [FEATURE] tsdb-labels: Add tool to print label names and values of a TSDB block. #6317
* [ENHANCEMENT] trafficdump: Trafficdump can now parse OTEL requests. Entire request is dumped to output, there's no filtering of fields or matching of series done. #6108

## 2.10.5

### Grafana Mimir

* [ENHANCEMENT] Update Docker base images from `alpine:3.18.3` to `alpine:3.18.5`. #6897
* [BUGFIX] Fixed possible series matcher corruption leading to wrong series being included in query results. #6886

### Documentation

* [ENHANCEMENT] Document the concept of native histograms and how to send them to Mimir, migration path. #6757
* [ENHANCEMENT] Document native histograms query and visualization. #6757

## 2.10.4

### Grafana Mimir

* [BUGFIX] Update otelhttp library to v0.44.0 as a mitigation for CVE-2023-45142. #6634

## 2.10.3

### Grafana Mimir

* [BUGFIX] Update grpc-go library to 1.57.2-dev that includes a fix for a bug introduced in 1.57.1. #6419

## 2.10.2

### Grafana Mimir

* [BUGFIX] Update grpc-go library to 1.57.1 and `golang.org/x/net` to `0.17`, which include fix for CVE-2023-44487. #6349

## 2.10.1

### Grafana Mimir

* [CHANGE] Update Go version to 1.21.3. #6244 #6325
* [BUGFIX] Query-frontend: Don't retry read requests rejected by the ingester due to utilization based read path limiting. #6032
* [BUGFIX] Ingester: fix panic in WAL replay of certain native histograms. #6086

## 2.10.0

### Grafana Mimir

* [CHANGE] Store-gateway: skip verifying index header integrity upon loading. To enable verification set `blocks_storage.bucket_store.index_header.verify_on_load: true`. #5174
* [CHANGE] Querier: change the default value of the experimental `-querier.streaming-chunks-per-ingester-buffer-size` flag to 256. #5203
* [CHANGE] Querier: only initiate query requests to ingesters in the `ACTIVE` state in the ring. #5342
* [CHANGE] Querier: renamed `-querier.prefer-streaming-chunks` to `-querier.prefer-streaming-chunks-from-ingesters` to enable streaming chunks from ingesters to queriers. #5182
* [CHANGE] Querier: `-query-frontend.cache-unaligned-requests` has been moved from a global flag to a per-tenant override. #5312
* [CHANGE] Ingester: removed `cortex_ingester_shipper_dir_syncs_total` and `cortex_ingester_shipper_dir_sync_failures_total` metrics. The former metric was not much useful, and the latter was never incremented. #5396
* [CHANGE] Ingester: removed logging of errors related to hitting per-instance limits to reduce resource usage when ingesters are under pressure. #5585
* [CHANGE] gRPC clients: use default connect timeout of 5s, and therefore enable default connect backoff max delay of 5s. #5562
* [CHANGE] Ingester: the `-validation.create-grace-period` is now enforced in the ingester too, other than distributor and query-frontend. If you've configured `-validation.create-grace-period` then make sure the configuration is applied to ingesters too. #5712
* [CHANGE] Distributor: the `-validation.create-grace-period` is now enforced for examplars too in the distributor. If an examplar has timestamp greater than "now + grace_period", then the exemplar will be dropped and the metric `cortex_discarded_exemplars_total{reason="exemplar_too_far_in_future",user="..."}` increased. #5761
* [CHANGE] Query-frontend: the `-validation.create-grace-period` is now enforced in the query-frontend even when the configured value is 0. When the value is 0, the query end time range is truncated to the current real-world time. #5829
* [CHANGE] Store-gateway: deprecated configuration parameters for index header under `blocks-storage.bucket-store` and use a new configurations in `blocks-storage.bucket-store.index-header`, deprecated configuration will be removed in Mimir 2.12. Configuration changes: #5726
  * `-blocks-storage.bucket-store.index-header-lazy-loading-enabled` is deprecated, use the new configuration `-blocks-storage.bucket-store.index-header.lazy-loading-enabled`
  * `-blocks-storage.bucket-store.index-header-lazy-loading-idle-timeout` is deprecated, use the new configuration `-blocks-storage.bucket-store.index-header.lazy-loading-idle-timeout`
  * `-blocks-storage.bucket-store.index-header-lazy-loading-concurrency` is deprecated, use the new configuration `-blocks-storage.bucket-store.index-header.lazy-loading-concurrency`
* [CHANGE] Store-gateway: remove experimental fine-grained chunks caching. The following experimental configuration parameters have been removed `-blocks-storage.bucket-store.chunks-cache.fine-grained-chunks-caching-enabled`, `-blocks-storage.bucket-store.fine-grained-chunks-caching-ranges-per-series`. #5816 #5875
* [CHANGE] Ingester: remove deprecated `blocks-storage.tsdb.max-tsdb-opening-concurrency-on-startup`. #5850
* [FEATURE] Introduced `-distributor.service-overload-status-code-on-rate-limit-enabled` flag for configuring status code to 529 instead of 429 upon rate limit exhaustion. #5752
* [FEATURE] Cardinality API: added a new `count_method` parameter which enables counting active series. #5136
* [FEATURE] Query-frontend: added experimental support to cache cardinality, label names and label values query responses. The cache will be used when `-query-frontend.cache-results` is enabled, and `-query-frontend.results-cache-ttl-for-cardinality-query` or `-query-frontend.results-cache-ttl-for-labels-query` set to a value greater than 0. The following metrics have been added to track the query results cache hit ratio per `request_type`: #5212 #5235 #5426 #5524
  * `cortex_frontend_query_result_cache_requests_total{request_type="query_range|cardinality|label_names_and_values"}`
  * `cortex_frontend_query_result_cache_hits_total{request_type="query_range|cardinality|label_names_and_values"}`
* [FEATURE] Added `-<prefix>.s3.list-objects-version` flag to configure the S3 list objects version. #5099
* [FEATURE] Ingester: add optional CPU/memory utilization based read request limiting, considered experimental. Disabled by default, enable by configuring limits via both of the following flags: #5012 #5392 #5394 #5526 #5508 #5704
  * `-ingester.read-path-cpu-utilization-limit`
  * `-ingester.read-path-memory-utilization-limit`
  * `-ingester.log-utilization-based-limiter-cpu-samples`
* [FEATURE] Ruler: support filtering results from rule status endpoint by `file`, `rule_group` and `rule_name`. #5291
* [FEATURE] Ingester: add experimental support for creating tokens by using spread minimizing strategy. This can be enabled with `-ingester.ring.token-generation-strategy: spread-minimizing` and `-ingester.ring.spread-minimizing-zones: <all available zones>`. In that case `-ingester.ring.tokens-file-path` must be empty. #5308 #5324
* [FEATURE] Storegateway: Persist sparse index-headers to disk and read from disk on index-header loads instead of reconstructing. #5465 #5651 #5726
* [FEATURE] Ingester: add experimental CLI flag `-ingester.ring.spread-minimizing-join-ring-in-order` that allows an ingester to register tokens in the ring only after all previous ingesters (with ID lower than its own ID) have already been registered. #5541
* [FEATURE] Ingester: add experimental support to compact the TSDB Head when the number of in-memory series is equal or greater than `-blocks-storage.tsdb.early-head-compaction-min-in-memory-series`, and the ingester estimates that the per-tenant TSDB Head compaction will reduce in-memory series by at least `-blocks-storage.tsdb.early-head-compaction-min-estimated-series-reduction-percentage`. #5371
* [FEATURE] Ingester: add new metrics for tracking native histograms in active series: `cortex_ingester_active_native_histogram_series`, `cortex_ingester_active_native_histogram_series_custom_tracker`, `cortex_ingester_active_native_histogram_buckets`, `cortex_ingester_active_native_histogram_buckets_custom_tracker`. The first 2 are the subsets of the existing and unmodified `cortex_ingester_active_series` and `cortex_ingester_active_series_custom_tracker` respectively, only tracking native histogram series, and the last 2 are the equivalents for tracking the number of buckets in native histogram series. #5318
* [FEATURE] Add experimental CLI flag `-<prefix>.s3.native-aws-auth-enabled` that allows to enable the default credentials provider chain of the AWS SDK. #5636
* [FEATURE] Distributor: add experimental support for circuit breaking when writing to ingesters via `-ingester.client.circuit-breaker.enabled`, `-ingester.client.circuit-breaker.failure-threshold`, or `-ingester.client.circuit-breaker.cooldown-period` or their corresponding YAML. #5650
* [FEATURE] The following features are no longer considered experimental. #5701 #5872
  * Ruler storage cache (`-ruler-storage.cache.*`)
  * Exclude ingesters running in specific zones (`-ingester.ring.excluded-zones`)
  * Cardinality-based query sharding (`-query-frontend.query-sharding-target-series-per-shard`)
  * Cardinality query result caching (`-query-frontend.results-cache-ttl-for-cardinality-query`)
  * Label names and values query result caching (`-query-frontend.results-cache-ttl-for-labels-query`)
  * Query expression size limit (`-query-frontend.max-query-expression-size-bytes`)
  * Peer discovery / tenant sharding for overrides exporters (`-overrides-exporter.ring.enabled`)
  * Configuring enabled metrics in overrides exporter (`-overrides-exporter.enabled-metrics`)
  * Per-tenant results cache TTL (`-query-frontend.results-cache-ttl`, `-query-frontend.results-cache-ttl-for-out-of-order-time-window`)
  * Shutdown delay (`-shutdown-delay`)
* [FEATURE] Querier: add experimental CLI flag `-tenant-federation.max-concurrent` to adjust the max number of per-tenant queries that can be run at a time when executing a single multi-tenant query. #5874
* [FEATURE] Alertmanager: add Microsoft Teams as a supported integration. #5840
* [ENHANCEMENT] Overrides-exporter: Add new metrics for write path and alertmanager (`max_global_metadata_per_user`, `max_global_metadata_per_metric`, `request_rate`, `request_burst_size`, `alertmanager_notification_rate_limit`, `alertmanager_max_dispatcher_aggregation_groups`, `alertmanager_max_alerts_count`, `alertmanager_max_alerts_size_bytes`) and added flag `-overrides-exporter.enabled-metrics` to explicitly configure desired metrics, e.g. `-overrides-exporter.enabled-metrics=request_rate,ingestion_rate`. Default value for this flag is: `ingestion_rate,ingestion_burst_size,max_global_series_per_user,max_global_series_per_metric,max_global_exemplars_per_user,max_fetched_chunks_per_query,max_fetched_series_per_query,ruler_max_rules_per_rule_group,ruler_max_rule_groups_per_tenant`. #5376
* [ENHANCEMENT] Cardinality API: when zone aware replication is enabled, the label values cardinality API can now tolerate single zone failure #5178
* [ENHANCEMENT] Distributor: optimize sending requests to ingesters when incoming requests don't need to be modified. For now this feature can be disabled by setting `-timeseries-unmarshal-caching-optimization-enabled=false`. #5137
* [ENHANCEMENT] Add advanced CLI flags to control gRPC client behaviour: #5161
  * `-<prefix>.connect-timeout`
  * `-<prefix>.connect-backoff-base-delay`
  * `-<prefix>.connect-backoff-max-delay`
  * `-<prefix>.initial-stream-window-size`
  * `-<prefix>.initial-connection-window-size`
* [ENHANCEMENT] Query-frontend: added "response_size_bytes" field to "query stats" log. #5196
* [ENHANCEMENT] Querier: refine error messages for per-tenant query limits, informing the user of the preferred strategy for not hitting the limit, in addition to how they may tweak the limit. #5059
* [ENHANCEMENT] Distributor: optimize sending of requests to ingesters by reusing memory buffers for marshalling requests. This optimization can be enabled by setting `-distributor.write-requests-buffer-pooling-enabled` to `true`. #5195 #5805 #5830
* [ENHANCEMENT] Querier: add experimental `-querier.minimize-ingester-requests` option to initially query only the minimum set of ingesters required to reach quorum. #5202 #5259 #5263
* [ENHANCEMENT] Querier: improve error message when streaming chunks from ingesters to queriers and a query limit is reached. #5245
* [ENHANCEMENT] Use new data structure for labels, to reduce memory consumption. #3555 #5731
* [ENHANCEMENT] Update alpine base image to 3.18.2. #5276
* [ENHANCEMENT] Ruler: add `cortex_ruler_sync_rules_duration_seconds` metric, tracking the time spent syncing all rule groups owned by the ruler instance. #5311
* [ENHANCEMENT] Store-gateway: add experimental `blocks-storage.bucket-store.index-header-lazy-loading-concurrency` config option to limit the number of concurrent index-headers loads when lazy loading. #5313 #5605
* [ENHANCEMENT] Ingester and querier: improve level of detail in traces emitted for queries that hit ingesters. #5315
* [ENHANCEMENT] Querier: add `cortex_querier_queries_rejected_total` metric that counts the number of queries rejected due to hitting a limit (eg. max series per query or max chunks per query). #5316 #5440 #5450
* [ENHANCEMENT] Querier: add experimental `-querier.minimize-ingester-requests-hedging-delay` option to initiate requests to further ingesters when request minimisation is enabled and not all initial requests have completed. #5368
* [ENHANCEMENT] Clarify docs for `-ingester.client.*` flags to make it clear that these are used by both queriers and distributors. #5375
* [ENHANCEMENT] Querier and store-gateway: add experimental support for streaming chunks from store-gateways to queriers while evaluating queries. This can be enabled with `-querier.prefer-streaming-chunks-from-store-gateways=true`. #5182
* [ENHANCEMENT] Querier: enforce `max-chunks-per-query` limit earlier in query processing when streaming chunks from ingesters to queriers to avoid unnecessarily consuming resources for queries that will be aborted. #5369 #5447
* [ENHANCEMENT] Ingester: added `cortex_ingester_shipper_last_successful_upload_timestamp_seconds` metric tracking the last successful TSDB block uploaded to the bucket (unix timestamp in seconds). #5396
* [ENHANCEMENT] Ingester: add two metrics tracking resource utilization calculated by utilization based limiter: #5496
  * `cortex_ingester_utilization_limiter_current_cpu_load`: The current exponential weighted moving average of the ingester's CPU load
  * `cortex_ingester_utilization_limiter_current_memory_usage_bytes`: The current ingester memory utilization
* [ENHANCEMENT] Ruler: added `insight=true` field to ruler's prometheus component for rule evaluation logs. #5510
* [ENHANCEMENT] Distributor Ingester: add metrics to count the number of requests rejected for hitting per-instance limits, `cortex_distributor_instance_rejected_requests_total` and `cortex_ingester_instance_rejected_requests_total` respectively. #5551
* [ENHANCEMENT] Distributor: add support for ingesting exponential histograms that are over the native histogram scale limit of 8 in OpenTelemetry format by downscaling them. #5532 #5607
* [ENHANCEMENT] General: buffered logging: #5506
  * `-log.buffered` CLI flag enable buffered logging.
* [ENHANCEMENT] Distributor: add more detailed information to traces generated while processing OTLP write requests. #5539
* [ENHANCEMENT] Distributor: improve performance ingesting OTLP payloads. #5531 #5607 #5616
* [ENHANCEMENT] Ingester: optimize label-values with matchers call when number of matched series is small. #5600
* [ENHANCEMENT] Compactor: delete bucket-index, markers and debug files if there are no blocks left in the bucket index. This cleanup must be enabled by using `-compactor.no-blocks-file-cleanup-enabled` option. #5648
* [ENHANCEMENT] Ingester: reduce memory usage of active series tracker. #5665
* [ENHANCEMENT] Store-gateway: added `-store-gateway.sharding-ring.auto-forget-enabled` configuration parameter to control whether store-gateway auto-forget feature should be enabled or disabled (enabled by default). #5702
* [ENHANCEMENT] Compactor: added per tenant block upload counters `cortex_block_upload_api_blocks_total`, `cortex_block_upload_api_bytes_total`, and `cortex_block_upload_api_files_total`. #5738
* [ENHANCEMENT] Compactor: verify time range of compacted block(s) matches the time range of input blocks. #5760
* [ENHANCEMENT] Querier: improved observability of calls to ingesters during queries. #5724
* [ENHANCEMENT] Compactor: block backfilling logging is now more verbose. #5711
* [ENHANCEMENT] Added support to rate limit application logs: #5764
  * `-log.rate-limit-enabled`
  * `-log.rate-limit-logs-per-second`
  * `-log.rate-limit-logs-per-second-burst`
* [ENHANCEMENT] Ingester: added `cortex_ingester_tsdb_head_min_timestamp_seconds` and `cortex_ingester_tsdb_head_max_timestamp_seconds` metrics which return min and max time of all TSDB Heads open in an ingester. #5786 #5815
* [ENHANCEMENT] Querier: cancel query requests to ingesters in a zone upon first error received from the zone, to reduce wasted effort spent computing results that won't be used #5764
* [ENHANCEMENT] All: improve tracing of internal HTTP requests sent over httpgrpc. #5782
* [ENHANCEMENT] Querier: add experimental per-query chunks limit based on an estimate of the number of chunks that will be sent from ingesters and store-gateways that is enforced earlier during query evaluation. This limit is disabled by default and can be configured with `-querier.max-estimated-fetched-chunks-per-query-multiplier`. #5765
* [ENHANCEMENT] Ingester: add UI for listing tenants with TSDB on given ingester and viewing details of tenants's TSDB on given ingester. #5803 #5824
* [ENHANCEMENT] Querier: improve observability of calls to store-gateways during queries. #5809
* [ENHANCEMENT] Query-frontend: improve tracing of interactions with query-scheduler. #5818
* [ENHANCEMENT] Query-scheduler: improve tracing of requests when request is rejected by query-scheduler. #5848
* [ENHANCEMENT] Ingester: avoid logging some errors that could cause logging contention. #5494 #5581
* [ENHANCEMENT] Store-gateway: wait for query gate after loading blocks. #5507
* [ENHANCEMENT] Store-gateway: always include `__name__` posting group in selection in order to reduce the number of object storage API calls. #5246
* [ENHANCEMENT] Ingester: track active series by ref instead of hash/labels to reduce memory usage. #5134 #5193
* [ENHANCEMENT] Go: updated to 1.21.1. #5955 #5960
* [ENHANCEMENT] Alertmanager: updated to alertmanager 0.26.0. #5840
* [BUGFIX] Ingester: Handle when previous ring state is leaving and the number of tokens has changed. #5204
* [BUGFIX] Querier: fix issue where queries that use the `timestamp()` function fail with `execution: attempted to read series at index 0 from stream, but the stream has already been exhausted` if streaming chunks from ingesters to queriers is enabled. #5370
* [BUGFIX] memberlist: bring back `memberlist_client_kv_store_count` metric that used to exist in Cortex, but got lost during dskit updates before Mimir 2.0. #5377
* [BUGFIX] Querier: pass on HTTP 503 query response code. #5364
* [BUGFIX] Store-gateway: Fix issue where stopping a store-gateway could cause all store-gateways to unload all blocks. #5464
* [BUGFIX] Allocate ballast in smaller blocks to avoid problem when entire ballast was kept in memory working set. #5565
* [BUGFIX] Querier: retry frontend result notification when an error is returned. #5591
* [BUGFIX] Querier: fix issue where `cortex_ingester_client_request_duration_seconds` metric did not include streaming query requests that did not return any series. #5695
* [BUGFIX] Ingester: fix ActiveSeries tracker double-counting series that have been deleted from the Head while still being active and then recreated again. #5678
* [BUGFIX] Ingester: don't set "last update time" of TSDB into the future when opening TSDB. This could prevent detecting of idle TSDB for a long time, if sample in distant future was ingested. #5787
* [BUGFIX] Store-gateway: fix bug when lazy index header could be closed prematurely even when still in use. #5795
* [BUGFIX] Ruler: gracefully shut down rule evaluations. #5778
* [BUGFIX] Querier: fix performance when ingesters stream samples. #5836
* [BUGFIX] Ingester: fix spurious `not found` errors on label values API during head compaction. #5957
* [BUGFIX] All: updated Minio object storage client from 7.0.62 to 7.0.63 to fix auto-detection of AWS GovCloud environments. #5905

### Mixin

* [CHANGE] Dashboards: show all workloads in selected namespace on "rollout progress" dashboard. #5113
* [CHANGE] Dashboards: show the number of updated and ready pods for each workload in the "rollout progress" panel on the "rollout progress" dashboard. #5113
* [CHANGE] Dashboards: removed "Query results cache misses" panel on the "Mimir / Queries" dashboard. #5423
* [CHANGE] Dashboards: default to shared crosshair on all dashboards. #5489
* [CHANGE] Dashboards: sort variable drop-down lists from A to Z, rather than Z to A. #5490
* [CHANGE] Alerts: removed `MimirProvisioningTooManyActiveSeries` alert. You should configure `-ingester.instance-limits.max-series` and rely on `MimirIngesterReachingSeriesLimit` alert instead. #5593
* [CHANGE] Alerts: removed `MimirProvisioningTooManyWrites` alert. The alerting threshold used in this alert was chosen arbitrarily and ingesters receiving an higher number of samples / sec don't necessarily have any issue. You should rely on SLOs metrics and alerts instead. #5706
* [CHANGE] Alerts: don't raise `MimirRequestErrors` or `MimirRequestLatency` alert for the `/debug/pprof` endpoint. #5826
* [ENHANCEMENT] Dashboards: adjust layout of "rollout progress" dashboard panels so that the "rollout progress" panel doesn't require scrolling. #5113
* [ENHANCEMENT] Dashboards: show container name first in "pods count per version" panel on "rollout progress" dashboard. #5113
* [ENHANCEMENT] Dashboards: show time spend waiting for turn when lazy loading index headers in the "index-header lazy load gate latency" panel on the "queries" dashboard. #5313
* [ENHANCEMENT] Dashboards: split query results cache hit ratio by request type in "Query results cache hit ratio" panel on the "Mimir / Queries" dashboard. #5423
* [ENHANCEMENT] Dashboards: add "rejected queries" panel to "queries" dashboard. #5429
* [ENHANCEMENT] Dashboards: add native histogram active series and active buckets to "tenants" dashboard. #5543
* [ENHANCEMENT] Dashboards: add panels to "Mimir / Writes" for requests rejected for per-instance limits. #5638
* [ENHANCEMENT] Dashboards: rename "Blocks currently loaded" to "Blocks currently owned" in the "Mimir / Queries" dashboard. #5705
* [ENHANCEMENT] Alerts: Add `MimirIngestedDataTooFarInTheFuture` warning alert that triggers when Mimir ingests sample with timestamp more than 1h in the future. #5822
* [BUGFIX] Alerts: fix `MimirIngesterRestarts` to fire only when the ingester container is restarted, excluding the cases the pod is rescheduled. #5397
* [BUGFIX] Dashboards: fix "unhealthy pods" panel on "rollout progress" dashboard showing only a number rather than the name of the workload and the number of unhealthy pods if only one workload has unhealthy pods. #5113 #5200
* [BUGFIX] Alerts: fixed `MimirIngesterHasNotShippedBlocks` and `MimirIngesterHasNotShippedBlocksSinceStart` alerts. #5396
* [BUGFIX] Alerts: Fix `MimirGossipMembersMismatch` to include `admin-api` and custom compactor pods. `admin-api` is a GEM component. #5641 #5797
* [BUGFIX] Dashboards: fix autoscaling dashboard panels that could show multiple series for a single component. #5810
* [BUGFIX] Dashboards: fix ruler-querier scaling metric panel query and split into CPU and memory scaling metric panels. #5739

### Jsonnet

* [CHANGE] Removed `_config.querier.concurrency` configuration option and replaced it with `_config.querier_max_concurrency` and `_config.ruler_querier_max_concurrency` to allow to easily fine tune it for different querier deployments. #5322
* [CHANGE] Change `_config.multi_zone_ingester_max_unavailable` to 50. #5327
* [CHANGE] Change distributors rolling update strategy configuration: `maxSurge` and `maxUnavailable` are set to `15%` and `0`. #5714
* [FEATURE] Alertmanager: Add horizontal pod autoscaler config, that can be enabled using `autoscaling_alertmanager_enabled: true`. #5194 #5249
* [ENHANCEMENT] Enable the `track_sizes` feature for Memcached pods to help determine cache efficiency. #5209
* [ENHANCEMENT] Add per-container map for environment variables. #5181
* [ENHANCEMENT] Add `PodDisruptionBudget`s for compactor, continuous-test, distributor, overrides-exporter, querier, query-frontend, query-scheduler, rollout-operator, ruler, ruler-querier, ruler-query-frontend, ruler-query-scheduler, and all memcached workloads. #5098
* [ENHANCEMENT] Ruler: configure the ruler storage cache when the metadata cache is enabled. #5326 #5334
* [ENHANCEMENT] Shuffle-sharding: ingester shards in user-classes can now be configured to target different series and limit percentage utilization through `_config.shuffle_sharding.target_series_per_ingester` and `_config.shuffle_sharding.target_utilization_percentage` values. #5470
* [ENHANCEMENT] Distributor: allow adjustment of the targeted CPU usage as a percentage of requested CPU. This can be adjusted with `_config.autoscaling_distributor_cpu_target_utilization`. #5525
* [ENHANCEMENT] Ruler: add configuration option `_config.ruler_remote_evaluation_max_query_response_size_bytes` to easily set the maximum query response size allowed (in bytes). #5592
* [ENHANCEMENT] Distributor: dynamically set `GOMAXPROCS` based on the CPU request. This should reduce distributor CPU utilization, assuming the CPU request is set to a value close to the actual utilization. #5588
* [ENHANCEMENT] Querier: dynamically set `GOMAXPROCS` based on the CPU request. This should reduce noisy neighbour issues created by the querier, whose CPU utilization could eventually saturate the Kubernetes node if unbounded. #5646 #5658
* [ENHANCEMENT] Allow to remove an entry from the configured environment variable for a given component, setting the environment value to `null` in the `*_env_map` objects (e.g. `store_gateway_env_map+:: { 'field': null}`). #5599
* [ENHANCEMENT] Allow overriding the default number of replicas for `etcd`. #5589
* [ENHANCEMENT] Memcached: reduce memory request for results, chunks and metadata caches. The requested memory is 5% greater than the configured memcached max cache size. #5661
* [ENHANCEMENT] Autoscaling: Add the following configuration options to fine tune autoscaler target utilization: #5679 #5682 #5689
  * `autoscaling_querier_target_utilization` (defaults to `0.75`)
  * `autoscaling_mimir_read_target_utilization` (defaults to `0.75`)
  * `autoscaling_ruler_querier_cpu_target_utilization` (defaults to `1`)
  * `autoscaling_distributor_memory_target_utilization` (defaults to `1`)
  * `autoscaling_ruler_cpu_target_utilization` (defaults to `1`)
  * `autoscaling_query_frontend_cpu_target_utilization` (defaults to `1`)
  * `autoscaling_ruler_query_frontend_cpu_target_utilization` (defaults to `1`)
  * `autoscaling_alertmanager_cpu_target_utilization` (defaults to `1`)
* [ENHANCEMENT] Gossip-ring: add appProtocol for istio compatibility. #5680
* [ENHANCEMENT] Add _config.commonConfig to allow adding common configuration parameters for all Mimir components. #5703
* [ENHANCEMENT] Update rollout-operator to `v0.7.0`. #5718
* [ENHANCEMENT] Increase the default rollout speed for store-gateway when lazy loading is disabled. #5823
* [ENHANCEMENT] Add autoscaling on memory for ruler-queriers. #5739
* [ENHANCEMENT] Deduplicate scaled object creation for most objects that scale on CPU and memory. #6411
* [BUGFIX] Fix compilation when index, chunks or metadata caches are disabled. #5710
* [BUGFIX] Autoscaling: treat OOMing containers as though they are using their full memory request. #5739
* [BUGFIX] Autoscaling: if no containers are up, report 0 memory usage instead of no data. #6411

### Mimirtool

* [ENHANCEMENT] Mimirtool uses paging to fetch all dashboards from Grafana when running `mimirtool analyse grafana`. This allows the tool to work correctly when running against Grafana instances with more than a 1000 dashboards. #5825
* [ENHANCEMENT] Extract metric name from queries that have a `__name__` matcher. #5911
* [BUGFIX] Mimirtool no longer parses label names as metric names when handling templating variables that are populated using `label_values(<label_name>)` when running `mimirtool analyse grafana`. #5832
* [BUGFIX] Fix panic when analyzing a grafana dashboard with multiline queries in templating variables. #5911

### Query-tee

* [CHANGE] Proxy `Content-Type` response header from backend. Previously `Content-Type: text/plain; charset=utf-8` was returned on all requests. #5183
* [CHANGE] Increase default value of `-proxy.compare-skip-recent-samples` to avoid racing with recording rule evaluation. #5561
* [CHANGE] Add `-backend.skip-tls-verify` to optionally skip TLS verification on backends. #5656

### Documentation

* [CHANGE] Fix reference to `get-started` documentation directory. #5476
* [CHANGE] Fix link to external OTLP/HTTP documentation.
* [ENHANCEMENT] Improved `MimirRulerTooManyFailedQueries` runbook. #5586
* [ENHANCEMENT] Improved "Recover accidentally deleted blocks" runbook. #5620
* [ENHANCEMENT] Documented options and trade-offs to query label names and values. #5582
* [ENHANCEMENT] Improved `MimirRequestErrors` runbook for alertmanager. #5694

### Tools

* [CHANGE] copyblocks: add support for S3 and the ability to copy between different object storage services. Due to this, the `-source-service` and `-destination-service` flags are now required and the `-service` flag has been removed. #5486
* [FEATURE] undelete-block-gcs: Added new tool for undeleting blocks on GCS storage. #5610 #5855
* [FEATURE] wal-reader: Added new tool for printing entries in TSDB WAL. #5780
* [ENHANCEMENT] ulidtime: add -seconds flag to print timestamps as Unix timestamps. #5621
* [ENHANCEMENT] ulidtime: exit with status code 1 if some ULIDs can't be parsed. #5621
* [ENHANCEMENT] tsdb-index-toc: added index-header size estimates. #5652
* [BUGFIX] Stop tools from panicking when `-help` flag is passed. #5412
* [BUGFIX] Remove github.com/golang/glog command line flags from tools. #5413

## 2.9.4

### Grafana Mimir

* [ENHANCEMENT] Update Docker base images from `alpine:3.18.3` to `alpine:3.18.5`. #6895

## 2.9.3

### Grafana Mimir

* [BUGFIX] Update `go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp` to `0.44` which includes a fix for CVE-2023-45142. #6637

## 2.9.2

### Grafana Mimir

* [BUGFIX] Update grpc-go library to 1.56.3 and `golang.org/x/net` to `0.17`, which include fix for CVE-2023-44487. #6353 #6364

## 2.9.1

### Grafana Mimir

* [ENHANCEMENT] Update alpine base image to 3.18.3. #6021

## 2.9.0

### Grafana Mimir

* [CHANGE] Store-gateway: change expanded postings, postings, and label values index cache key format. These caches will be invalidated when rolling out the new Mimir version. #4770 #4978 #5037
* [CHANGE] Distributor: remove the "forwarding" feature as it isn't necessary anymore. #4876
* [CHANGE] Query-frontend: Change the default value of `-query-frontend.query-sharding-max-regexp-size-bytes` from `0` to `4096`. #4932
* [CHANGE] Querier: `-querier.query-ingesters-within` has been moved from a global flag to a per-tenant override. #4287
* [CHANGE] Querier: Use `-blocks-storage.tsdb.retention-period` instead of `-querier.query-ingesters-within` for calculating the lookback period for shuffle sharded ingesters. Setting `-querier.query-ingesters-within=0` no longer disables shuffle sharding on the read path. #4287
* [CHANGE] Block upload: `/api/v1/upload/block/{block}/files` endpoint now allows file uploads with no `Content-Length`. #4956
* [CHANGE] Store-gateway: deprecate configuration parameters for chunk pooling, they will be removed in Mimir 2.11. The following options are now also ignored: #4996
  * `-blocks-storage.bucket-store.max-chunk-pool-bytes`
  * `-blocks-storage.bucket-store.chunk-pool-min-bucket-size-bytes`
  * `-blocks-storage.bucket-store.chunk-pool-max-bucket-size-bytes`
* [CHANGE] Store-gateway: remove metrics `cortex_bucket_store_chunk_pool_requested_bytes_total` and `cortex_bucket_store_chunk_pool_returned_bytes_total`. #4996
* [CHANGE] Compactor: change default of `-compactor.partial-block-deletion-delay` to `1d`. This will automatically clean up partial blocks that were a result of failed block upload or deletion. #5026
* [CHANGE] Compactor: the deprecated configuration parameter `-compactor.consistency-delay` has been removed. #5050
* [CHANGE] Store-gateway: the deprecated configuration parameter `-blocks-storage.bucket-store.consistency-delay` has been removed. #5050
* [CHANGE] The configuration parameter `-blocks-storage.bucket-store.bucket-index.enabled` has been deprecated and will be removed in Mimir 2.11. Mimir is running by default with the bucket index enabled since version 2.0, and starting from the version 2.11 it will not be possible to disable it. #5051
* [CHANGE] The configuration parameters `-querier.iterators` and `-query.batch-iterators` have been deprecated and will be removed in Mimir 2.11. Mimir runs by default with `-querier.batch-iterators=true`, and starting from version 2.11 it will not be possible to change this. #5114
* [CHANGE] Compactor: change default of `-compactor.first-level-compaction-wait-period` to 25m. #5128
* [CHANGE] Ruler: changed default of `-ruler.poll-interval` from `1m` to `10m`. Starting from this release, the configured rule groups will also be re-synced each time they're modified calling the ruler configuration API. #5170
* [FEATURE] Query-frontend: add `-query-frontend.log-query-request-headers` to enable logging of request headers in query logs. #5030
* [FEATURE] Store-gateway: add experimental feature to retain lazy-loaded index headers between restarts by eagerly loading them during startup. This is disabled by default and can only be enabled if lazy loading is enabled. To enable this set the following: #5606
  * `-blocks-storage.bucket-store.index-header-lazy-loading-enabled` must be set to true
  * `-blocks-storage.bucket-store.index-header.eager-loading-startup-enabled` must be set to true
* [ENHANCEMENT] Add per-tenant limit `-validation.max-native-histogram-buckets` to be able to ignore native histogram samples that have too many buckets. #4765
* [ENHANCEMENT] Store-gateway: reduce memory usage in some LabelValues calls. #4789
* [ENHANCEMENT] Store-gateway: add a `stage` label to the metric `cortex_bucket_store_series_data_touched`. This label now applies to `data_type="chunks"` and `data_type="series"`. The `stage` label has 2 values: `processed` - the number of series that parsed - and `returned` - the number of series selected from the processed bytes to satisfy the query. #4797 #4830
* [ENHANCEMENT] Distributor: make `__meta_tenant_id` label available in relabeling rules configured via `metric_relabel_configs`. #4725
* [ENHANCEMENT] Compactor: added the configurable limit `compactor.block-upload-max-block-size-bytes` or `compactor_block_upload_max_block_size_bytes` to limit the byte size of uploaded or validated blocks. #4680
* [ENHANCEMENT] Querier: reduce CPU utilisation when shuffle sharding is enabled with large shard sizes. #4851
* [ENHANCEMENT] Packaging: facilitate configuration management by instructing systemd to start mimir with a configuration file. #4810
* [ENHANCEMENT] Store-gateway: reduce memory allocations when looking up postings from cache. #4861 #4869 #4962 #5047
* [ENHANCEMENT] Store-gateway: retain only necessary bytes when reading series from the bucket. #4926
* [ENHANCEMENT] Ingester, store-gateway: clear the shutdown marker after a successful shutdown to enable reusing their persistent volumes in case the ingester or store-gateway is restarted. #4985
* [ENHANCEMENT] Store-gateway, query-frontend: Reduced memory allocations when looking up cached entries from Memcached. #4862
* [ENHANCEMENT] Alertmanager: Add additional template function `queryFromGeneratorURL` returning query URL decoded query from the `GeneratorURL` field of an alert. #4301
* [ENHANCEMENT] Ruler: added experimental ruler storage cache support. The cache should reduce the number of "list objects" API calls issued to the object storage when there are 2+ ruler replicas running in a Mimir cluster. The cache can be configured setting `-ruler-storage.cache.*` CLI flags or their respective YAML config options. #4950 #5054
* [ENHANCEMENT] Store-gateway: added HTTP `/store-gateway/prepare-shutdown` endpoint for gracefully scaling down of store-gateways. A gauge `cortex_store_gateway_prepare_shutdown_requested` has been introduced for tracing this process. #4955
* [ENHANCEMENT] Updated Kuberesolver dependency (github.com/sercand/kuberesolver) from v2.4.0 to v4.0.0 and gRPC dependency (google.golang.org/grpc) from v1.47.0 to v1.53.0. #4922
* [ENHANCEMENT] Introduced new options for logging HTTP request headers: `-server.log-request-headers` enables logging HTTP request headers, `-server.log-request-headers-exclude-list` lists headers which should not be logged. #4922
* [ENHANCEMENT] Block upload: `/api/v1/upload/block/{block}/files` endpoint now disables read and write HTTP timeout, overriding `-server.http-read-timeout` and `-server.http-write-timeout` values. This is done to allow large file uploads to succeed. #4956
* [ENHANCEMENT] Alertmanager: Introduce new metrics from upstream. #4918
  * `cortex_alertmanager_notifications_failed_total` (added `reason` label)
  * `cortex_alertmanager_nflog_maintenance_total`
  * `cortex_alertmanager_nflog_maintenance_errors_total`
  * `cortex_alertmanager_silences_maintenance_total`
  * `cortex_alertmanager_silences_maintenance_errors_total`
* [ENHANCEMENT] Add native histogram support for `cortex_request_duration_seconds` metric family. #4987
* [ENHANCEMENT] Ruler: do not list rule groups in the object storage for disabled tenants. #5004
* [ENHANCEMENT] Query-frontend and querier: add HTTP API endpoint `<prometheus-http-prefix>/api/v1/format_query` to format a PromQL query. #4373
* [ENHANCEMENT] Query-frontend: Add `cortex_query_frontend_regexp_matcher_count` and `cortex_query_frontend_regexp_matcher_optimized_count` metrics to track optimization of regular expression label matchers. #4813
* [ENHANCEMENT] Alertmanager: Add configuration option to enable or disable the deletion of alertmanager state from object storage. This is useful when migrating alertmanager tenants from one cluster to another, because it avoids a condition where the state object is copied but then deleted before the configuration object is copied. #4989
* [ENHANCEMENT] Querier: only use the minimum set of chunks from ingesters when querying, and cancel unnecessary requests to ingesters sooner if we know their results won't be used. #5016
* [ENHANCEMENT] Add `-enable-go-runtime-metrics` flag to expose all go runtime metrics as Prometheus metrics. #5009
* [ENHANCEMENT] Ruler: trigger a synchronization of tenant's rule groups as soon as they change the rules configuration via API. This synchronization is in addition of the periodic syncing done every `-ruler.poll-interval`. The new behavior is enabled by default, but can be disabled with `-ruler.sync-rules-on-changes-enabled=false` (configurable on a per-tenant basis too). If you disable the new behaviour, then you may want to revert `-ruler.poll-interval` to `1m`. #4975 #5053 #5115 #5170
* [ENHANCEMENT] Distributor: Improve invalid tenant shard size error message. #5024
* [ENHANCEMENT] Store-gateway: record index header loading time separately in `cortex_bucket_store_series_request_stage_duration_seconds{stage="load_index_header"}`. Now index header loading will be visible in the "Mimir / Queries" dashboard in the "Series request p99/average latency" panels. #5011 #5062
* [ENHANCEMENT] Querier and ingester: add experimental support for streaming chunks from ingesters to queriers while evaluating queries. This can be enabled with `-querier.prefer-streaming-chunks=true`. #4886 #5078 #5094 #5126
* [ENHANCEMENT] Update Docker base images from `alpine:3.17.3` to `alpine:3.18.0`. #5065
* [ENHANCEMENT] Compactor: reduced the number of "object exists" API calls issued by the compactor to the object storage when syncing block's `meta.json` files. #5063
* [ENHANCEMENT] Distributor: Push request rate limits (`-distributor.request-rate-limit` and `-distributor.request-burst-size`) and their associated YAML configuration are now stable. #5124
* [ENHANCEMENT] Go: updated to 1.20.5. #5185
* [ENHANCEMENT] Update alpine base image to 3.18.2. #5274 #5276
* [BUGFIX] Metadata API: Mimir will now return an empty object when no metadata is available, matching Prometheus. #4782
* [BUGFIX] Store-gateway: add collision detection on expanded postings and individual postings cache keys. #4770
* [BUGFIX] Ruler: Support the `type=alert|record` query parameter for the API endpoint `<prometheus-http-prefix>/api/v1/rules`. #4302
* [BUGFIX] Backend: Check that alertmanager's data-dir doesn't overlap with bucket-sync dir. #4921
* [BUGFIX] Alertmanager: Allow to rate-limit webex, telegram and discord notifications. #4979
* [BUGFIX] Store-gateway: panics when decoding LabelValues responses that contain more than 655360 values. These responses are no longer cached. #5021
* [BUGFIX] Querier: don't leak memory when processing query requests from query-frontends (ie. when the query-scheduler is disabled). #5199

### Documentation

* [ENHANCEMENT] Improve `MimirIngesterReachingTenantsLimit` runbook. #4744 #4752
* [ENHANCEMENT] Add `symbol table size exceeds` case to `MimirCompactorHasNotSuccessfullyRunCompaction` runbook. #4945
* [ENHANCEMENT] Clarify which APIs use query sharding. #4948

### Mixin

* [CHANGE] Alerts: Remove `MimirQuerierHighRefetchRate`. #4980
* [CHANGE] Alerts: Remove `MimirTenantHasPartialBlocks`. This is obsoleted by the changed default of `-compactor.partial-block-deletion-delay` to `1d`, which will auto remediate this alert. #5026
* [ENHANCEMENT] Alertmanager dashboard: display active aggregation groups #4772
* [ENHANCEMENT] Alerts: `MimirIngesterTSDBWALCorrupted` now only fires when there are more than one corrupted WALs in single-zone deployments and when there are more than two zones affected in multi-zone deployments. #4920
* [ENHANCEMENT] Alerts: added labels to duplicated `MimirRolloutStuck` and `MimirCompactorHasNotUploadedBlocks` rules in order to distinguish them. #5023
* [ENHANCEMENT] Dashboards: fix holes in graph for lightly loaded clusters #4915
* [ENHANCEMENT] Dashboards: allow configuring additional services for the Rollout Progress dashboard. #5007
* [ENHANCEMENT] Alerts: do not fire `MimirAllocatingTooMuchMemory` alert for any matching container outside of namespaces where Mimir is running. #5089
* [BUGFIX] Dashboards: show cancelled requests in a different color to successful requests in throughput panels on dashboards. #5039
* [BUGFIX] Dashboards: fix dashboard panels that showed percentages with axes from 0 to 10000%. #5084
* [BUGFIX] Remove dependency on upstream Kubernetes mixin. #4732

### Jsonnet

* [CHANGE] Ruler: changed ruler autoscaling policy, extended scale down period from 60s to 600s. #4786
* [CHANGE] Update to v0.5.0 rollout-operator. #4893
* [CHANGE] Backend: add `alertmanager_args` to `mimir-backend` when running in read-write deployment mode. Remove hardcoded `filesystem` alertmanager storage. This moves alertmanager's data-dir to `/data/alertmanager` by default. #4907 #4921
* [CHANGE] Remove `-pdb` suffix from `PodDisruptionBudget` names. This will create new `PodDisruptionBudget` resources. Make sure to prune the old resources; otherwise, rollouts will be blocked. #5109
* [CHANGE] Query-frontend: enable query sharding for cardinality estimation via `-query-frontend.query-sharding-target-series-per-shard` by default if the results cache is enabled. #5128
* [ENHANCEMENT] Ingester: configure `-blocks-storage.tsdb.head-compaction-interval=15m` to spread TSDB head compaction over a wider time range. #4870
* [ENHANCEMENT] Ingester: configure `-blocks-storage.tsdb.wal-replay-concurrency` to CPU request minus 1. #4864
* [ENHANCEMENT] Compactor: configure `-compactor.first-level-compaction-wait-period` to TSDB head compaction interval plus 10 minutes. #4872
* [ENHANCEMENT] Store-gateway: set `GOMEMLIMIT` to the memory request value. This should reduce the likelihood the store-gateway may go out of memory, at the cost of an higher CPU utilization due to more frequent garbage collections when the memory utilization gets closer or above the configured requested memory. #4971
* [ENHANCEMENT] Store-gateway: dynamically set `GOMAXPROCS` based on the CPU request. This should reduce the likelihood a high load on the store-gateway will slow down the entire Kubernetes node. #5104
* [ENHANCEMENT] Store-gateway: add `store_gateway_lazy_loading_enabled` configuration option which combines disabled lazy-loading and reducing blocks sync concurrency. Reducing blocks sync concurrency improves startup times with disabled lazy loading on HDDs. #5025
* [ENHANCEMENT] Update `rollout-operator` image to `v0.6.0`. #5155
* [BUGFIX] Backend: configure `-ruler.alertmanager-url` to `mimir-backend` when running in read-write deployment mode. #4892
* [ENHANCEMENT] Memcached: don't overwrite upsteam memcached statefulset jsonnet to allow chosing between antiAffinity and topologySpreadConstraints.

### Mimirtool

* [CHANGE] check rules: will fail on duplicate rules when `--strict` is provided. #5035
* [FEATURE] sync/diff can now include/exclude namespaces based on a regular expression using `--namespaces-regex` and `--ignore-namespaces-regex`. #5100
* [ENHANCEMENT] analyze prometheus: allow to specify `-prometheus-http-prefix`. #4966
* [ENHANCEMENT] analyze grafana: allow to specify `--folder-title` to limit dashboards analysis based on their exact folder title. #4973

### Tools

* [CHANGE] copyblocks: copying between Azure Blob Storage buckets is now supported in addition to copying between Google Cloud Storage buckets. As a result, the `--service` flag is now required to be specified (accepted values are `gcs` or `abs`). #4756

## 2.8.0

### Grafana Mimir

* [CHANGE] Ingester: changed experimental CLI flag from `-out-of-order-blocks-external-label-enabled` to `-ingester.out-of-order-blocks-external-label-enabled` #4440
* [CHANGE] Store-gateway: The following metrics have been removed: #4332
  * `cortex_bucket_store_series_get_all_duration_seconds`
  * `cortex_bucket_store_series_merge_duration_seconds`
* [CHANGE] Ingester: changed default value of `-blocks-storage.tsdb.retention-period` from `24h` to `13h`. If you're running Mimir with a custom configuration and you're overriding `-querier.query-store-after` to a value greater than the default `12h` then you should increase `-blocks-storage.tsdb.retention-period` accordingly. #4382
* [CHANGE] Ingester: the configuration parameter `-blocks-storage.tsdb.max-tsdb-opening-concurrency-on-startup` has been deprecated and will be removed in Mimir 2.10. #4445
* [CHANGE] Query-frontend: Cached results now contain timestamp which allows Mimir to check if cached results are still valid based on current TTL configured for tenant. Results cached by previous Mimir version are used until they expire from cache, which can take up to 7 days. If you need to use per-tenant TTL sooner, please flush results cache manually. #4439
* [CHANGE] Ingester: the `cortex_ingester_tsdb_wal_replay_duration_seconds` metrics has been removed. #4465
* [CHANGE] Query-frontend and ruler: use protobuf internal query result payload format by default. This feature is no longer considered experimental. #4557 #4709
* [CHANGE] Ruler: reject creating federated rule groups while tenant federation is disabled. Previously the rule groups would be silently dropped during bucket sync. #4555
* [CHANGE] Compactor: the `/api/v1/upload/block/{block}/finish` endpoint now returns a `429` status code when the compactor has reached the limit specified by `-compactor.max-block-upload-validation-concurrency`. #4598
* [CHANGE] Compactor: when starting a block upload the maximum byte size of the block metadata provided in the request body is now limited to 1 MiB. If this limit is exceeded a `413` status code is returned. #4683
* [CHANGE] Store-gateway: cache key format for expanded postings has changed. This will invalidate the expanded postings in the index cache when deployed. #4667
* [FEATURE] Cache: Introduce experimental support for using Redis for results, chunks, index, and metadata caches. #4371
* [FEATURE] Vault: Introduce experimental integration with Vault to fetch secrets used to configure TLS for clients. Server TLS secrets will still be read from a file. `tls-ca-path`, `tls-cert-path` and `tls-key-path` will denote the path in Vault for the following CLI flags when `-vault.enabled` is true: #4446.
  * `-distributor.ha-tracker.etcd.*`
  * `-distributor.ring.etcd.*`
  * `-distributor.forwarding.grpc-client.*`
  * `-querier.store-gateway-client.*`
  * `-ingester.client.*`
  * `-ingester.ring.etcd.*`
  * `-querier.frontend-client.*`
  * `-query-frontend.grpc-client-config.*`
  * `-query-frontend.results-cache.redis.*`
  * `-blocks-storage.bucket-store.index-cache.redis.*`
  * `-blocks-storage.bucket-store.chunks-cache.redis.*`
  * `-blocks-storage.bucket-store.metadata-cache.redis.*`
  * `-compactor.ring.etcd.*`
  * `-store-gateway.sharding-ring.etcd.*`
  * `-ruler.client.*`
  * `-ruler.alertmanager-client.*`
  * `-ruler.ring.etcd.*`
  * `-ruler.query-frontend.grpc-client-config.*`
  * `-alertmanager.sharding-ring.etcd.*`
  * `-alertmanager.alertmanager-client.*`
  * `-memberlist.*`
  * `-query-scheduler.grpc-client-config.*`
  * `-query-scheduler.ring.etcd.*`
  * `-overrides-exporter.ring.etcd.*`
* [FEATURE] Distributor, ingester, querier, query-frontend, store-gateway: add experimental support for native histograms. Requires that the experimental protobuf query result response format is enabled by `-query-frontend.query-result-response-format=protobuf` on the query frontend. #4286 #4352 #4354 #4376 #4377 #4387 #4396 #4425 #4442 #4494 #4512 #4513 #4526
* [FEATURE] Added `-<prefix>.s3.storage-class` flag to configure the S3 storage class for objects written to S3 buckets. #4300
* [FEATURE] Add `freebsd` to the target OS when generating binaries for a Mimir release. #4654
* [FEATURE] Ingester: Add `prepare-shutdown` endpoint which can be used as part of Kubernetes scale down automations. #4718
* [ENHANCEMENT] Add timezone information to Alpine Docker images. #4583
* [ENHANCEMENT] Ruler: Sync rules when ruler JOINING the ring instead of ACTIVE, In order to reducing missed rule iterations during ruler restarts. #4451
* [ENHANCEMENT] Allow to define service name used for tracing via `JAEGER_SERVICE_NAME` environment variable. #4394
* [ENHANCEMENT] Querier and query-frontend: add experimental, more performant protobuf query result response format enabled with `-query-frontend.query-result-response-format=protobuf`. #4304 #4318 #4375
* [ENHANCEMENT] Compactor: added experimental configuration parameter `-compactor.first-level-compaction-wait-period`, to configure how long the compactor should wait before compacting 1st level blocks (uploaded by ingesters). This configuration option allows to reduce the chances compactor begins compacting blocks before all ingesters have uploaded their blocks to the storage. #4401
* [ENHANCEMENT] Store-gateway: use more efficient chunks fetching and caching. #4255
* [ENHANCEMENT] Query-frontend and ruler: add experimental, more performant protobuf internal query result response format enabled with `-ruler.query-frontend.query-result-response-format=protobuf`. #4331
* [ENHANCEMENT] Ruler: increased tolerance for missed iterations on alerts, reducing the chances of flapping firing alerts during ruler restarts. #4432
* [ENHANCEMENT] Optimized `.*` and `.+` regular expression label matchers. #4432
* [ENHANCEMENT] Optimized regular expression label matchers with alternates (e.g. `a|b|c`). #4647
* [ENHANCEMENT] Added an in-memory cache for regular expression matchers, to avoid parsing and compiling the same expression multiple times when used in recurring queries. #4633
* [ENHANCEMENT] Query-frontend: results cache TTL is now configurable by using `-query-frontend.results-cache-ttl` and `-query-frontend.results-cache-ttl-for-out-of-order-time-window` options. These values can also be specified per tenant. Default values are unchanged (7 days and 10 minutes respectively). #4385
* [ENHANCEMENT] Ingester: added advanced configuration parameter `-blocks-storage.tsdb.wal-replay-concurrency` representing the maximum number of CPUs used during WAL replay. #4445
* [ENHANCEMENT] Ingester: added metrics `cortex_ingester_tsdb_open_duration_seconds_total` to measure the total time it takes to open all existing TSDBs. The time tracked by this metric also includes the TSDBs WAL replay duration. #4465
* [ENHANCEMENT] Store-gateway: use streaming implementation for LabelNames RPC. The batch size for streaming is controlled by `-blocks-storage.bucket-store.batch-series-size`. #4464
* [ENHANCEMENT] Memcached: Add support for TLS or mTLS connections to cache servers. #4535
* [ENHANCEMENT] Compactor: blocks index files are now validated for correctness for blocks uploaded via the TSDB block upload feature. #4503
* [ENHANCEMENT] Compactor: block chunks and segment files are now validated for correctness for blocks uploaded via the TSDB block upload feature. #4549
* [ENHANCEMENT] Ingester: added configuration options to configure the "postings for matchers" cache of each compacted block queried from ingesters: #4561
  * `-blocks-storage.tsdb.block-postings-for-matchers-cache-ttl`
  * `-blocks-storage.tsdb.block-postings-for-matchers-cache-size`
  * `-blocks-storage.tsdb.block-postings-for-matchers-cache-force`
* [ENHANCEMENT] Compactor: validation of blocks uploaded via the TSDB block upload feature is now configurable on a per tenant basis: #4585
  * `-compactor.block-upload-validation-enabled` has been added, `compactor_block_upload_validation_enabled` can be used to override per tenant
  * `-compactor.block-upload.block-validation-enabled` was the previous global flag and has been removed
* [ENHANCEMENT] TSDB Block Upload: block upload validation concurrency can now be limited with `-compactor.max-block-upload-validation-concurrency`. #4598
* [ENHANCEMENT] OTLP: Add support for converting OTel exponential histograms to Prometheus native histograms. The ingestion of native histograms must be enabled, please set `-ingester.native-histograms-ingestion-enabled` to `true`. #4063 #4639
* [ENHANCEMENT] Query-frontend: add metric `cortex_query_fetched_index_bytes_total` to measure TSDB index bytes fetched to execute a query. #4597
* [ENHANCEMENT] Query-frontend: add experimental limit to enforce a max query expression size in bytes via `-query-frontend.max-query-expression-size-bytes` or `max_query_expression_size_bytes`. #4604
* [ENHANCEMENT] Query-tee: improve message logged when comparing responses and one response contains a non-JSON payload. #4588
* [ENHANCEMENT] Distributor: add ability to set per-distributor limits via `distributor_limits` block in runtime configuration in addition to the existing configuration. #4619
* [ENHANCEMENT] Querier: reduce peak memory consumption for queries that touch a large number of chunks. #4625
* [ENHANCEMENT] Query-frontend: added experimental `-query-frontend.query-sharding-max-regexp-size-bytes` limit to query-frontend. When set to a value greater than 0, query-frontend disabled query sharding for any query with a regexp matcher longer than the configured limit. #4632
* [ENHANCEMENT] Store-gateway: include statistics from LabelValues and LabelNames calls in `cortex_bucket_store_series*` metrics. #4673
* [ENHANCEMENT] Query-frontend: improve readability of distributed tracing spans. #4656
* [ENHANCEMENT] Update Docker base images from `alpine:3.17.2` to `alpine:3.17.3`. #4685
* [ENHANCEMENT] Querier: improve performance when shuffle sharding is enabled and the shard size is large. #4711
* [ENHANCEMENT] Ingester: improve performance when Active Series Tracker is in use. #4717
* [ENHANCEMENT] Store-gateway: optionally select `-blocks-storage.bucket-store.series-selection-strategy`, which can limit the impact of large posting lists (when many series share the same label name and value). #4667 #4695 #4698
* [ENHANCEMENT] Querier: Cache the converted float histogram from chunk iterator, hence there is no need to lookup chunk every time to get the converted float histogram. #4684
* [ENHANCEMENT] Ruler: Improve rule upload performance when not enforcing per-tenant rule group limits. #4828
* [ENHANCEMENT] Improved memory limit on the in-memory cache used for regular expression matchers. #4751
* [BUGFIX] Querier: Streaming remote read will now continue to return multiple chunks per frame after the first frame. #4423
* [BUGFIX] Store-gateway: the values for `stage="processed"` for the metrics `cortex_bucket_store_series_data_touched` and  `cortex_bucket_store_series_data_size_touched_bytes` when using fine-grained chunks caching is now reporting the correct values of chunks held in memory. #4449
* [BUGFIX] Compactor: fixed reporting a compaction error when compactor is correctly shut down while populating blocks. #4580
* [BUGFIX] OTLP: Do not drop exemplars of the OTLP Monotonic Sum metric. #4063
* [BUGFIX] Packaging: flag `/etc/default/mimir` and `/etc/sysconfig/mimir` as config to prevent overwrite. #4587
* [BUGFIX] Query-frontend: don't retry queries which error inside PromQL. #4643
* [BUGFIX] Store-gateway & query-frontend: report more consistent statistics for fetched index bytes. #4671
* [BUGFIX] Native histograms: fix how IsFloatHistogram determines if mimirpb.Histogram is a float histogram. #4706
* [BUGFIX] Query-frontend: fix query sharding for native histograms. #4666
* [BUGFIX] Ring status page: fixed the owned tokens percentage value displayed. #4730
* [BUGFIX] Querier: fixed chunk iterator that can return sample with wrong timestamp. #4450
* [BUGFIX] Packaging: fix preremove script preventing upgrades. #4801
* [BUGFIX] Security: updates Go to version 1.20.4 to fix CVE-2023-24539, CVE-2023-24540, CVE-2023-29400. #4903

### Mixin

* [ENHANCEMENT] Queries: Display data touched per sec in bytes instead of number of items. #4492
* [ENHANCEMENT] `_config.job_names.<job>` values can now be arrays of regular expressions in addition to a single string. Strings are still supported and behave as before. #4543
* [ENHANCEMENT] Queries dashboard: remove mention to store-gateway "streaming enabled" in panels because store-gateway only support streaming series since Mimir 2.7. #4569
* [ENHANCEMENT] Ruler: Add panel description for Read QPS panel in Ruler dashboard to explain values when in remote ruler mode. #4675
* [BUGFIX] Ruler dashboard: show data for reads from ingesters. #4543
* [BUGFIX] Pod selector regex for deployments: change `(.*-mimir-)` to `(.*mimir-)`. #4603

### Jsonnet

* [CHANGE] Ruler: changed ruler deployment max surge from `0` to `50%`, and max unavailable from `1` to `0`. #4381
* [CHANGE] Memcached connections parameters `-blocks-storage.bucket-store.index-cache.memcached.max-idle-connections`, `-blocks-storage.bucket-store.chunks-cache.memcached.max-idle-connections` and `-blocks-storage.bucket-store.metadata-cache.memcached.max-idle-connections` settings are now configured based on `max-get-multi-concurrency` and `max-async-concurrency`. #4591
* [CHANGE] Add support to use external Redis as cache. Following are some changes in the jsonnet config: #4386 #4640
  * Renamed `memcached_*_enabled` config options to `cache_*_enabled`
  * Renamed `memcached_*_max_item_size_mb` config options to `cache_*_max_item_size_mb`
  * Added `cache_*_backend` config options
* [CHANGE] Store-gateway StatefulSets with disabled multi-zone deployment are also unregistered from the ring on shutdown. This eliminated resharding during rollouts, at the cost of extra effort during scaling down store-gateways. For more information see [Scaling down store-gateways](https://grafana.com/docs/mimir/v2.7.x/operators-guide/run-production-environment/scaling-out/#scaling-down-store-gateways). #4713
* [CHANGE] Removed `$._config.querier.replicas` and `$._config.queryFrontend.replicas`. If you need to customize the number of querier or query-frontend replicas, and autoscaling is disabled, please set an override as is done for other stateless components (e.g. distributors). #5130
* [ENHANCEMENT] Alertmanager: add `alertmanager_data_disk_size` and  `alertmanager_data_disk_class` configuration options, by default no storage class is set. #4389
* [ENHANCEMENT] Update `rollout-operator` to `v0.4.0`. #4524
* [ENHANCEMENT] Update memcached to `memcached:1.6.19-alpine`. #4581
* [ENHANCEMENT] Add support for mTLS connections to Memcached servers. #4553
* [ENHANCEMENT] Update the `memcached-exporter` to `v0.11.2`. #4570
* [ENHANCEMENT] Autoscaling: Add `autoscaling_query_frontend_memory_target_utilization`, `autoscaling_ruler_query_frontend_memory_target_utilization`, and `autoscaling_ruler_memory_target_utilization` configuration options, for controlling the corresponding autoscaler memory thresholds. Each has a default of 1, i.e. 100%. #4612
* [ENHANCEMENT] Distributor: add ability to set per-distributor limits via `distributor_instance_limits` using runtime configuration. #4627
* [BUGFIX] Add missing query sharding settings for user_24M and user_32M plans. #4374

### Mimirtool

* [ENHANCEMENT] Backfill: mimirtool will now sleep and retry if it receives a 429 response while trying to finish an upload due to validation concurrency limits. #4598
* [ENHANCEMENT] `gauge` panel type is supported now in `mimirtool analyze dashboard`. #4679
* [ENHANCEMENT] Set a `User-Agent` header on requests to Mimir or Prometheus servers. #4700

### Mimir Continuous Test

* [FEATURE] Allow continuous testing of native histograms as well by enabling the flag `-tests.write-read-series-test.histogram-samples-enabled`. The metrics exposed by the tool will now have a new label called `type` with possible values of `float`, `histogram_float_counter`, `histogram_float_gauge`, `histogram_int_counter`, `histogram_int_gauge`, the list of metrics impacted: #4457
  * `mimir_continuous_test_writes_total`
  * `mimir_continuous_test_writes_failed_total`
  * `mimir_continuous_test_queries_total`
  * `mimir_continuous_test_queries_failed_total`
  * `mimir_continuous_test_query_result_checks_total`
  * `mimir_continuous_test_query_result_checks_failed_total`
* [ENHANCEMENT] Added a new metric `mimir_continuous_test_build_info` that reports version information, similar to the existing `cortex_build_info` metric exposed by other Mimir components. #4712
* [ENHANCEMENT] Add coherency for the selected ranges and instants of test queries. #4704

### Query-tee

### Documentation

* [CHANGE] Clarify what deprecation means in the lifecycle of configuration parameters. #4499
* [CHANGE] Update compactor `split-groups` and `split-and-merge-shards` recommendation on component page. #4623
* [FEATURE] Add instructions about how to configure native histograms. #4527
* [ENHANCEMENT] Runbook for MimirCompactorHasNotSuccessfullyRunCompaction extended to include scenario where compaction has fallen behind. #4609
* [ENHANCEMENT] Add explanation for QPS values for reads in remote ruler mode and writes generally, to the Ruler dashboard page. #4629
* [ENHANCEMENT] Expand zone-aware replication page to cover single physical availability zone deployments. #4631
* [FEATURE] Add instructions to use puppet module. #4610
* [FEATURE] Add documentation on how deploy mixin with terraform. #4161

### Tools

* [ENHANCEMENT] tsdb-index: iteration over index is now faster when any equal matcher is supplied. #4515

## 2.7.3

### Grafana Mimir

* [BUGFIX] Security: updates Go to version 1.20.4 to fix CVE-2023-24539, CVE-2023-24540, CVE-2023-29400. #4905

## 2.7.2

### Grafana Mimir

* [BUGFIX] Security: updated Go version to 1.20.3 to fix CVE-2023-24538 #4795

## 2.7.1

**Note**: During the release process, version 2.7.0 was tagged too early, before completing the release checklist and production testing. Release 2.7.1 doesn't include any code changes since 2.7.0, but now has proper release notes, published documentation, and has been fully tested in our production environment.

### Grafana Mimir

* [CHANGE] Ingester: the configuration parameter `-ingester.ring.readiness-check-ring-health` has been deprecated and will be removed in Mimir 2.9. #4422
* [CHANGE] Ruler: changed default value of `-ruler.evaluation-delay-duration` option from 0 to 1m. #4250
* [CHANGE] Querier: Errors with status code `422` coming from the store-gateway are propagated and not converted to the consistency check error anymore. #4100
* [CHANGE] Store-gateway: When a query hits `max_fetched_chunks_per_query` and `max_fetched_series_per_query` limits, an error with the status code `422` is created and returned. #4056
* [CHANGE] Packaging: Migrate FPM packaging solution to NFPM. Rationalize packages dependencies and add package for all binaries. #3911
* [CHANGE] Store-gateway: Deprecate flag `-blocks-storage.bucket-store.chunks-cache.subrange-size` since there's no benefit to changing the default of `16000`. #4135
* [CHANGE] Experimental support for ephemeral storage introduced in Mimir 2.6.0 has been removed. Following options are no longer available: #4252
  * `-blocks-storage.ephemeral-tsdb.*`
  * `-distributor.ephemeral-series-enabled`
  * `-distributor.ephemeral-series-matchers`
  * `-ingester.max-ephemeral-series-per-user`
  * `-ingester.instance-limits.max-ephemeral-series`
Querying with using `{__mimir_storage__="ephemeral"}` selector no longer works. All label values with `ephemeral-` prefix in `reason` label of `cortex_discarded_samples_total` metric are no longer available. Following metrics have been removed:
  * `cortex_ingester_ephemeral_series`
  * `cortex_ingester_ephemeral_series_created_total`
  * `cortex_ingester_ephemeral_series_removed_total`
  * `cortex_ingester_ingested_ephemeral_samples_total`
  * `cortex_ingester_ingested_ephemeral_samples_failures_total`
  * `cortex_ingester_memory_ephemeral_users`
  * `cortex_ingester_queries_ephemeral_total`
  * `cortex_ingester_queried_ephemeral_samples`
  * `cortex_ingester_queried_ephemeral_series`
* [CHANGE] Store-gateway: use mmap-less index-header reader by default and remove mmap-based index header reader. The following flags have changed: #4280
   * `-blocks-storage.bucket-store.index-header.map-populate-enabled` has been removed
   * `-blocks-storage.bucket-store.index-header.stream-reader-enabled` has been removed
   * `-blocks-storage.bucket-store.index-header.stream-reader-max-idle-file-handles` has been renamed to `-blocks-storage.bucket-store.index-header.max-idle-file-handles`, and the corresponding configuration file option has been renamed from `stream_reader_max_idle_file_handles` to `max_idle_file_handles`
* [CHANGE] Store-gateway: the streaming store-gateway is now enabled by default. The new default setting for `-blocks-storage.bucket-store.batch-series-size` is `5000`. #4330
* [CHANGE] Compactor: the configuration parameter `-compactor.consistency-delay` has been deprecated and will be removed in Mimir 2.9. #4409
* [CHANGE] Store-gateway: the configuration parameter `-blocks-storage.bucket-store.consistency-delay` has been deprecated and will be removed in Mimir 2.9. #4409
* [FEATURE] Ruler: added `keep_firing_for` support to alerting rules. #4099
* [FEATURE] Distributor, ingester: ingestion of native histograms. The new per-tenant limit `-ingester.native-histograms-ingestion-enabled` controls whether native histograms are stored or ignored. #4159
* [FEATURE] Query-frontend: Introduce experimental `-query-frontend.query-sharding-target-series-per-shard` to allow query sharding to take into account cardinality of similar requests executed previously. This feature uses the same cache that's used for results caching. #4121 #4177 #4188 #4254
* [ENHANCEMENT] Go: update go to 1.20.1. #4266
* [ENHANCEMENT] Ingester: added `out_of_order_blocks_external_label_enabled` shipper option to label out-of-order blocks before shipping them to cloud storage. #4182 #4297
* [ENHANCEMENT] Ruler: introduced concurrency when loading per-tenant rules configuration. This improvement is expected to speed up the ruler start up time in a Mimir cluster with a large number of tenants. #4258
* [ENHANCEMENT] Compactor: Add `reason` label to `cortex_compactor_runs_failed_total`. The value can be `shutdown` or `error`. #4012
* [ENHANCEMENT] Store-gateway: enforce `max_fetched_series_per_query`. #4056
* [ENHANCEMENT] Query-frontend: Disambiguate logs for failed queries. #4067
* [ENHANCEMENT] Query-frontend: log caller user agent in query stats logs. #4093
* [ENHANCEMENT] Store-gateway: add `data_type` label with values on `cortex_bucket_store_partitioner_extended_ranges_total`, `cortex_bucket_store_partitioner_expanded_ranges_total`, `cortex_bucket_store_partitioner_requested_ranges_total`, `cortex_bucket_store_partitioner_expanded_bytes_total`, `cortex_bucket_store_partitioner_requested_bytes_total` for `postings`, `series`, and `chunks`. #4095
* [ENHANCEMENT] Store-gateway: Reduce memory allocation rate when loading TSDB chunks from Memcached. #4074
* [ENHANCEMENT] Query-frontend: track `cortex_frontend_query_response_codec_duration_seconds` and `cortex_frontend_query_response_codec_payload_bytes` metrics to measure the time taken and bytes read / written while encoding and decoding query result payloads. #4110
* [ENHANCEMENT] Alertmanager: expose additional upstream metrics `cortex_alertmanager_dispatcher_aggregation_groups`, `cortex_alertmanager_dispatcher_alert_processing_duration_seconds`. #4151
* [ENHANCEMENT] Querier and query-frontend: add experimental, more performant protobuf internal query result response format enabled with `-query-frontend.query-result-response-format=protobuf`. #4153
* [ENHANCEMENT] Store-gateway: use more efficient chunks fetching and caching. This should reduce CPU, memory utilization, and receive bandwidth of a store-gateway. Enable with `-blocks-storage.bucket-store.chunks-cache.fine-grained-chunks-caching-enabled=true`. #4163 #4174 #4227
* [ENHANCEMENT] Query-frontend: Wait for in-flight queries to finish before shutting down. #4073 #4170
* [ENHANCEMENT] Store-gateway: added `encode` and `other` stage to `cortex_bucket_store_series_request_stage_duration_seconds` metric. #4179
* [ENHANCEMENT] Ingester: log state of TSDB when shipping or forced compaction can't be done due to unexpected state of TSDB. #4211
* [ENHANCEMENT] Update Docker base images from `alpine:3.17.1` to `alpine:3.17.2`. #4240
* [ENHANCEMENT] Store-gateway: add a `stage` label to the metrics `cortex_bucket_store_series_data_fetched`, `cortex_bucket_store_series_data_size_fetched_bytes`, `cortex_bucket_store_series_data_touched`, `cortex_bucket_store_series_data_size_touched_bytes`. This label only applies to `data_type="chunks"`. For `fetched` metrics with `data_type="chunks"` the `stage` label has 2 values: `fetched` - the chunks or bytes that were fetched from the cache or the object store, `refetched` - the chunks or bytes that had to be refetched from the cache or the object store because their size was underestimated during the first fetch. For `touched` metrics with `data_type="chunks"` the `stage` label has 2 values: `processed` - the chunks or bytes that were read from the fetched chunks or bytes and were processed in memory, `returned` - the chunks or bytes that were selected from the processed bytes to satisfy the query. #4227 #4316
* [ENHANCEMENT] Compactor: improve the partial block check related to `compactor.partial-block-deletion-delay` to potentially issue less requests to object storage. #4246
* [ENHANCEMENT] Memcached: added `-*.memcached.min-idle-connections-headroom-percentage` support to configure the minimum number of idle connections to keep open as a percentage (0-100) of the number of recently used idle connections. This feature is disabled when set to a negative value (default), which means idle connections are kept open indefinitely. #4249
* [ENHANCEMENT] Querier and store-gateway: optimized regular expression label matchers with case insensitive alternate operator. #4340 #4357
* [ENHANCEMENT] Compactor: added the experimental flag `-compactor.block-upload.block-validation-enabled` with the default `true` to configure whether block validation occurs on backfilled blocks. #3411
* [ENHANCEMENT] Ingester: apply a jitter to the first TSDB head compaction interval configured via `-blocks-storage.tsdb.head-compaction-interval`. Subsequent checks will happen at the configured interval. This should help to spread the TSDB head compaction among different ingesters over the configured interval. #4364
* [ENHANCEMENT] Ingester: the maximum accepted value for `-blocks-storage.tsdb.head-compaction-interval` has been increased from 5m to 15m. #4364
* [BUGFIX] Store-gateway: return `Canceled` rather than `Aborted` or `Internal` error when the calling querier cancels a label names or values request, and return `Internal` if processing the request fails for another reason. #4061
* [BUGFIX] Querier: track canceled requests with status code `499` in the metrics instead of `503` or `422`. #4099
* [BUGFIX] Ingester: compact out-of-order data during `/ingester/flush` or when TSDB is idle. #4180
* [BUGFIX] Ingester: conversion of global limits `max-series-per-user`, `max-series-per-metric`, `max-metadata-per-user` and `max-metadata-per-metric` into corresponding local limits now takes into account the number of ingesters in each zone. #4238
* [BUGFIX] Ingester: track `cortex_ingester_memory_series` metric consistently with `cortex_ingester_memory_series_created_total` and `cortex_ingester_memory_series_removed_total`. #4312
* [BUGFIX] Querier: fixed a bug which was incorrectly matching series with regular expression label matchers with begin/end anchors in the middle of the regular expression. #4340

### Mixin

* [CHANGE] Move auto-scaling panel rows down beneath logical network path in Reads and Writes dashboards. #4049
* [CHANGE] Make distributor auto-scaling metric panels show desired number of replicas. #4218
* [CHANGE] Alerts: The alert `MimirMemcachedRequestErrors` has been renamed to `MimirCacheRequestErrors`. #4242
* [ENHANCEMENT] Alerts: Added `MimirAutoscalerKedaFailing` alert firing when a KEDA scaler is failing. #4045
* [ENHANCEMENT] Add auto-scaling panels to ruler dashboard. #4046
* [ENHANCEMENT] Add gateway auto-scaling panels to Reads and Writes dashboards. #4049 #4216
* [ENHANCEMENT] Dashboards: distinguish between label names and label values queries. #4065
* [ENHANCEMENT] Add query-frontend and ruler-query-frontend auto-scaling panels to Reads and Ruler dashboards. #4199
* [BUGFIX] Alerts: Fixed `MimirAutoscalerNotActive` to not fire if scaling metric does not exist, to avoid false positives on scaled objects with 0 min replicas. #4045
* [BUGFIX] Alerts: `MimirCompactorHasNotSuccessfullyRunCompaction` is no longer triggered by frequent compactor restarts. #4012
* [BUGFIX] Tenants dashboard: Correctly show the ruler-query-scheduler queue size. #4152

### Jsonnet

* [CHANGE] Create the `query-frontend-discovery` service only when Mimir is deployed in microservice mode without query-scheduler. #4353
* [CHANGE] Add results cache backend config to `ruler-query-frontend` configuration to allow cache reuse for cardinality-estimation based sharding. #4257
* [ENHANCEMENT] Add support for ruler auto-scaling. #4046
* [ENHANCEMENT] Add optional `weight` param to `newQuerierScaledObject` and `newRulerQuerierScaledObject` to allow running multiple querier deployments on different node types. #4141
* [ENHANCEMENT] Add support for query-frontend and ruler-query-frontend auto-scaling. #4199
* [BUGFIX] Shuffle sharding: when applying user class limits, honor the minimum shard size configured in `$._config.shuffle_sharding.*`. #4363

### Mimirtool

* [FEATURE] Added `keep_firing_for` support to rules configuration. #4099
* [ENHANCEMENT] Add `-tls-insecure-skip-verify` to rules, alertmanager and backfill commands. #4162

### Query-tee

* [CHANGE] Increase default value of `-backend.read-timeout` to 150s, to accommodate default querier and query frontend timeout of 120s. #4262
* [ENHANCEMENT] Log errors that occur while performing requests to compare two endpoints. #4262
* [ENHANCEMENT] When comparing two responses that both contain an error, only consider the comparison failed if the errors differ. Previously, if either response contained an error, the comparison always failed, even if both responses contained the same error. #4262
* [ENHANCEMENT] Include the value of the `X-Scope-OrgID` header when logging a comparison failure. #4262
* [BUGFIX] Parameters (expression, time range etc.) for a query request where the parameters are in the HTTP request body rather than in the URL are now logged correctly when responses differ. #4265

### Documentation

* [ENHANCEMENT] Add guide on alternative migration method for Thanos to Mimir #3554
* [ENHANCEMENT] Restore "Migrate from Cortex" for Jsonnet. #3929
* [ENHANCEMENT] Document migration from microservices to read-write deployment mode. #3951
* [ENHANCEMENT] Do not error when there is nothing to commit as part of a publish #4058
* [ENHANCEMENT] Explain how to run Mimir locally using docker-compose #4079
* [ENHANCEMENT] Docs: use long flag names in runbook commands. #4088
* [ENHANCEMENT] Clarify how ingester replication happens. #4101
* [ENHANCEMENT] Improvements to the Get Started guide. #4315
* [BUGFIX] Added indentation to Azure and SWIFT backend definition. #4263

### Tools

* [ENHANCEMENT] Adapt tsdb-print-chunk for native histograms. #4186
* [ENHANCEMENT] Adapt tsdb-index-health for blocks containing native histograms. #4186
* [ENHANCEMENT] Adapt tsdb-chunks tool to handle native histograms. #4186

## 2.6.2

* [BUGFIX] Security: updates Go to version 1.20.4 to fix CVE-2023-24539, CVE-2023-24540, CVE-2023-29400. #4903

## 2.6.1

### Grafana Mimir

* [BUGFIX] Security: updates Go to version 1.20.3 to fix CVE-2023-24538 #4798

## 2.6.0

### Grafana Mimir

* [CHANGE] Querier: Introduce `-querier.max-partial-query-length` to limit the time range for partial queries at the querier level and deprecate `-store.max-query-length`. #3825 #4017
* [CHANGE] Store-gateway: Remove experimental `-blocks-storage.bucket-store.max-concurrent-reject-over-limit` flag. #3706
* [CHANGE] Ingester: If shipping is enabled block retention will now be relative to the upload time to cloud storage. If shipping is disabled block retention will be relative to the creation time of the block instead of the mintime of the last block created. #3816
* [CHANGE] Query-frontend: Deprecated CLI flag `-query-frontend.align-querier-with-step` has been removed. #3982
* [CHANGE] Alertmanager: added default configuration for `-alertmanager.configs.fallback`. Allows tenants to send alerts without first uploading an Alertmanager configuration. #3541
* [FEATURE] Store-gateway: streaming of series. The store-gateway can now stream results back to the querier instead of buffering them. This is expected to greatly reduce peak memory consumption while keeping latency the same. You can enable this feature by setting `-blocks-storage.bucket-store.batch-series-size` to a value in the high thousands (5000-10000). This is still an experimental feature and is subject to a changing API and instability. #3540 #3546 #3587 #3606 #3611 #3620 #3645 #3355 #3697 #3666 #3687 #3728 #3739 #3751 #3779 #3839
* [FEATURE] Alertmanager: Added support for the Webex receiver. #3758
* [FEATURE] Limits: Added the `-validation.separate-metrics-group-label` flag. This allows further separation of the `cortex_discarded_samples_total` metric by an additional `group` label - which is configured by this flag to be the value of a specific label on an incoming timeseries. Active groups are tracked and inactive groups are cleaned up on a defined interval. The maximum number of groups tracked is controlled by the `-max-separate-metrics-groups-per-user` flag. #3439
* [FEATURE] Overrides-exporter: Added experimental ring support to overrides-exporter via `-overrides-exporter.ring.enabled`. When enabled, the ring is used to establish a leader replica for the export of limit override metrics. #3908 #3953
* [FEATURE] Ephemeral storage (experimental): Mimir can now accept samples into "ephemeral storage". Such samples are available for querying for a short amount of time (`-blocks-storage.ephemeral-tsdb.retention-period`, defaults to 10 minutes), and then removed from memory. To use ephemeral storage, distributor must be configured with `-distributor.ephemeral-series-enabled` option. Series matching `-distributor.ephemeral-series-matchers` will be marked for storing into ephemeral storage in ingesters. Each tenant needs to have ephemeral storage enabled by using `-ingester.max-ephemeral-series-per-user` limit, which defaults to 0 (no ephemeral storage). Ingesters have new `-ingester.instance-limits.max-ephemeral-series` limit for total number of series in ephemeral storage across all tenants. If ingestion of samples into ephemeral storage fails, `cortex_discarded_samples_total` metric will use values prefixed with `ephemeral-` for `reason` label. Querying of ephemeral storage is possible by using `{__mimir_storage__="ephemeral"}` as metric selector. Following new metrics related to ephemeral storage are introduced: #3897 #3922 #3961 #3997 #4004
  * `cortex_ingester_ephemeral_series`
  * `cortex_ingester_ephemeral_series_created_total`
  * `cortex_ingester_ephemeral_series_removed_total`
  * `cortex_ingester_ingested_ephemeral_samples_total`
  * `cortex_ingester_ingested_ephemeral_samples_failures_total`
  * `cortex_ingester_memory_ephemeral_users`
  * `cortex_ingester_queries_ephemeral_total`
  * `cortex_ingester_queried_ephemeral_samples`
  * `cortex_ingester_queried_ephemeral_series`
* [ENHANCEMENT] Added new metric `thanos_shipper_last_successful_upload_time`: Unix timestamp (in seconds) of the last successful TSDB block uploaded to the bucket. #3627
* [ENHANCEMENT] Ruler: Added `-ruler.alertmanager-client.tls-enabled` configuration for alertmanager client. #3432 #3597
* [ENHANCEMENT] Activity tracker logs now have `component=activity-tracker` label. #3556
* [ENHANCEMENT] Distributor: remove labels with empty values #2439
* [ENHANCEMENT] Query-frontend: track query HTTP requests in the Activity Tracker. #3561
* [ENHANCEMENT] Store-gateway: Add experimental alternate implementation of index-header reader that does not use memory mapped files. The index-header reader is expected to improve stability of the store-gateway. You can enable this implementation with the flag `-blocks-storage.bucket-store.index-header.stream-reader-enabled`. #3639 #3691 #3703 #3742 #3785 #3787 #3797
* [ENHANCEMENT] Query-scheduler: add `cortex_query_scheduler_cancelled_requests_total` metric to track the number of requests that are already cancelled when dequeued. #3696
* [ENHANCEMENT] Store-gateway: add `cortex_bucket_store_partitioner_extended_ranges_total` metric to keep track of the ranges that the partitioner decided to overextend and merge in order to save API call to the object storage. #3769
* [ENHANCEMENT] Compactor: Auto-forget unhealthy compactors after ten failed ring heartbeats. #3771
* [ENHANCEMENT] Ruler: change default value of `-ruler.for-grace-period` from `10m` to `2m` and update help text. The new default value reflects how we operate Mimir at Grafana Labs. #3817
* [ENHANCEMENT] Ingester: Added experimental flags to force usage of _postings for matchers cache_. These flags will be removed in the future and it's not recommended to change them. #3823
  * `-blocks-storage.tsdb.head-postings-for-matchers-cache-ttl`
  * `-blocks-storage.tsdb.head-postings-for-matchers-cache-size`
  * `-blocks-storage.tsdb.head-postings-for-matchers-cache-force`
* [ENHANCEMENT] Ingester: Improved series selection performance when some of the matchers do not match any series. #3827
* [ENHANCEMENT] Alertmanager: Add new additional template function `tenantID` returning id of the tenant owning the alert. #3758
* [ENHANCEMENT] Alertmanager: Add additional template function `grafanaExploreURL` returning URL to grafana explore with range query. #3849
* [ENHANCEMENT] Reduce overhead of debug logging when filtered out. #3875
* [ENHANCEMENT] Update Docker base images from `alpine:3.16.2` to `alpine:3.17.1`. #3898
* [ENHANCEMENT] Ingester: Add new `/ingester/tsdb_metrics` endpoint to return tenant-specific TSDB metrics. #3923
* [ENHANCEMENT] Query-frontend: CLI flag `-query-frontend.max-total-query-length` and its associated YAML configuration is now stable. #3882
* [ENHANCEMENT] Ruler: rule groups now support optional and experimental `align_evaluation_time_on_interval` field, which causes all evaluations to happen on interval-aligned timestamp. #4013
* [ENHANCEMENT] Query-scheduler: ring-based service discovery is now stable. #4028
* [ENHANCEMENT] Store-gateway: improved performance of prefix matching on the labels. #4055 #4080
* [BUGFIX] Log the names of services that are not yet running rather than `unsupported value type` when calling `/ready` and some services are not running. #3625
* [BUGFIX] Alertmanager: Fix template spurious deletion with relative data dir. #3604
* [BUGFIX] Security: update prometheus/exporter-toolkit for CVE-2022-46146. #3675
* [BUGFIX] Security: update golang.org/x/net for CVE-2022-41717. #3755
* [BUGFIX] Debian package: Fix post-install, environment file path and user creation. #3720
* [BUGFIX] memberlist: Fix panic during Mimir startup when Mimir receives gossip message before it's ready. #3746
* [BUGFIX] Store-gateway: fix `cortex_bucket_store_partitioner_requested_bytes_total` metric to not double count overlapping ranges. #3769
* [BUGFIX] Update `github.com/thanos-io/objstore` to address issue with Multipart PUT on s3-compatible Object Storage. #3802 #3821
* [BUGFIX] Distributor, Query-scheduler: Make sure ring metrics include a `cortex_` prefix as expected by dashboards. #3809
* [BUGFIX] Querier: canceled requests are no longer reported as "consistency check" failures. #3837 #3927
* [BUGFIX] Distributor: don't panic when `metric_relabel_configs` in overrides contains null element. #3868
* [BUGFIX] Distributor: don't panic when OTLP histograms don't have any buckets. #3853
* [BUGFIX] Ingester, Compactor: fix panic that can occur when compaction fails. #3955
* [BUGFIX] Store-gateway: return `Canceled` rather than `Aborted` error when the calling querier cancels the request. #4007

### Mixin

* [ENHANCEMENT] Alerts: Added `MimirIngesterInstanceHasNoTenants` alert that fires when an ingester replica is not receiving write requests for any tenant. #3681
* [ENHANCEMENT] Alerts: Extended `MimirAllocatingTooMuchMemory` to check read-write deployment containers. #3710
* [ENHANCEMENT] Alerts: Added `MimirAlertmanagerInstanceHasNoTenants` alert that fires when an alertmanager instance ows no tenants. #3826
* [ENHANCEMENT] Alerts: Added `MimirRulerInstanceHasNoRuleGroups` alert that fires when a ruler replica is not assigned any rule group to evaluate. #3723
* [ENHANCEMENT] Support for baremetal deployment for alerts and scaling recording rules. #3719
* [ENHANCEMENT] Dashboards: querier autoscaling now supports multiple scaled objects (configurable via `$._config.autoscale.querier.hpa_name`). #3962
* [BUGFIX] Alerts: Fixed `MimirIngesterRestarts` alert when Mimir is deployed in read-write mode. #3716
* [BUGFIX] Alerts: Fixed `MimirIngesterHasNotShippedBlocks` and `MimirIngesterHasNotShippedBlocksSinceStart` alerts for when Mimir is deployed in read-write or monolithic modes and updated them to use new `thanos_shipper_last_successful_upload_time` metric. #3627
* [BUGFIX] Alerts: Fixed `MimirMemoryMapAreasTooHigh` alert when Mimir is deployed in read-write mode. #3626
* [BUGFIX] Alerts: Fixed `MimirCompactorSkippedBlocksWithOutOfOrderChunks` matching on non-existent label. #3628
* [BUGFIX] Dashboards: Fix `Rollout Progress` dashboard incorrectly using Gateway metrics when Gateway was not enabled. #3709
* [BUGFIX] Tenants dashboard: Make it compatible with all deployment types. #3754
* [BUGFIX] Alerts: Fixed `MimirCompactorHasNotUploadedBlocks` to not fire if compactor has nothing to do. #3793
* [BUGFIX] Alerts: Fixed `MimirAutoscalerNotActive` to not fire if scaling metric is 0, to avoid false positives on scaled objects with 0 min replicas. #3999

### Jsonnet

* [CHANGE] Replaced the deprecated `policy/v1beta1` with `policy/v1` when configuring a PodDisruptionBudget for read-write deployment mode. #3811
* [CHANGE] Removed `-server.http-write-timeout` default option value from querier and query-frontend, as it defaults to a higher value in the code now, and cannot be lower than `-querier.timeout`. #3836
* [CHANGE] Replaced `-store.max-query-length` with `-query-frontend.max-total-query-length` in the query-frontend config. #3879
* [CHANGE] Changed default `mimir_backend_data_disk_size` from `100Gi` to `250Gi`. #3894
* [ENHANCEMENT] Update `rollout-operator` to `v0.2.0`. #3624
* [ENHANCEMENT] Add `user_24M` and `user_32M` classes to operations config. #3367
* [ENHANCEMENT] Update memcached image from `memcached:1.6.16-alpine` to `memcached:1.6.17-alpine`. #3914
* [ENHANCEMENT] Allow configuring the ring for overrides-exporter. #3995
* [BUGFIX] Apply ingesters and store-gateways per-zone CLI flags overrides to read-write deployment mode too. #3766
* [BUGFIX] Apply overrides-exporter CLI flags to mimir-backend when running Mimir in read-write deployment mode. #3790
* [BUGFIX] Fixed `mimir-write` and `mimir-read` Kubernetes service to correctly balance requests among pods. #3855 #3864 #3906
* [BUGFIX] Fixed `ruler-query-frontend` and `mimir-read` gRPC server configuration to force clients to periodically re-resolve the backend addresses. #3862
* [BUGFIX] Fixed `mimir-read` CLI flags to ensure query-frontend configuration takes precedence over querier configuration. #3877

### Mimirtool

* [ENHANCEMENT] Update `mimirtool config convert` to work with Mimir 2.4, 2.5, 2.6 changes. #3952
* [ENHANCEMENT] Mimirtool is now available to install through Homebrew with `brew install mimirtool`. #3776
* [ENHANCEMENT] Added `--concurrency` to `mimirtool rules sync` command. #3996
* [BUGFIX] Fix summary output from `mimirtool rules sync` to display correct number of groups created and updated. #3918

### Documentation

* [BUGFIX] Querier: Remove assertion that the `-querier.max-concurrent` flag must also be set for the query-frontend. #3678
* [ENHANCEMENT] Update migration from cortex documentation. #3662
* [ENHANCEMENT] Query-scheduler: documented how to migrate from DNS-based to ring-based service discovery. #4028

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
* [ENHANCEMENT] Added `/api/v1/status/config` and `/api/v1/status/flags` APIs to maintain compatibility with prometheus. #3596 #3983
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
* [ENHANCEMENT] Added the `-tests.write-protocol` flag to write using the `prometheus` remote write protocol or `otlp-http` in the `mimir-continuous-test` suite. #5719

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
