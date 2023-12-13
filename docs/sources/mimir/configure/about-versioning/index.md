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

```yaml
# (experimental) Maximum number of groups allowed per user by which specified
# distributor and ingester metrics can be further separated.
# CLI flag: -max-separate-metrics-groups-per-user
[max_separate_metrics_groups_per_user: <int> | default = 1000]

api:
  # (experimental) If true, store metadata when ingesting metrics via OTLP. This
  # makes metric descriptions and types available for metrics ingested via OTLP.
  # CLI flag: -distributor.enable-otlp-metadata-storage
  [enable_otel_metadata_translation: <boolean> | default = false]

# The server block configures the HTTP and gRPC server of the launched
# service(s).
[server: <server>]

# The distributor block configures the distributor.
[distributor: <distributor>]

# The querier block configures the querier.
[querier: <querier>]

# The ingester_client block configures how the distributors connect to the
# ingesters.
[ingester_client: <ingester_client>]

# The ingester block configures the ingester.
[ingester: <ingester>]

# The limits block configures default and per-tenant limits imposed by
# components.
[limits: <limits>]

# The frontend_worker block configures the worker running within the querier,
# picking up and executing queries enqueued by the query-frontend or the
# query-scheduler.
[frontend_worker: <frontend_worker>]

# The frontend block configures the query-frontend.
[frontend: <frontend>]

# The blocks_storage block configures the blocks storage.
[blocks_storage: <blocks_storage>]

# The compactor block configures the compactor component.
[compactor: <compactor>]

tenant_federation:
  # (experimental) The number of workers used for each tenant federated query.
  # This setting limits the maximum number of per-tenant queries executed at a
  # time for a tenant federated query.
  # CLI flag: -tenant-federation.max-concurrent
  [max_concurrent: <int> | default = 16]

vault:
  # (experimental) Enables fetching of keys and certificates from Vault
  # CLI flag: -vault.enabled
  [enabled: <boolean> | default = false]

  # (experimental) Location of the Vault server
  # CLI flag: -vault.url
  [url: <string> | default = ""]

  # (experimental) Location of secrets engine within Vault
  # CLI flag: -vault.mount-path
  [mount_path: <string> | default = ""]

  auth:
    # (experimental) Authentication type to use. Supported types are: approle,
    # kubernetes, userpass, token
    # CLI flag: -vault.auth.type
    [type: <string> | default = ""]

    approle:
      # (experimental) Role ID of the AppRole
      # CLI flag: -vault.auth.approle.role-id
      [role_id: <string> | default = ""]

      # (experimental) Secret ID issued against the AppRole
      # CLI flag: -vault.auth.approle.secret-id
      [secret_id: <string> | default = ""]

      # (experimental) Response wrapping token if the Secret ID is response
      # wrapped
      # CLI flag: -vault.auth.approle.wrapping-token
      [wrapping_token: <boolean> | default = false]

      # (experimental) Path if the Vault backend was mounted using a non-default
      # path
      # CLI flag: -vault.auth.approle.mount-path
      [mount_path: <string> | default = ""]

    kubernetes:
      # (experimental) The Kubernetes named role
      # CLI flag: -vault.auth.kubernetes.role-name
      [role_name: <string> | default = ""]

      # (experimental) The Service Account JWT
      # CLI flag: -vault.auth.kubernetes.service-account-token
      [service_account_token: <string> | default = ""]

      # (experimental) Path to where the Kubernetes service account token is
      # mounted. By default it lives at
      # /var/run/secrets/kubernetes.io/serviceaccount/token. Field will be used
      # if the service_account_token is not specified.
      # CLI flag: -vault.auth.kubernetes.service-account-token-path
      [service_account_token_path: <string> | default = ""]

      # (experimental) Path if the Vault backend was mounted using a non-default
      # path
      # CLI flag: -vault.auth.kubernetes.mount-path
      [mount_path: <string> | default = ""]

    userpass:
      # (experimental) The userpass auth method username
      # CLI flag: -vault.auth.userpass.username
      [username: <string> | default = ""]

      # (experimental) The userpass auth method password
      # CLI flag: -vault.auth.userpass.password
      [password: <string> | default = ""]

      # (experimental) Path if the Vault backend was mounted using a non-default
      # path
      # CLI flag: -vault.auth.userpass.mount-path
      [mount_path: <string> | default = ""]

    token:
      # (experimental) The token used to authenticate against Vault
      # CLI flag: -vault.auth.token
      [token: <string> | default = ""]

# The ruler block configures the ruler.
[ruler: <ruler>]

# The ruler_storage block configures the ruler storage backend.
[ruler_storage: <ruler_storage>]

# The alertmanager block configures the alertmanager.
[alertmanager: <alertmanager>]

# The alertmanager_storage block configures the alertmanager storage backend.
[alertmanager_storage: <alertmanager_storage>]

# The query_scheduler block configures the query-scheduler.
[query_scheduler: <query_scheduler>]

# The common block holds configurations that configure multiple components at a
# time.
[common: <common>]

# (experimental) Enables optimized marshaling of timeseries.
# CLI flag: -timeseries-unmarshal-caching-optimization-enabled
[timeseries_unmarshal_caching_optimization_enabled: <boolean> | default = true]
```

### common

The `common` block holds configurations that configure multiple components at a time.

```yaml
storage:
  # The s3_backend block configures the connection to Amazon S3 object storage
  # backend.
  # The CLI flags prefix for this block configuration is: common.storage
  [s3: <s3_storage_backend>]
```

### server

The `server` block configures the HTTP and gRPC server of the launched service(s).

```yaml
# (experimental) If non-zero, configures the amount of GRPC server workers used
# to serve the requests.
# CLI flag: -server.grpc.num-workers
[grpc_server_num_workers: <int> | default = 0]
```

### distributor

The `distributor` block configures the distributor.

```yaml
retry_after_header:
  # (experimental) Enabled controls inclusion of the Retry-After header in the
  # response: true includes it for client retry guidance, false omits it.
  # CLI flag: -distributor.retry-after-header.enabled
  [enabled: <boolean> | default = false]

  # (experimental) Base duration in seconds for calculating the Retry-After
  # header in responses to 429/5xx errors.
  # CLI flag: -distributor.retry-after-header.base-seconds
  [base_seconds: <int> | default = 3]

  # (experimental) Sets the upper limit on the number of Retry-Attempt
  # considered for calculation. It caps the Retry-Attempt header without
  # rejecting additional attempts, controlling exponential backoff calculations.
  # For example, when the base-seconds is set to 3 and max-backoff-exponent to
  # 5, the maximum retry duration would be 3 * 2^5 = 96 seconds.
  # CLI flag: -distributor.retry-after-header.max-backoff-exponent
  [max_backoff_exponent: <int> | default = 5]

# (experimental) Enable pooling of buffers used for marshaling write requests.
# CLI flag: -distributor.write-requests-buffer-pooling-enabled
[write_requests_buffer_pooling_enabled: <boolean> | default = false]

# (experimental) Use experimental method of limiting push requests.
# CLI flag: -distributor.limit-inflight-requests-using-grpc-method-limiter
[limit_inflight_requests_using_grpc_method_limiter: <boolean> | default = false]

# (experimental) Number of pre-allocated workers used to forward push requests
# to the ingesters. If 0, no workers will be used and a new goroutine will be
# spawned for each ingester push request. If not enough workers available, new
# goroutine will be spawned. (Note: this is a performance optimization, not a
# limiting feature.)
# CLI flag: -distributor.reusable-ingester-push-workers
[reusable_ingester_push_workers: <int> | default = 0]
```

### ingester

The `ingester` block configures the ingester.

```yaml
ring:
  # (experimental) Specifies the strategy used for generating tokens for
  # ingesters. Supported values are: random,spread-minimizing.
  # CLI flag: -ingester.ring.token-generation-strategy
  [token_generation_strategy: <string> | default = "random"]

  # (experimental) True to allow this ingester registering tokens in the ring
  # only after all previous ingesters (with ID lower than the current one) have
  # already been registered. This configuration option is supported only when
  # the token generation strategy is set to "spread-minimizing".
  # CLI flag: -ingester.ring.spread-minimizing-join-ring-in-order
  [spread_minimizing_join_ring_in_order: <boolean> | default = false]

  # (experimental) Comma-separated list of zones in which spread minimizing
  # strategy is used for token generation. This value must include all zones in
  # which ingesters are deployed, and must not change over time. This
  # configuration is used only when "token-generation-strategy" is set to
  # "spread-minimizing".
  # CLI flag: -ingester.ring.spread-minimizing-zones
  [spread_minimizing_zones: <string> | default = ""]

# (experimental) Period with which to update the per-tenant TSDB configuration.
# CLI flag: -ingester.tsdb-config-update-period
[tsdb_config_update_period: <duration> | default = 15s]

# (experimental) CPU utilization limit, as CPU cores, for CPU/memory utilization
# based read request limiting. Use 0 to disable it.
# CLI flag: -ingester.read-path-cpu-utilization-limit
[read_path_cpu_utilization_limit: <float> | default = 0]

# (experimental) Memory limit, in bytes, for CPU/memory utilization based read
# request limiting. Use 0 to disable it.
# CLI flag: -ingester.read-path-memory-utilization-limit
[read_path_memory_utilization_limit: <int> | default = 0]

# (experimental) Enable logging of utilization based limiter CPU samples.
# CLI flag: -ingester.log-utilization-based-limiter-cpu-samples
[log_utilization_based_limiter_cpu_samples: <boolean> | default = false]

# (experimental) Use experimental method of limiting push requests.
# CLI flag: -ingester.limit-inflight-requests-using-grpc-method-limiter
[limit_inflight_requests_using_grpc_method_limiter: <boolean> | default = false]

# (experimental) Each error will be logged once in this many times. Use 0 to log
# all of them.
# CLI flag: -ingester.error-sample-rate
[error_sample_rate: <int> | default = 0]

# (experimental) When enabled only gRPC errors will be returned by the ingester.
# CLI flag: -ingester.return-only-grpc-errors
[return_only_grpc_errors: <boolean> | default = false]

# (experimental) When enabled, only series currently owned by ingester according
# to the ring are used when checking user per-tenant series limit.
# CLI flag: -ingester.use-ingester-owned-series-for-limits
[use_ingester_owned_series_for_limits: <boolean> | default = false]

# (experimental) This option enables tracking of ingester-owned series based on
# ring state, even if -ingester.use-ingester-owned-series-for-limits is
# disabled.
# CLI flag: -ingester.track-ingester-owned-series
[track_ingester_owned_series: <boolean> | default = false]

# (experimental) How often to check for ring changes and possibly recompute
# owned series as a result of detected change.
# CLI flag: -ingester.owned-series-update-interval
[owned_series_update_interval: <duration> | default = 15s]
```

### querier

The `querier` block configures the querier.

```yaml
# (experimental) Request ingesters stream chunks. Ingesters will only respond
# with a stream of chunks if the target ingester supports this, and this
# preference will be ignored by ingesters that do not support this.
# CLI flag: -querier.prefer-streaming-chunks-from-ingesters
[prefer_streaming_chunks_from_ingesters: <boolean> | default = true]

# (experimental) Request store-gateways stream chunks. Store-gateways will only
# respond with a stream of chunks if the target store-gateway supports this, and
# this preference will be ignored by store-gateways that do not support this.
# CLI flag: -querier.prefer-streaming-chunks-from-store-gateways
[prefer_streaming_chunks_from_store_gateways: <boolean> | default = false]

# (experimental) Number of series to buffer per store-gateway when streaming
# chunks from store-gateways.
# CLI flag: -querier.streaming-chunks-per-store-gateway-buffer-size
[streaming_chunks_per_store_gateway_series_buffer_size: <int> | default = 256]

# (experimental) If true, when querying ingesters, only the minimum required
# ingesters required to reach quorum will be queried initially, with other
# ingesters queried only if needed due to failures from the initial set of
# ingesters. Enabling this option reduces resource consumption for the happy
# path at the cost of increased latency for the unhappy path.
# CLI flag: -querier.minimize-ingester-requests
[minimize_ingester_requests: <boolean> | default = true]
```

### frontend

The `frontend` block configures the query-frontend.

```yaml
# (experimental) If a querier disconnects without sending notification about
# graceful shutdown, the query-frontend will keep the querier in the tenant's
# shard until the forget delay has passed. This feature is useful to reduce the
# blast radius when shuffle-sharding is enabled.
# CLI flag: -query-frontend.querier-forget-delay
[querier_forget_delay: <duration> | default = 0s]

# Configures the gRPC client used to communicate between the query-frontends and
# the query-schedulers.
# The CLI flags prefix for this block configuration is:
# query-frontend.grpc-client-config
[grpc_client_config: <grpc_client>]

results_cache:
  # The memcached block configures the Memcached-based caching backend.
  # The CLI flags prefix for this block configuration is:
  # query-frontend.results-cache
  [memcached: <memcached>]

# (experimental) Maximum time to wait for the query-frontend to become ready
# before rejecting requests received before the frontend was ready. 0 to disable
# (i.e. fail immediately if a request is received while the frontend is still
# starting up)
# CLI flag: -query-frontend.not-running-timeout
[not_running_timeout: <duration> | default = 0s]
```

### query_scheduler

The `query_scheduler` block configures the query-scheduler.

```yaml
# (experimental) If a querier disconnects without sending notification about
# graceful shutdown, the query-scheduler will keep the querier in the tenant's
# shard until the forget delay has passed. This feature is useful to reduce the
# blast radius when shuffle-sharding is enabled.
# CLI flag: -query-scheduler.querier-forget-delay
[querier_forget_delay: <duration> | default = 0s]

# This configures the gRPC client used to report errors back to the
# query-frontend.
# The CLI flags prefix for this block configuration is:
# query-scheduler.grpc-client-config
[grpc_client_config: <grpc_client>]

# (experimental) Service discovery mode that query-frontends and queriers use to
# find query-scheduler instances. When query-scheduler ring-based service
# discovery is enabled, this option needs be set on query-schedulers,
# query-frontends and queriers. Supported values are: dns, ring.
# CLI flag: -query-scheduler.service-discovery-mode
[service_discovery_mode: <string> | default = "dns"]
```

### ruler

The `ruler` block configures the ruler.

```yaml
# Configures the gRPC client used to communicate between ruler instances.
# The CLI flags prefix for this block configuration is: ruler.client
[ruler_client: <grpc_client>]

query_frontend:
  # Configures the gRPC client used to communicate between the rulers and
  # query-frontends.
  # The CLI flags prefix for this block configuration is:
  # ruler.query-frontend.grpc-client-config
  [grpc_client_config: <grpc_client>]
```

### ruler_storage

The `ruler_storage` block configures the ruler storage backend.

```yaml
# The s3_backend block configures the connection to Amazon S3 object storage
# backend.
# The CLI flags prefix for this block configuration is: ruler-storage
[s3: <s3_storage_backend>]

cache:
  # The memcached block configures the Memcached-based caching backend.
  # The CLI flags prefix for this block configuration is: ruler-storage.cache
  [memcached: <memcached>]
```

### alertmanager

The `alertmanager` block configures the alertmanager.

```yaml
alertmanager_client:
  # (experimental) Initial stream window size. Values less than the default are
  # not supported and are ignored. Setting this to a value other than the
  # default disables the BDP estimator.
  # CLI flag: -alertmanager.alertmanager-client.initial-stream-window-size
  [initial_stream_window_size: <int> | default = 63KiB1023B]

  # (experimental) Initial connection window size. Values less than the default
  # are not supported and are ignored. Setting this to a value other than the
  # default disables the BDP estimator.
  # CLI flag: -alertmanager.alertmanager-client.initial-connection-window-size
  [initial_connection_window_size: <int> | default = 63KiB1023B]
```

### alertmanager_storage

The `alertmanager_storage` block configures the alertmanager storage backend.

```yaml
# The s3_backend block configures the connection to Amazon S3 object storage
# backend.
# The CLI flags prefix for this block configuration is: alertmanager-storage
[s3: <s3_storage_backend>]
```

### ingester_client

The `ingester_client` block configures how the distributors connect to the ingesters.

```yaml
# Configures the gRPC client used to communicate with ingesters from
# distributors, queriers and rulers.
# The CLI flags prefix for this block configuration is: ingester.client
[grpc_client_config: <grpc_client>]

circuit_breaker:
  # (experimental) Enable circuit breaking when making requests to ingesters
  # CLI flag: -ingester.client.circuit-breaker.enabled
  [enabled: <boolean> | default = false]

  # (experimental) Max percentage of requests that can fail over period before
  # the circuit breaker opens
  # CLI flag: -ingester.client.circuit-breaker.failure-threshold
  [failure_threshold: <int> | default = 10]

  # (experimental) How many requests must have been executed in period for the
  # circuit breaker to be eligible to open for the rate of failures
  # CLI flag: -ingester.client.circuit-breaker.failure-execution-threshold
  [failure_execution_threshold: <int> | default = 100]

  # (experimental) Moving window of time that the percentage of failed requests
  # is computed over
  # CLI flag: -ingester.client.circuit-breaker.thresholding-period
  [thresholding_period: <duration> | default = 1m]

  # (experimental) How long the circuit breaker will stay in the open state
  # before allowing some requests
  # CLI flag: -ingester.client.circuit-breaker.cooldown-period
  [cooldown_period: <duration> | default = 1m]
```

### grpc_client

The `grpc_client` block configures the gRPC client used to communicate between two Mimir components. The supported CLI flags `<prefix>` used to reference this configuration block are:

- `ingester.client`
- `querier.frontend-client`
- `querier.scheduler-client`
- `query-frontend.grpc-client-config`
- `query-scheduler.grpc-client-config`
- `ruler.client`
- `ruler.query-frontend.grpc-client-config`

&nbsp;

```yaml
# (experimental) Initial stream window size. Values less than the default are
# not supported and are ignored. Setting this to a value other than the default
# disables the BDP estimator.
# CLI flag: -<prefix>.initial-stream-window-size
[initial_stream_window_size: <int> | default = 63KiB1023B]

# (experimental) Initial connection window size. Values less than the default
# are not supported and are ignored. Setting this to a value other than the
# default disables the BDP estimator.
# CLI flag: -<prefix>.initial-connection-window-size
[initial_connection_window_size: <int> | default = 63KiB1023B]
```

### frontend_worker

The `frontend_worker` block configures the worker running within the querier, picking up and executing queries enqueued by the query-frontend or the query-scheduler.

```yaml
# Configures the gRPC client used to communicate between the querier and the
# query-frontend.
# The CLI flags prefix for this block configuration is: querier.frontend-client
[grpc_client_config: <grpc_client>]

# Configures the gRPC client used to communicate between the querier and the
# query-scheduler.
# The CLI flags prefix for this block configuration is: querier.scheduler-client
[query_scheduler_grpc_client_config: <grpc_client>]
```

### limits

The `limits` block configures default and per-tenant limits imposed by components.

```yaml
# (experimental) Per-tenant burst factor which is the maximum burst size allowed
# as a multiple of the per-tenant ingestion rate, this burst-factor must be
# greater than or equal to 1. If this is set it will override the
# ingestion-burst-size option.
# CLI flag: -distributor.ingestion-burst-factor
[ingestion_burst_factor: <float> | default = 0]

# (experimental) List of metric relabel configurations. Note that in most
# situations, it is more effective to use metrics relabeling directly in the
# Prometheus server, e.g. remote_write.write_relabel_configs. Labels available
# during the relabeling phase and cleaned afterwards: __meta_tenant_id
[metric_relabel_configs: <relabel_config...> | default = ]

# (experimental) If enabled, rate limit errors will be reported to the client
# with HTTP status code 529 (Service is overloaded). If disabled, status code
# 429 (Too Many Requests) is used. Enabling
# -distributor.retry-after-header.enabled before utilizing this option is
# strongly recommended as it helps prevent premature request retries by the
# client.
# CLI flag: -distributor.service-overload-status-code-on-rate-limit-enabled
[service_overload_status_code_on_rate_limit_enabled: <boolean> | default = false]

# (experimental) The maximum number of exemplars in memory, across the cluster.
# 0 to disable exemplars ingestion.
# CLI flag: -ingester.max-global-exemplars-per-user
[max_global_exemplars_per_user: <int> | default = 0]

# (experimental) Enable ingestion of native histogram samples. If false, native
# histogram samples are ignored without an error. To query native histograms
# with query-sharding enabled make sure to set
# -query-frontend.query-result-response-format to 'protobuf'.
# CLI flag: -ingester.native-histograms-ingestion-enabled
[native_histograms_ingestion_enabled: <boolean> | default = false]

# (experimental) Non-zero value enables out-of-order support for most recent
# samples that are within the time window in relation to the TSDB's maximum
# time, i.e., within [db.maxTime-timeWindow, db.maxTime]). The ingester will
# need more memory as a factor of rate of out-of-order samples being ingested
# and the number of series that are getting out-of-order samples. If query falls
# into this window, cached results will use value from
# -query-frontend.results-cache-ttl-for-out-of-order-time-window option to
# specify TTL for resulting cache entry.
# CLI flag: -ingester.out-of-order-time-window
[out_of_order_time_window: <duration> | default = 0s]

# (experimental) Whether the shipper should label out-of-order blocks with an
# external label before uploading them. Setting this label will compact
# out-of-order blocks separately from non-out-of-order blocks
# CLI flag: -ingester.out-of-order-blocks-external-label-enabled
[out_of_order_blocks_external_label_enabled: <boolean> | default = false]

# (experimental) Label used to define the group label for metrics separation.
# For each write request, the group is obtained from the first non-empty group
# label from the first timeseries in the incoming list of timeseries. Specific
# distributor and ingester metrics will be further separated adding a 'group'
# label with group label's value. Currently applies to the following metrics:
# cortex_discarded_samples_total
# CLI flag: -validation.separate-metrics-group-label
[separate_metrics_group_label: <string> | default = ""]

# (experimental) Maximum number of chunks estimated to be fetched in a single
# query from ingesters and long-term storage, as a multiple of
# -querier.max-fetched-chunks-per-query. This limit is enforced in the querier.
# Must be greater than or equal to 1, or 0 to disable.
# CLI flag: -querier.max-estimated-fetched-chunks-per-query-multiplier
[max_estimated_fetched_chunks_per_query_multiplier: <float> | default = 0]

# (experimental) Split instant queries by an interval and execute in parallel. 0
# to disable it.
# CLI flag: -query-frontend.split-instant-queries-by-interval
[split_instant_queries_by_interval: <duration> | default = 0s]

# (experimental) List of queries to block.
[blocked_queries: <blocked_queries_config...> | default = ]

# (experimental) Controls whether recording rules evaluation is enabled. This
# configuration option can be used to forcefully disable recording rules
# evaluation on a per-tenant basis.
# CLI flag: -ruler.recording-rules-evaluation-enabled
[ruler_recording_rules_evaluation_enabled: <boolean> | default = true]

# (experimental) Controls whether alerting rules evaluation is enabled. This
# configuration option can be used to forcefully disable alerting rules
# evaluation on a per-tenant basis.
# CLI flag: -ruler.alerting-rules-evaluation-enabled
[ruler_alerting_rules_evaluation_enabled: <boolean> | default = true]
```

### blocks_storage

The `blocks_storage` block configures the blocks storage.

```yaml
# The s3_backend block configures the connection to Amazon S3 object storage
# backend.
# The CLI flags prefix for this block configuration is: blocks-storage
[s3: <s3_storage_backend>]

# This configures how the querier and store-gateway discover and synchronize
# blocks stored in the bucket.
bucket_store:
  index_cache:
    # The memcached block configures the Memcached-based caching backend.
    # The CLI flags prefix for this block configuration is:
    # blocks-storage.bucket-store.index-cache
    [memcached: <memcached>]

  chunks_cache:
    # The memcached block configures the Memcached-based caching backend.
    # The CLI flags prefix for this block configuration is:
    # blocks-storage.bucket-store.chunks-cache
    [memcached: <memcached>]

  metadata_cache:
    # The memcached block configures the Memcached-based caching backend.
    # The CLI flags prefix for this block configuration is:
    # blocks-storage.bucket-store.metadata-cache
    [memcached: <memcached>]

  index_header:
    # (experimental) If enabled, store-gateway will periodically persist block
    # IDs of lazy loaded index-headers and load them eagerly during startup.
    # Ignored if index-header lazy loading is disabled.
    # CLI flag: -blocks-storage.bucket-store.index-header.eager-loading-startup-enabled
    [eager_loading_startup_enabled: <boolean> | default = true]

    # (experimental) Maximum number of concurrent index header loads across all
    # tenants. If set to 0, concurrency is unlimited.
    # CLI flag: -blocks-storage.bucket-store.index-header.lazy-loading-concurrency
    [lazy_loading_concurrency: <int> | default = 4]

    # (experimental) If enabled, store-gateway will persist a sparse version of
    # the index-header to disk on construction and load sparse index-headers
    # from disk instead of the whole index-header.
    # CLI flag: -blocks-storage.bucket-store.index-header.sparse-persistence-enabled
    [sparse_persistence_enabled: <boolean> | default = true]

  # (experimental) This option controls the strategy to selection of series and
  # deferring application of matchers. A more aggressive strategy will fetch
  # less posting lists at the cost of more series. This is useful when querying
  # large blocks in which many series share the same label name and value.
  # Supported values (most aggressive to least aggressive): speculative,
  # worst-case, worst-case-small-posting-lists, all.
  # CLI flag: -blocks-storage.bucket-store.series-selection-strategy
  [series_selection_strategy: <string> | default = "worst-case"]

  series_selection_strategies:
    # (experimental) This option is only used when
    # blocks-storage.bucket-store.series-selection-strategy=worst-case.
    # Increasing the series preference results in fetching more series than
    # postings. Must be a positive floating point number.
    # CLI flag: -blocks-storage.bucket-store.series-selection-strategies.worst-case-series-preference
    [worst_case_series_preference: <float> | default = 0.75]

tsdb:
  # (experimental) How much variance (as percentage between 0 and 1) should be
  # applied to the chunk end time, to spread chunks writing across time. Doesn't
  # apply to the last chunk of the chunk range. 0 means no variance.
  # CLI flag: -blocks-storage.tsdb.head-chunks-end-time-variance
  [head_chunks_end_time_variance: <float> | default = 0]

  # (experimental) True to enable snapshotting of in-memory TSDB data on disk
  # when shutting down.
  # CLI flag: -blocks-storage.tsdb.memory-snapshot-on-shutdown
  [memory_snapshot_on_shutdown: <boolean> | default = false]

  # (experimental) Maximum capacity for out of order chunks, in samples between
  # 1 and 255.
  # CLI flag: -blocks-storage.tsdb.out-of-order-capacity-max
  [out_of_order_capacity_max: <int> | default = 32]

  # (experimental) How long to cache postings for matchers in the Head and
  # OOOHead. 0 disables the cache and just deduplicates the in-flight calls.
  # CLI flag: -blocks-storage.tsdb.head-postings-for-matchers-cache-ttl
  [head_postings_for_matchers_cache_ttl: <duration> | default = 10s]

  # (experimental) Maximum size in bytes of the cache for postings for matchers
  # in the Head and OOOHead when TTL is greater than 0.
  # CLI flag: -blocks-storage.tsdb.head-postings-for-matchers-cache-max-bytes
  [head_postings_for_matchers_cache_max_bytes: <int> | default = 104857600]

  # (experimental) Force the cache to be used for postings for matchers in the
  # Head and OOOHead, even if it's not a concurrent (query-sharding) call.
  # CLI flag: -blocks-storage.tsdb.head-postings-for-matchers-cache-force
  [head_postings_for_matchers_cache_force: <boolean> | default = false]

  # (experimental) How long to cache postings for matchers in each compacted
  # block queried from the ingester. 0 disables the cache and just deduplicates
  # the in-flight calls.
  # CLI flag: -blocks-storage.tsdb.block-postings-for-matchers-cache-ttl
  [block_postings_for_matchers_cache_ttl: <duration> | default = 10s]

  # (experimental) Maximum size in bytes of the cache for postings for matchers
  # in each compacted block when TTL is greater than 0.
  # CLI flag: -blocks-storage.tsdb.block-postings-for-matchers-cache-max-bytes
  [block_postings_for_matchers_cache_max_bytes: <int> | default = 104857600]

  # (experimental) Force the cache to be used for postings for matchers in
  # compacted blocks, even if it's not a concurrent (query-sharding) call.
  # CLI flag: -blocks-storage.tsdb.block-postings-for-matchers-cache-force
  [block_postings_for_matchers_cache_force: <boolean> | default = false]

  # (experimental) When the number of in-memory series in the ingester is equal
  # to or greater than this setting, the ingester tries to compact the TSDB
  # Head. The early compaction removes from the memory all samples and inactive
  # series up until -ingester.active-series-metrics-idle-timeout time ago. After
  # an early compaction, the ingester will not accept any sample with a
  # timestamp older than -ingester.active-series-metrics-idle-timeout time ago
  # (unless out of order ingestion is enabled). The ingester checks every
  # -blocks-storage.tsdb.head-compaction-interval whether an early compaction is
  # required. Use 0 to disable it.
  # CLI flag: -blocks-storage.tsdb.early-head-compaction-min-in-memory-series
  [early_head_compaction_min_in_memory_series: <int> | default = 0]

  # (experimental) When the early compaction is enabled, the early compaction is
  # triggered only if the estimated series reduction is at least the configured
  # percentage (0-100).
  # CLI flag: -blocks-storage.tsdb.early-head-compaction-min-estimated-series-reduction-percentage
  [early_head_compaction_min_estimated_series_reduction_percentage: <int> | default = 15]
```

### compactor

The `compactor` block configures the compactor component.

```yaml
# (experimental) If enabled, will delete the bucket-index, markers and debug
# files in the tenant bucket when there are no blocks left in the index.
# CLI flag: -compactor.no-blocks-file-cleanup-enabled
[no_blocks_file_cleanup_enabled: <boolean> | default = false]
```

### memcached

The `memcached` block configures the Memcached-based caching backend. The supported CLI flags `<prefix>` used to reference this configuration block are:

- `blocks-storage.bucket-store.chunks-cache`
- `blocks-storage.bucket-store.index-cache`
- `blocks-storage.bucket-store.metadata-cache`
- `query-frontend.results-cache`
- `ruler-storage.cache`

&nbsp;

```yaml
# (experimental) The size of the write buffer (in bytes). The buffer is
# allocated for each connection to memcached.
# CLI flag: -<prefix>.memcached.write-buffer-size-bytes
[write_buffer_size_bytes: <int> | default = 4096]

# (experimental) The size of the read buffer (in bytes). The buffer is allocated
# for each connection to memcached.
# CLI flag: -<prefix>.memcached.read-buffer-size-bytes
[read_buffer_size_bytes: <int> | default = 4096]
```

### s3_storage_backend

The s3_backend block configures the connection to Amazon S3 object storage backend. The supported CLI flags `<prefix>` used to reference this configuration block are:

- `alertmanager-storage`
- `blocks-storage`
- `common.storage`
- `ruler-storage`

&nbsp;

```yaml
# (experimental) The S3 storage class to use, not set by default. Details can be
# found at https://aws.amazon.com/s3/storage-classes/. Supported values are:
# STANDARD, REDUCED_REDUNDANCY, GLACIER, STANDARD_IA, ONEZONE_IA,
# INTELLIGENT_TIERING, DEEP_ARCHIVE, OUTPOSTS, GLACIER_IR, SNOW, EXPRESS_ONEZONE
# CLI flag: -<prefix>.s3.storage-class
[storage_class: <string> | default = ""]

# (experimental) If enabled, it will use the default authentication methods of
# the AWS SDK for go based on known environment variables and known AWS config
# files.
# CLI flag: -<prefix>.s3.native-aws-auth-enabled
[native_aws_auth_enabled: <boolean> | default = false]

# (experimental) The minimum file size in bytes used for multipart uploads. If
# 0, the value is optimally computed for each object.
# CLI flag: -<prefix>.s3.part-size
[part_size: <int> | default = 0]

# (experimental) If enabled, a Content-MD5 header is sent with S3 Put Object
# requests. Consumes more resources to compute the MD5, but may improve
# compatibility with object storage services that do not support checksums.
# CLI flag: -<prefix>.s3.send-content-md5
[send_content_md5: <boolean> | default = false]
```

## Deprecated features

Deprecated features are usable up until the release that indicates their removal.

The following features or configuration parameters are currently deprecated and will be **removed in Mimir 2.11**:

```yaml
# The blocks_storage block configures the blocks storage.
[blocks_storage: <blocks_storage>]
```

### blocks_storage

The `blocks_storage` block configures the blocks storage.

```yaml
# This configures how the querier and store-gateway discover and synchronize
# blocks stored in the bucket.
bucket_store:
  # (deprecated) If enabled, store-gateway will lazy load an index-header only
  # once required by a query.
  # CLI flag: -blocks-storage.bucket-store.index-header-lazy-loading-enabled
  [index_header_lazy_loading_enabled: <boolean> | default = true]

  # (deprecated) If index-header lazy loading is enabled and this setting is >
  # 0, the store-gateway will offload unused index-headers after 'idle timeout'
  # inactivity.
  # CLI flag: -blocks-storage.bucket-store.index-header-lazy-loading-idle-timeout
  [index_header_lazy_loading_idle_timeout: <duration> | default = 1h]

tsdb:
  # (deprecated) Maximum number of entries in the cache for postings for
  # matchers in the Head and OOOHead when TTL is greater than 0.
  # CLI flag: -blocks-storage.tsdb.head-postings-for-matchers-cache-size
  [head_postings_for_matchers_cache_size: <int> | default = 100]

  # (deprecated) Maximum number of entries in the cache for postings for
  # matchers in each compacted block when TTL is greater than 0.
  # CLI flag: -blocks-storage.tsdb.block-postings-for-matchers-cache-size
  [block_postings_for_matchers_cache_size: <int> | default = 100]
```
