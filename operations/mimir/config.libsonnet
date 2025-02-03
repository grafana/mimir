{
  _config+: {
    namespace: error 'must define namespace',
    cluster: error 'must define cluster',
    cluster_domain:: 'cluster.local.',
    replication_factor: 3,
    external_url: error 'must define external url for cluster',

    node_selector: null,

    aws_region: error 'must specify AWS region',

    // The deployment mode to use. Supported values are:
    // `microservices`: Provides only the k8s objects for each component as microservices.
    // `read-write`: Provides only mimir-read, mimir-write, and mimir-backend k8s objects.
    // `migration`: Provides both the microservices and read-write services.
    deployment_mode: 'microservices',
    is_microservices_deployment_mode: $._config.deployment_mode == 'microservices' || $._config.deployment_mode == 'migration',
    is_read_write_deployment_mode: $._config.deployment_mode == 'read-write' || $._config.deployment_mode == 'migration',

    // If false, ingesters are not unregistered on shutdown and left in the ring with
    // the LEAVING state. Setting to false prevents series resharding during ingesters rollouts,
    // but requires to:
    // 1. Either manually forget ingesters on scale down or invoke the /ingester/shutdown endpoint
    // 2. Ensure ingester ID is preserved during rollouts
    unregister_ingesters_on_shutdown: true,

    // Controls whether multiple pods for the same service can be scheduled on the same node.
    // Distributing the pods over different nodes improves performance and also realiability,
    // especially important in case of ingester where losing multiple ingesters can cause data loss.
    ingester_allow_multiple_replicas_on_same_node: false,
    store_gateway_allow_multiple_replicas_on_same_node: false,

    // Controls the max skew for pod topology spread constraints. Set to a negative value to
    // not configure the pod topology spread constraints at all.
    // See: https://kubernetes.io/docs/concepts/workloads/pods/pod-topology-spread-constraints/
    distributor_topology_spread_max_skew: 1,
    query_frontend_topology_spread_max_skew: 1,
    querier_topology_spread_max_skew: 1,
    ruler_topology_spread_max_skew: 1,
    ruler_querier_topology_spread_max_skew: 1,

    // Controls how many concurrent queries are run in the querier.
    querier_max_concurrency: 8,
    ruler_querier_max_concurrency: $._config.querier_max_concurrency,

    test_exporter_enabled: false,
    test_exporter_start_time: error 'must specify test exporter start time',
    test_exporter_user_id: error 'must specify test exporter used id',

    // storage_backend will be used for all components that use block storage.
    // Each component can override this by specific CLI flags.
    // See https://grafana.com/docs/mimir/latest/configure/about-configurations/#common-configurations
    storage_backend: error 'should specify storage backend',  // Available options are 'gcs', 's3', 'azure'.

    // GCS authentication can be configured by setting a non-null service account value, which will be then rendered
    // as a CLI flag. Please note that there are alternative ways of configuring GCS authentication:
    // See https://grafana.com/docs/mimir/latest/reference-configuration-parameters/#gcs_storage_backend
    // See https://cloud.google.com/storage/docs/authentication#libauth
    storage_gcs_service_account: null,

    // S3 credentials are optional and will be only set as CLI flags if not null.
    // This is useful because S3 can be accessed without credentials under certain conditions.
    // See: https://aws.amazon.com/premiumsupport/knowledge-center/s3-private-connection-no-authentication/
    storage_s3_secret_access_key: null,
    storage_s3_access_key_id: null,
    storage_s3_endpoint: 's3.dualstack.%(aws_region)s.amazonaws.com' % $._config,

    // Azure credentials are required by the client implementation when azure is used.
    storage_azure_account_name: error 'must specify Azure account name',
    storage_azure_account_key: error 'must specify Azure account key',

    jaeger_agent_host: null,

    // Allow to configure the alertmanager disk.
    alertmanager_data_disk_size: '100Gi',
    alertmanager_data_disk_class: null,

    // Allow to configure the ingester disk.
    ingester_data_disk_size: '100Gi',
    ingester_data_disk_class: 'fast',

    // Allow to configure the store-gateway disk.
    store_gateway_data_disk_size: '50Gi',
    store_gateway_data_disk_class: 'standard',

    // Allow to configure the compactor disk.
    compactor_data_disk_size: '250Gi',
    compactor_data_disk_class: 'standard',

    // Allow to fine tune compactor.
    compactor_max_concurrency: 1,
    // While this is the default value, we want to pass the same to the -blocks-storage.bucket-store.sync-interval
    compactor_cleanup_interval: '15m',

    // Enable concurrent rollout of compactor through the usage of the rollout operator.
    // This feature modifies the compactor StatefulSet which cannot be altered, so if it already exists it has to be deleted and re-applied again in order to be enabled.
    cortex_compactor_concurrent_rollout_enabled: false,
    // Maximum number of unavailable replicas during a compactor rollout when using cortex_compactor_concurrent_rollout_enabled feature.
    // Computed from compactor replicas by default, but can also be specified as percentage, for example "25%".
    cortex_compactor_max_unavailable: std.max(std.floor($.compactor_statefulset.spec.replicas / 2), 1),

    // Enable use of bucket index by querier, ruler and store-gateway.
    bucket_index_enabled: true,

    store_gateway_replication_factor: 3,

    // When store_gateway_lazy_loading_enabled: true, block index-headers are pre-downloaded but lazy loaded at query time.
    // Enabling lazy loading results in faster startup times at the cost of some latency during query time.
    store_gateway_lazy_loading_enabled: true,

    // Number of memcached replicas for each memcached statefulset
    memcached_frontend_replicas: 3,
    memcached_index_queries_replicas: 3,
    memcached_chunks_replicas: 3,
    memcached_metadata_replicas: 1,

    cache_frontend_enabled: true,
    cache_frontend_max_item_size_mb: 5,
    cache_frontend_connection_limit: 16384,
    memcached_frontend_mtls_enabled: false,

    cache_index_queries_enabled: true,
    cache_index_queries_max_item_size_mb: 5,
    cache_index_queries_connection_limit: 16384,
    memcached_index_queries_mtls_enabled: false,

    cache_chunks_enabled: true,
    cache_chunks_max_item_size_mb: 1,
    cache_chunks_connection_limit: 16384,
    memcached_chunks_mtls_enabled: false,

    cache_metadata_enabled: true,
    cache_metadata_max_item_size_mb: 1,
    cache_metadata_connection_limit: 16384,
    memcached_metadata_mtls_enabled: false,

    // mTLS can be used for connections to each cache cluster. If enabled for each type of
    // cache, the same CA, server, and client credentials are used. That is, you cannot use
    // different credentials for the frontend, chunks, index, and metadata caches.
    // NOTE: The certificates in the secrets must be under a key named "$SECRET.pem" where
    // $SECRET is the name of the secret itself. For example, if the CA cert secret is
    // named "memcached-ca", the cert must be under the key "memcached-ca.pem".
    memcached_mtls_server_name: null,
    memcached_mtls_ca_cert_secret: error 'CA cert secret must be set',
    memcached_mtls_server_cert_secret: error 'server cert secret must be set',
    memcached_mtls_server_key_secret: error 'server key secret must be set',
    memcached_mtls_client_cert_secret: error 'client cert secret must be set',
    memcached_mtls_client_key_secret: error 'client key secret must be set',

    // Paths within containers that secrets are mounted at. It shouldn't be necessary to
    // change these settings.
    memcached_ca_cert_path: '/var/secrets/memcached-ca-cert/',
    memcached_client_key_path: '/var/secrets/memcached-client-key/',
    memcached_client_cert_path: '/var/secrets/memcached-client-cert/',
    memcached_server_key_path: '/var/secrets/memcached-server-key/',
    memcached_server_cert_path: '/var/secrets/memcached-server-cert/',

    // Number of etcd replicas.
    etcd_replicas: 3,

    // The query-tee is an optional service which can be used to send
    // the same input query to multiple backends and make them compete
    // (comparing performances).
    query_tee_enabled: false,
    query_tee_backend_endpoints: [],
    query_tee_backend_preferred: '',
    query_tee_node_port: null,

    // Common configuration parameters
    commonConfig:: {},

    // usage_stats_enabled enables the reporting of anonymous usage statistics about the Mimir installation.
    // For more details about usage statistics, see:
    // https://grafana.com/docs/mimir/latest/configure/about-anonymous-usage-statistics-reporting/
    usage_stats_enabled: true,
    usageStatsConfig: if !$._config.usage_stats_enabled then {} else {
      'usage-stats.installation-mode': 'jsonnet',
    },

    grpcConfig:: {
      'server.grpc.keepalive.min-time-between-pings': '10s',
      'server.grpc.keepalive.ping-without-stream-allowed': true,
    },

    // gRPC server configuration to apply to ingress services used by clients doing
    // client-side load balancing in front of it. Since gRPC clients re-resolve the configured
    // address when a connection fails or is closed, we do force the clients to reconnect
    // periodically in order to have them re-resolve the configured address and eventually
    // discover new replicas (e.g. after a scale up event).
    grpcIngressConfig:: {
      'server.grpc.keepalive.max-connection-age': '2m',
      'server.grpc.keepalive.max-connection-age-grace': '5m',
      'server.grpc.keepalive.max-connection-idle': '1m',
    },

    storageConfig: {
      'common.storage.backend': $._config.storage_backend,
    } + (
      if $._config.storage_backend == 's3' then {
        'common.storage.s3.endpoint': $._config.storage_s3_endpoint,
        'common.storage.s3.access-key-id': $._config.storage_s3_access_key_id,
        'common.storage.s3.secret-access-key': $._config.storage_s3_secret_access_key,
      }
      else if $._config.storage_backend == 'azure' then {
        'common.storage.azure.account-name': $._config.storage_azure_account_name,
        'common.storage.azure.account-key': $._config.storage_azure_account_key,
      }
      else if $._config.storage_backend == 'gcs' then {
        'common.storage.gcs.service-account': $._config.storage_gcs_service_account,
      }
      else {}
    ),

    blocks_storage_bucket_name: error 'must specify blocks storage bucket name',

    blocksStorageConfig: {
      [
      if $._config.storage_backend == 'gcs' then 'blocks-storage.gcs.bucket-name'
      else if $._config.storage_backend == 's3' then 'blocks-storage.s3.bucket-name'
      else if $._config.storage_backend == 'azure' then 'blocks-storage.azure.container-name'
      ]: $._config.blocks_storage_bucket_name,
    },

    queryBlocksStorageConfig:: {
      'blocks-storage.bucket-store.sync-dir': '/data/tsdb',

      'store-gateway.sharding-ring.store': 'consul',
      'store-gateway.sharding-ring.consul.hostname': 'consul.%(namespace)s.svc.%(cluster_domain)s:8500' % $._config,
      'store-gateway.sharding-ring.prefix': '',
      'store-gateway.sharding-ring.replication-factor': $._config.store_gateway_replication_factor,

      // Relax pressure on KV store when running at scale.
      // When changing this, please remember to also change the hearbeat period defined in store_gateway_args.
      'store-gateway.sharding-ring.heartbeat-timeout': '4m',
    },

    // Querier component config (shared between the ruler and querier).
    queryConfig: {
      // Don't allow individual queries of longer than 32days.  Due to day query
      // splitting in the frontend, the reality is this only limits rate(foo[32d])
      // type queries. 32 days to allow for comparision over the last month (31d) and
      // then some.
      'querier.max-partial-query-length': '768h',
    } + $.mimirRuntimeConfigFile,

    // PromQL query engine config (shared between all services running PromQL engine, like the ruler and querier).
    queryEngineConfig: {},

    // The ingester ring client config that should be shared across all Mimir services
    // using or watching the ingester ring.
    ingesterRingClientConfig: {
      'ingester.ring.consul.hostname': 'consul.%(namespace)s.svc.%(cluster_domain)s:8500' % $._config,
      'ingester.ring.replication-factor': $._config.replication_factor,
      'distributor.health-check-ingesters': true,
      'ingester.ring.store': 'consul',
      'ingester.ring.prefix': '',

      // Relax pressure on KV store when running at scale.
      // When changing this, please remember to also change the hearbeat period defined in ingester_args.
      'ingester.ring.heartbeat-timeout': '10m',
    },

    local querySchedulerRingConfig = {
      'query-scheduler.ring.store': 'consul',
      'query-scheduler.ring.consul.hostname': 'consul.%(namespace)s.svc.%(cluster_domain)s:8500' % $._config,
      'query-scheduler.ring.prefix': '',
    },

    // The query-scheduler ring client config that should be shared across all Mimir services
    // using or watching the query-scheduler ring.
    querySchedulerRingClientConfig:
      if $._config.query_scheduler_service_discovery_mode != 'ring' || !$._config.query_scheduler_service_discovery_ring_read_path_enabled then
        {}
      else
        querySchedulerRingConfig,

    // The query-scheduler ring lifecycler config (set only to the query-scheduler).
    querySchedulerRingLifecyclerConfig:
      if $._config.query_scheduler_service_discovery_mode != 'ring' then
        {}
      else
        querySchedulerRingConfig,

    overridesExporterRingConfig:
      if !$._config.overrides_exporter_ring_enabled then
        {}
      else
        {
          'overrides-exporter.ring.enabled': true,
          'overrides-exporter.ring.store': 'consul',
          'overrides-exporter.ring.consul.hostname': 'consul.%(namespace)s.svc.%(cluster_domain)s:8500' % $._config,
          'overrides-exporter.ring.prefix': '',
          'overrides-exporter.ring.wait-stability-min-duration': '1m',
        },

    ruler_enabled: false,
    ruler_storage_backend: $._config.storage_backend,
    ruler_storage_bucket_name: error 'must specify the ruler storage bucket name',
    ruler_local_directory: error 'you must specify the local directory for ruler storage',

    rulerStorageConfig:
      {
        [
        if $._config.ruler_storage_backend == 'gcs' then 'ruler-storage.gcs.bucket-name'
        else if $._config.ruler_storage_backend == 's3' then 'ruler-storage.s3.bucket-name'
        else if $._config.ruler_storage_backend == 'azure' then 'ruler-storage.azure.container-name'
        ]: $._config.ruler_storage_bucket_name,

        [if $._config.ruler_storage_backend != $._config.storage_backend then 'ruler-storage.backend']: $._config.ruler_storage_backend,
        [if $._config.ruler_storage_backend == 'local' then 'ruler-storage.local.directory']: $._config.ruler_local_directory,
      },

    server_http_port: 8080,

    alertmanager: {
      replicas: 3,
      fallback_config: {},
      ring_store: 'consul',
      ring_hostname: 'consul.%(namespace)s.svc.%(cluster_domain)s:8500' % $._config,
      ring_replication_factor: $._config.replication_factor,
    },

    alertmanager_enabled: false,
    alertmanager_storage_backend: $._config.storage_backend,
    alertmanager_storage_bucket_name: error 'you must specify the alertmanager storage bucket name',
    alertmanager_local_directory: error 'you must specify the local directory for alertmanager storage',

    alertmanagerStorageConfig:
      {
        [
        if $._config.alertmanager_storage_backend == 'gcs' then 'alertmanager-storage.gcs.bucket-name'
        else if $._config.alertmanager_storage_backend == 's3' then 'alertmanager-storage.s3.bucket-name'
        else if $._config.alertmanager_storage_backend == 'azure' then 'alertmanager-storage.azure.container-name'
        ]: $._config.alertmanager_storage_bucket_name,

        [if $._config.alertmanager_storage_backend != $._config.storage_backend then 'alertmanager-storage.backend']: $._config.alertmanager_storage_backend,
        [if $._config.alertmanager_storage_backend == 'local' then 'alertmanager-storage.local.path']: $._config.alertmanager_local_directory,
      },

    // === Per-tenant usage limits. ===
    //
    // These are the defaults.
    limits: $._config.overrides.extra_small_user,

    // These are all the flags for the default limits.
    distributorLimitsConfig: {
      'distributor.ingestion-rate-limit': $._config.limits.ingestion_rate,
      'distributor.ingestion-burst-size': $._config.limits.ingestion_burst_size,
    },
    ingesterLimitsConfig: {
      'ingester.max-global-series-per-user': $._config.limits.max_global_series_per_user,
      'ingester.max-global-metadata-per-user': $._config.limits.max_global_metadata_per_user,
      'ingester.max-global-metadata-per-metric': $._config.limits.max_global_metadata_per_metric,
    },
    rulerLimitsConfig: {
      'ruler.max-rules-per-rule-group': $._config.limits.ruler_max_rules_per_rule_group,
      'ruler.max-rule-groups-per-tenant': $._config.limits.ruler_max_rule_groups_per_tenant,
    },
    compactorLimitsConfig: {
      'compactor.blocks-retention-period': $._config.limits.compactor_blocks_retention_period,
    },

    limitsConfig: self.distributorLimitsConfig + self.ingesterLimitsConfig + self.rulerLimitsConfig + self.compactorLimitsConfig,

    overrides_configmap: 'overrides',
    overrides_configmap_mountpoint: '/etc/mimir',

    // Configmaps mounted to all components. Maps config map name to mount point.
    configmaps: {
      [$._config.overrides_configmap]: $._config.overrides_configmap_mountpoint,
    },

    // Paths to runtime config files. Paths are passed to -runtime-config.files in specified order.
    runtime_config_files: ['%s/overrides.yaml' % $._config.overrides_configmap_mountpoint],

    overrides: {
      extra_small_user:: {
        // Our limit should be 100k, but we need some room of about ~50% to take rollouts into account
        max_global_series_per_user: 150000,
        max_global_metadata_per_user: std.ceil(self.max_global_series_per_user * 0.2),
        max_global_metadata_per_metric: 10,

        ingestion_rate: 10000,
        ingestion_burst_size: 200000,

        // 1400 rules
        ruler_max_rules_per_rule_group: 20,
        ruler_max_rule_groups_per_tenant: 70,

        // No retention for now.
        compactor_blocks_retention_period: '0',
      },

      medium_small_user:: {
        max_global_series_per_user: 300000,
        max_global_metadata_per_user: std.ceil(self.max_global_series_per_user * 0.2),
        max_global_metadata_per_metric: 10,

        ingestion_rate: 30000,
        ingestion_burst_size: 300000,

        // 2000 rules
        ruler_max_rules_per_rule_group: 20,
        ruler_max_rule_groups_per_tenant: 100,
      },

      small_user:: {
        max_global_series_per_user: 1000000,
        max_global_metadata_per_user: std.ceil(self.max_global_series_per_user * 0.2),
        max_global_metadata_per_metric: 10,

        ingestion_rate: 100000,
        ingestion_burst_size: 1000000,

        // 2800 rules
        ruler_max_rules_per_rule_group: 20,
        ruler_max_rule_groups_per_tenant: 140,
      },

      medium_user:: {
        max_global_series_per_user: 3000000,  // 3M
        max_global_metadata_per_user: std.ceil(self.max_global_series_per_user * 0.2),
        max_global_metadata_per_metric: 10,

        ingestion_rate: 350000,  // 350K
        ingestion_burst_size: 3500000,  // 3.5M

        // 3600 rules
        ruler_max_rules_per_rule_group: 20,
        ruler_max_rule_groups_per_tenant: 180,
      },

      big_user:: {
        max_global_series_per_user: 6000000,  // 6M
        max_global_metadata_per_user: std.ceil(self.max_global_series_per_user * 0.2),
        max_global_metadata_per_metric: 10,

        ingestion_rate: 700000,  // 700K
        ingestion_burst_size: 7000000,  // 7M

        // 4400 rules
        ruler_max_rules_per_rule_group: 20,
        ruler_max_rule_groups_per_tenant: 220,
      },

      super_user:: {
        max_global_series_per_user: 12000000,  // 12M
        max_global_metadata_per_user: std.ceil(self.max_global_series_per_user * 0.2),
        max_global_metadata_per_metric: 10,

        ingestion_rate: 1500000,  // 1.5M
        ingestion_burst_size: 15000000,  // 15M

        // 5200 rules
        ruler_max_rules_per_rule_group: 20,
        ruler_max_rule_groups_per_tenant: 260,

        compactor_split_and_merge_shards: 2,
        compactor_tenant_shard_size: 2,
        compactor_split_groups: 2,
      },

      // This user class has limits increased by +50% compared to the previous one.
      mega_user+:: {
        max_global_series_per_user: 16000000,  // 16M
        max_global_metadata_per_user: std.ceil(self.max_global_series_per_user * 0.2),
        max_global_metadata_per_metric: 10,

        ingestion_rate: 2250000,  // 2.25M
        ingestion_burst_size: 22500000,  // 22.5M

        // 6000 rules
        ruler_max_rules_per_rule_group: 20,
        ruler_max_rule_groups_per_tenant: 300,

        compactor_split_and_merge_shards: 2,
        compactor_tenant_shard_size: 2,
        compactor_split_groups: 2,
      },

      user_24M+:: {
        max_global_series_per_user: 24000000,  // 24M
        max_global_metadata_per_user: std.ceil(self.max_global_series_per_user * 0.2),
        max_global_metadata_per_metric: 10,

        ingestion_rate: 3500000,  // 3.5M
        ingestion_burst_size: 35000000,  // 35M

        // 7000 rules
        ruler_max_rules_per_rule_group: 20,
        ruler_max_rule_groups_per_tenant: 350,

        compactor_split_and_merge_shards: 4,
        compactor_tenant_shard_size: 4,
        compactor_split_groups: 4,
      },

      user_32M+:: {
        max_global_series_per_user: 32000000,  // 32M
        max_global_metadata_per_user: std.ceil(self.max_global_series_per_user * 0.2),
        max_global_metadata_per_metric: 10,

        ingestion_rate: 4500000,  // 4.5M
        ingestion_burst_size: 45000000,  // 45M

        // 8000 rules
        ruler_max_rules_per_rule_group: 20,
        ruler_max_rule_groups_per_tenant: 400,

        compactor_split_and_merge_shards: 4,
        compactor_tenant_shard_size: 4,
        compactor_split_groups: 8,
      },
    },

    // if not empty, passed to overrides.yaml as another top-level field
    multi_kv_config: {},

    enable_pod_priorities: true,

    // Enables query-scheduler component, and reconfigures querier and query-frontend to use it.
    query_scheduler_enabled: true,
    query_scheduler_service_discovery_mode: 'dns',  // Supported values: 'dns', 'ring'.

    // Migrating a Mimir cluster from DNS to ring-based service discovery is a two steps process:
    // 1. Set `query_scheduler_service_discovery_mode: 'ring' and `query_scheduler_service_discovery_ring_read_path_enabled: false`,
    //    so that query-schedulers join a ring, but queriers and query-frontends will still discover the query-scheduler via DNS.
    // 2. Remove the setting `query_scheduler_service_discovery_ring_read_path_enabled: false`, so that queriers and query-frontends
    //    will discover the query-schedulers via ring.
    query_scheduler_service_discovery_ring_read_path_enabled: true,

    // Enables streaming of chunks from ingesters using blocks.
    // Changing it will not cause new rollout of ingesters, as it gets passed to them via runtime-config.
    // Default value is true, left here for backwards compatibility until the flag is removed completely.
    ingester_stream_chunks_when_using_blocks: true,

    // Ingester limits are put directly into runtime config, if not null. Available limits:
    //    ingester_instance_limits: {
    //      max_inflight_push_requests: 0,  // Max inflight push requests per ingester. 0 = no limit.
    //      max_ingestion_rate: 0,  // Max ingestion rate (samples/second) per ingester. 0 = no limit.
    //      max_series: 0,  // Max number of series per ingester. 0 = no limit.
    //      max_tenants: 0,  // Max number of tenants per ingester. 0 = no limit.
    //    },
    ingester_instance_limits: null,

    // Distributor limits are put directly into runtime config, if not null. Available limits:
    //    distributor_instance_limits: {
    //      max_ingestion_rate: 0,  // Max ingestion rate (samples/second) per distributor. 0 = no limit.
    //      max_inflight_push_requests: 0,  // Max inflight push requests per distributor. 0 = no limit.
    //      max_inflight_push_requests_bytes: 0,  // Max sum of inflight push request sizes per distributor. 0 = no limit.
    //    },
    distributor_instance_limits: null,

    // Controls whether the TSDB Head early compaction is enabled in the ingester.
    ingester_tsdb_head_early_compaction_enabled: false,
    ingester_tsdb_head_early_compaction_reduction_percentage: 15,

    // The default threshold to triger the TSDB Head early compaction is once the ingester in-memory
    // series reach the 66% of the configured hard limit on max in-memory series. If the limit is not
    // configured, then we just use a constant default value.
    ingester_tsdb_head_early_compaction_min_in_memory_series:
      if $._config.ingester_instance_limits != null && std.objectHas($._config.ingester_instance_limits, 'max_series') then
        std.ceil($._config.ingester_instance_limits.max_series / 1.5)
      else
        2000000,

    gossip_member_label: 'gossip_ring_member',
    // Labels that service selectors should not use
    service_ignored_labels:: [self.gossip_member_label],
  },

  // Check configured deployment mode to ensure configuration is correct and consistent.
  assert std.member(['microservices', 'read-write', 'migration'], $._config.deployment_mode)
         : 'unsupported deployment mode "%s"' % $._config.deployment_mode,
  assert $._config.deployment_mode == 'migration' || ($._config.is_microservices_deployment_mode != $._config.is_read_write_deployment_mode)
         : 'do not explicitly set is_microservices_deployment_mode or is_read_write_deployment_mode, but use deployment_mode config option instead',

  local configMap = $.core.v1.configMap,

  overrides_config:
    configMap.new($._config.overrides_configmap) +
    configMap.withData({
      'overrides.yaml': $.util.manifestYaml(
        {
          // Recursively remove fields whose value has been explicitly set to "null". This technique allow us
          // us to easily remove fields using jsonnet overrides.
          overrides: $.util.removeNulls($._config.overrides),
        }
        + (if std.length($._config.multi_kv_config) > 0 then { multi_kv_config: $._config.multi_kv_config } else {})
        + (if !$._config.ingester_stream_chunks_when_using_blocks then { ingester_stream_chunks_when_using_blocks: false } else {})
        + (if $._config.ingester_instance_limits != null then { ingester_limits: $._config.ingester_instance_limits } else {})
        + (if $._config.distributor_instance_limits != null then { distributor_limits: $._config.distributor_instance_limits } else {}),
      ),
    }),

  // This removed the CPU limit from the config.  NB won't show up in subset
  // diffs, but ks apply will do the right thing.
  removeCPULimitsMixin:: {
    resources+: {
      // Can't use super.memory in limits, as we want to
      // override the whole limits struct.
      local memoryLimit = super.limits.memory,

      limits: {
        memory: memoryLimit,
      },
    },
  },

  query_frontend_enable_cardinality_estimation:: $._config.cache_frontend_enabled,

  query_frontend_caching_config:: (
    if $._config.cache_frontend_enabled then {
      // So that exporters like cloudwatch can still send in data and be un-cached.
      'query-frontend.max-cache-freshness': '10m',

      'query-frontend.cache-results': true,
      'query-frontend.results-cache.backend': 'memcached',
      'query-frontend.results-cache.memcached.addresses': 'dnssrvnoa+memcached-frontend.%(namespace)s.svc.%(cluster_domain)s:11211' % $._config,
      'query-frontend.results-cache.memcached.max-item-size': $._config.cache_frontend_max_item_size_mb * 1024 * 1024,
      'query-frontend.results-cache.memcached.timeout': '500ms',
    } + if $._config.memcached_frontend_mtls_enabled then {
      'query-frontend.results-cache.memcached.addresses': 'dnssrvnoa+memcached-frontend.%(namespace)s.svc.%(cluster_domain)s:11212' % $._config,
      'query-frontend.results-cache.memcached.connect-timeout': '1s',
      'query-frontend.results-cache.memcached.tls-enabled': true,
      'query-frontend.results-cache.memcached.tls-ca-path': $._config.memcached_ca_cert_path + $._config.memcached_mtls_ca_cert_secret + '.pem',
      'query-frontend.results-cache.memcached.tls-key-path': $._config.memcached_client_key_path + $._config.memcached_mtls_client_key_secret + '.pem',
      'query-frontend.results-cache.memcached.tls-cert-path': $._config.memcached_client_cert_path + $._config.memcached_mtls_client_cert_secret + '.pem',
      'query-frontend.results-cache.memcached.tls-server-name': if $._config.memcached_mtls_server_name != null then $._config.memcached_mtls_server_name else null,
    } else {}
    else {}
  ),

  blocks_chunks_concurrency_connection_config::
    (
      // We should keep a number of idle connections equal to the max "get" concurrency,
      // in order to avoid re-opening connections continuously (this would be slower
      // and fill up the conntrack table too).
      //
      // The downside of this approach is that we'll end up with an higher number of
      // active connections to memcached, so we have to make sure connections limit
      // set in memcached is high enough.
      (
        if $._config.cache_index_queries_enabled then {
          'blocks-storage.bucket-store.index-cache.memcached.max-get-multi-concurrency': 100,
          'blocks-storage.bucket-store.index-cache.memcached.max-idle-connections':
            $.store_gateway_args['blocks-storage.bucket-store.index-cache.memcached.max-get-multi-concurrency'] +
            $.store_gateway_args['blocks-storage.bucket-store.index-cache.memcached.max-async-concurrency'],
        } else {}
      ) + (
        if $._config.cache_chunks_enabled then {
          'blocks-storage.bucket-store.chunks-cache.memcached.max-get-multi-concurrency': 100,
          'blocks-storage.bucket-store.chunks-cache.memcached.max-idle-connections':
            $.store_gateway_args['blocks-storage.bucket-store.chunks-cache.memcached.max-get-multi-concurrency'] +
            $.store_gateway_args['blocks-storage.bucket-store.chunks-cache.memcached.max-async-concurrency'],
        } else {}
      ) + (
        if $._config.cache_metadata_enabled then {
          'blocks-storage.bucket-store.metadata-cache.memcached.max-get-multi-concurrency': 100,
          'blocks-storage.bucket-store.metadata-cache.memcached.max-idle-connections':
            $.store_gateway_args['blocks-storage.bucket-store.metadata-cache.memcached.max-get-multi-concurrency'] +
            $.store_gateway_args['blocks-storage.bucket-store.metadata-cache.memcached.max-async-concurrency'],
        } else {}
      )
    ),

  blocks_chunks_caching_config::
    (
      if $._config.cache_index_queries_enabled then {
        'blocks-storage.bucket-store.index-cache.backend': 'memcached',
        'blocks-storage.bucket-store.index-cache.memcached.addresses': 'dnssrvnoa+memcached-index-queries.%(namespace)s.svc.%(cluster_domain)s:11211' % $._config,
        'blocks-storage.bucket-store.index-cache.memcached.max-item-size': $._config.cache_index_queries_max_item_size_mb * 1024 * 1024,
        'blocks-storage.bucket-store.index-cache.memcached.max-async-concurrency': 50,
        'blocks-storage.bucket-store.index-cache.memcached.timeout': '750ms',
      } + if $._config.memcached_index_queries_mtls_enabled then {
        'blocks-storage.bucket-store.index-cache.memcached.addresses': 'dnssrvnoa+memcached-index-queries.%(namespace)s.svc.%(cluster_domain)s:11212' % $._config,
        'blocks-storage.bucket-store.index-cache.memcached.connect-timeout': '1s',
        'blocks-storage.bucket-store.index-cache.memcached.tls-enabled': true,
        'blocks-storage.bucket-store.index-cache.memcached.tls-ca-path': $._config.memcached_ca_cert_path + $._config.memcached_mtls_ca_cert_secret + '.pem',
        'blocks-storage.bucket-store.index-cache.memcached.tls-key-path': $._config.memcached_client_key_path + $._config.memcached_mtls_client_key_secret + '.pem',
        'blocks-storage.bucket-store.index-cache.memcached.tls-cert-path': $._config.memcached_client_cert_path + $._config.memcached_mtls_client_cert_secret + '.pem',
        'blocks-storage.bucket-store.index-cache.memcached.tls-server-name': if $._config.memcached_mtls_server_name != null then $._config.memcached_mtls_server_name else null,
      } else {}
      else {}
    ) + (
      if $._config.cache_chunks_enabled then {
        'blocks-storage.bucket-store.chunks-cache.backend': 'memcached',
        'blocks-storage.bucket-store.chunks-cache.memcached.addresses': 'dnssrvnoa+memcached.%(namespace)s.svc.%(cluster_domain)s:11211' % $._config,
        'blocks-storage.bucket-store.chunks-cache.memcached.max-item-size': $._config.cache_chunks_max_item_size_mb * 1024 * 1024,
        'blocks-storage.bucket-store.chunks-cache.memcached.max-async-concurrency': 50,
        'blocks-storage.bucket-store.chunks-cache.memcached.timeout': '750ms',
      } + if $._config.memcached_chunks_mtls_enabled then {
        'blocks-storage.bucket-store.chunks-cache.memcached.addresses': 'dnssrvnoa+memcached.%(namespace)s.svc.%(cluster_domain)s:11212' % $._config,
        'blocks-storage.bucket-store.chunks-cache.memcached.connect-timeout': '1s',
        'blocks-storage.bucket-store.chunks-cache.memcached.tls-enabled': true,
        'blocks-storage.bucket-store.chunks-cache.memcached.tls-ca-path': $._config.memcached_ca_cert_path + $._config.memcached_mtls_ca_cert_secret + '.pem',
        'blocks-storage.bucket-store.chunks-cache.memcached.tls-key-path': $._config.memcached_client_key_path + $._config.memcached_mtls_client_key_secret + '.pem',
        'blocks-storage.bucket-store.chunks-cache.memcached.tls-cert-path': $._config.memcached_client_cert_path + $._config.memcached_mtls_client_cert_secret + '.pem',
        'blocks-storage.bucket-store.chunks-cache.memcached.tls-server-name': if $._config.memcached_mtls_server_name != null then $._config.memcached_mtls_server_name else null,
      } else {}
      else {}
    ),

  blocks_metadata_caching_config::
    (
      if $._config.cache_metadata_enabled then {
        'blocks-storage.bucket-store.metadata-cache.backend': 'memcached',
        'blocks-storage.bucket-store.metadata-cache.memcached.addresses': 'dnssrvnoa+memcached-metadata.%(namespace)s.svc.%(cluster_domain)s:11211' % $._config,
        'blocks-storage.bucket-store.metadata-cache.memcached.max-item-size': $._config.cache_metadata_max_item_size_mb * 1024 * 1024,
        'blocks-storage.bucket-store.metadata-cache.memcached.max-async-concurrency': 50,
      } + if $._config.memcached_metadata_mtls_enabled then {
        'blocks-storage.bucket-store.metadata-cache.memcached.addresses': 'dnssrvnoa+memcached-metadata.%(namespace)s.svc.%(cluster_domain)s:11212' % $._config,
        'blocks-storage.bucket-store.metadata-cache.memcached.connect-timeout': '1s',
        'blocks-storage.bucket-store.metadata-cache.memcached.tls-enabled': true,
        'blocks-storage.bucket-store.metadata-cache.memcached.tls-ca-path': $._config.memcached_ca_cert_path + $._config.memcached_mtls_ca_cert_secret + '.pem',
        'blocks-storage.bucket-store.metadata-cache.memcached.tls-key-path': $._config.memcached_client_key_path + $._config.memcached_mtls_client_key_secret + '.pem',
        'blocks-storage.bucket-store.metadata-cache.memcached.tls-cert-path': $._config.memcached_client_cert_path + $._config.memcached_mtls_client_cert_secret + '.pem',
        'blocks-storage.bucket-store.metadata-cache.memcached.tls-server-name': if $._config.memcached_mtls_server_name != null then $._config.memcached_mtls_server_name else null,
      } else {}
      else {}
    ),

  ruler_storage_caching_config::
    (
      if $._config.cache_metadata_enabled then {
        'ruler-storage.cache.backend': 'memcached',
        'ruler-storage.cache.memcached.addresses': 'dnssrvnoa+memcached-metadata.%(namespace)s.svc.%(cluster_domain)s:11211' % $._config,
        'ruler-storage.cache.memcached.max-item-size': $._config.cache_metadata_max_item_size_mb * 1024 * 1024,
        'ruler-storage.cache.memcached.max-async-concurrency': 50,
      } + if $._config.memcached_metadata_mtls_enabled then {
        'ruler-storage.cache.memcached.addresses': 'dnssrvnoa+memcached-metadata.%(namespace)s.svc.%(cluster_domain)s:11212' % $._config,
        'ruler-storage.cache.memcached.connect-timeout': '1s',
        'ruler-storage.cache.memcached.tls-enabled': true,
        'ruler-storage.cache.memcached.tls-ca-path': $._config.memcached_ca_cert_path + $._config.memcached_mtls_ca_cert_secret + '.pem',
        'ruler-storage.cache.memcached.tls-key-path': $._config.memcached_client_key_path + $._config.memcached_mtls_client_key_secret + '.pem',
        'ruler-storage.cache.memcached.tls-cert-path': $._config.memcached_client_cert_path + $._config.memcached_mtls_client_cert_secret + '.pem',
        'ruler-storage.cache.memcached.tls-server-name': if $._config.memcached_mtls_server_name != null then $._config.memcached_mtls_server_name else null,
      } else {}
      else {}
    ),

  bucket_index_config:: if $._config.bucket_index_enabled then {
    // Bucket index is updated by compactor on each cleanup cycle.
    'blocks-storage.bucket-store.sync-interval': $._config.compactor_cleanup_interval,
  } else {
    'blocks-storage.bucket-store.bucket-index.enabled': false,
  },
}
