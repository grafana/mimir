{
  _config+: {
    namespace: error 'must define namespace',
    cluster: error 'must define cluster',
    replication_factor: 3,
    external_url: error 'must define external url for cluster',

    node_selector: null,

    aws_region: error 'must specify AWS region',

    // If false, ingesters are not unregistered on shutdown and left in the ring with
    // the LEAVING state. Setting to false prevents series resharding during ingesters rollouts,
    // but requires to:
    // 1. Either manually forget ingesters on scale down or invoke the /ingester/shutdown endpoint
    // 2. Ensure ingester ID is preserved during rollouts
    unregister_ingesters_on_shutdown: true,

    // Controls whether multiple pods for the same service can be scheduled on the same node.
    // Distributing the pods over different nodes improves performance and also realiability,
    // especially important in case of ingester where losing multiple ingesters can cause data loss.
    distributor_allow_multiple_replicas_on_same_node: false,
    ingester_allow_multiple_replicas_on_same_node: false,
    ruler_allow_multiple_replicas_on_same_node: false,
    querier_allow_multiple_replicas_on_same_node: false,
    query_frontend_allow_multiple_replicas_on_same_node: false,
    store_gateway_allow_multiple_replicas_on_same_node: false,

    test_exporter_enabled: false,
    test_exporter_start_time: error 'must specify test exporter start time',
    test_exporter_user_id: error 'must specify test exporter used id',

    querier: {
      replicas: 6,
      concurrency: 8,
    },

    queryFrontend: {
      replicas: 2,
    },

    jaeger_agent_host: null,

    blocks_storage_backend: 'gcs',  // Available options are 'gcs', 's3', 'azure'
    blocks_storage_bucket_name: error 'must specify blocks storage bucket name',
    blocks_storage_s3_endpoint: 's3.dualstack.us-east-1.amazonaws.com',
    blocks_storage_azure_account_name: if $._config.blocks_storage_backend == 'azure' then error 'must specify azure account name' else '',
    blocks_storage_azure_account_key: if $._config.blocks_storage_backend == 'azure' then error 'must specify azure account key' else '',

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

    // Enable use of bucket index by querier, ruler and store-gateway.
    bucket_index_enabled: true,

    store_gateway_replication_factor: 3,

    memcached_index_queries_enabled: true,
    memcached_index_queries_max_item_size_mb: 5,

    memcached_chunks_enabled: true,
    memcached_chunks_max_item_size_mb: 1,

    memcached_metadata_enabled: true,
    memcached_metadata_max_item_size_mb: 1,

    // The query-tee is an optional service which can be used to send
    // the same input query to multiple backends and make them compete
    // (comparing performances).
    query_tee_enabled: false,
    query_tee_backend_endpoints: [],
    query_tee_backend_preferred: '',

    grpcConfig:: {
      'server.grpc.keepalive.min-time-between-pings': '10s',
      'server.grpc.keepalive.ping-without-stream-allowed': true,
    },

    storageConfig:: {},
    genericBlocksStorageConfig:: {},
    queryBlocksStorageConfig:: {
      'blocks-storage.bucket-store.sync-dir': '/data/tsdb',

      'store-gateway.sharding-ring.store': 'consul',
      'store-gateway.sharding-ring.consul.hostname': 'consul.%s.svc.cluster.local:8500' % $._config.namespace,
      'store-gateway.sharding-ring.prefix': '',
      'store-gateway.sharding-ring.replication-factor': $._config.store_gateway_replication_factor,
    },
    gcsBlocksStorageConfig:: $._config.genericBlocksStorageConfig {
      'blocks-storage.backend': 'gcs',
      'blocks-storage.gcs.bucket-name': $._config.blocks_storage_bucket_name,
    },
    s3BlocksStorageConfig:: $._config.genericBlocksStorageConfig {
      'blocks-storage.backend': 's3',
      'blocks-storage.s3.bucket-name': $._config.blocks_storage_bucket_name,
      'blocks-storage.s3.endpoint': $._config.blocks_storage_s3_endpoint,
    },
    azureBlocksStorageConfig:: $._config.genericBlocksStorageConfig {
      'blocks-storage.backend': 'azure',
      'blocks-storage.azure.container-name': $._config.blocks_storage_bucket_name,
      'blocks-storage.azure.account-name': $._config.blocks_storage_azure_account_name,
      'blocks-storage.azure.account-key': $._config.blocks_storage_azure_account_key,
    },
    // Blocks storage configuration, used only when 'blocks' storage
    // engine is explicitly enabled.
    blocksStorageConfig: (
      if $._config.blocks_storage_backend == 'gcs' then $._config.gcsBlocksStorageConfig
      else if $._config.blocks_storage_backend == 's3' then $._config.s3BlocksStorageConfig
      else if $._config.blocks_storage_backend == 'azure' then $._config.azureBlocksStorageConfig
      else $._config.genericBlocksStorageConfig
    ),

    // Querier component config (shared between the ruler and querier).
    queryConfig: {
      'runtime-config.file': '%s/overrides.yaml' % $._config.overrides_configmap_mountpoint,

      // Don't allow individual queries of longer than 32days.  Due to day query
      // splitting in the frontend, the reality is this only limits rate(foo[32d])
      // type queries. 32 days to allow for comparision over the last month (31d) and
      // then some.
      'store.max-query-length': '768h',

      // Ingesters don't have data older than 13h, no need to ask them.
      'querier.query-ingesters-within': '13h',

      // No need to look at store for data younger than 12h, as ingesters have all of it.
      'querier.query-store-after': '12h',
    },

    // PromQL query engine config (shared between all services running PromQL engine, like the ruler and querier).
    queryEngineConfig: {},

    // The ingester ring client config that should be shared across all Mimir services
    // using or watching the ingester ring.
    ingesterRingClientConfig: {
      'ingester.ring.consul.hostname': 'consul.%s.svc.cluster.local:8500' % $._config.namespace,
      'ingester.ring.replication-factor': $._config.replication_factor,
      'distributor.health-check-ingesters': true,
      'ingester.ring.heartbeat-timeout': '10m',
      'ingester.ring.store': 'consul',
      'ingester.ring.prefix': '',
    },

    ruler_enabled: false,
    ruler_client_type: error 'you must specify a storage backend type for the ruler (azure, gcs, s3, local)',
    ruler_storage_bucket_name: error 'must specify the ruler storage bucket name',
    ruler_storage_azure_account_name: error 'must specify the ruler storage Azure account name',
    ruler_storage_azure_account_key: error 'must specify the ruler storage Azure account key',

    rulerClientConfig:
      {
        'ruler-storage.backend': $._config.ruler_client_type,
      } +
      {
        gcs: {
          'ruler-storage.gcs.bucket-name': $._config.ruler_storage_bucket_name,
        },
        s3: {
          'ruler-storage.s3.region': $._config.aws_region,
          'ruler-storage.s3.bucket-name': $._config.ruler_storage_bucket_name,
          'ruler-storage.s3.endpoint': 's3.dualstack.%s.amazonaws.com' % $._config.aws_region,
        },
        azure: {
          'ruler-storage.azure.container-name': $._config.ruler_storage_bucket_name,
          'ruler-storage.azure.account-name': $._config.ruler_storage_azure_account_name,
          'ruler-storage.azure.account-key': $._config.ruler_storage_azure_account_key,
        },
        'local': {
          'ruler-storage.local.directory': $._config.ruler_local_directory,
        },
      }[$._config.ruler_client_type],

    server_http_port: 8080,

    alertmanager: {
      replicas: 3,
      sharding_enabled: false,
      gossip_port: 9094,
      fallback_config: {},
      ring_store: 'consul',
      ring_hostname: 'consul.%s.svc.cluster.local:8500' % $._config.namespace,
      ring_replication_factor: $._config.replication_factor,
    },

    alertmanager_client_type: error 'you must specify a storage backend type for the alertmanager (azure, gcs, s3, local)',
    alertmanager_s3_bucket_name: error 'you must specify the alertmanager S3 bucket name',
    alertmanager_gcs_bucket_name: error 'you must specify a GCS bucket name',
    alertmanager_azure_container_name: error 'you must specify an Azure container name',

    alertmanagerStorageClientConfig:
      {
        'alertmanager-storage.backend': $._config.alertmanager_client_type,
      } +
      {
        azure: {
          'alertmanager-storage.azure.account-key': $._config.alertmanager_azure_account_key,
          'alertmanager-storage.azure.account-name': $._config.alertmanager_azure_account_name,
          'alertmanager-storage.azure.container-name': $._config.alertmanager_azure_container_name,
        },
        gcs: {
          'alertmanager-storage.gcs.bucket-name': $._config.alertmanager_gcs_bucket_name,
        },
        s3: {
          'alertmanager-storage.s3.region': $._config.aws_region,
          'alertmanager-storage.s3.bucket-name': $._config.alertmanager_s3_bucket_name,
        },
        'local': {
          'alertmanager-storage.local.path': $._config.alertmanager_local_directory,
        },
      }[$._config.alertmanager_client_type],

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
      'ingester.max-global-series-per-metric': $._config.limits.max_global_series_per_metric,
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

    overrides: {
      extra_small_user:: {
        // Our limit should be 100k, but we need some room of about ~50% to take rollouts into account
        max_global_series_per_user: 150000,
        max_global_series_per_metric: 20000,

        ingestion_rate: 10000,
        ingestion_burst_size: 200000,

        // 700 rules
        ruler_max_rules_per_rule_group: 20,
        ruler_max_rule_groups_per_tenant: 35,

        // No retention for now.
        compactor_blocks_retention_period: '0',
      },

      medium_small_user:: {
        max_global_series_per_user: 300000,
        max_global_series_per_metric: 30000,

        ingestion_rate: 30000,
        ingestion_burst_size: 300000,

        // 1000 rules
        ruler_max_rules_per_rule_group: 20,
        ruler_max_rule_groups_per_tenant: 50,
      },

      small_user:: {
        max_global_series_per_user: 1000000,
        max_global_series_per_metric: 100000,

        ingestion_rate: 100000,
        ingestion_burst_size: 1000000,

        // 1400 rules
        ruler_max_rules_per_rule_group: 20,
        ruler_max_rule_groups_per_tenant: 70,
      },

      medium_user:: {
        max_global_series_per_user: 3000000,  // 3M
        max_global_series_per_metric: 300000,  // 300K

        ingestion_rate: 350000,  // 350K
        ingestion_burst_size: 3500000,  // 3.5M

        // 1800 rules
        ruler_max_rules_per_rule_group: 20,
        ruler_max_rule_groups_per_tenant: 90,
      },

      big_user:: {
        max_global_series_per_user: 6000000,  // 6M
        max_global_series_per_metric: 600000,  // 600K

        ingestion_rate: 700000,  // 700K
        ingestion_burst_size: 7000000,  // 7M

        // 2200 rules
        ruler_max_rules_per_rule_group: 20,
        ruler_max_rule_groups_per_tenant: 110,
      },

      super_user:: {
        max_global_series_per_user: 12000000,  // 12M
        max_global_series_per_metric: 1200000,  // 1.2M

        ingestion_rate: 1500000,  // 1.5M
        ingestion_burst_size: 15000000,  // 15M

        // 2600 rules
        ruler_max_rules_per_rule_group: 20,
        ruler_max_rule_groups_per_tenant: 130,

        compactor_split_and_merge_shards: 2,
        compactor_tenant_shard_size: 2,
        compactor_split_groups: 2,
      },

      // This user class has limits increased by +50% compared to the previous one.
      mega_user+:: {
        max_global_series_per_user: 16000000,  // 16M
        max_global_series_per_metric: 1600000,  // 1.6M

        ingestion_rate: 2250000,  // 2.25M
        ingestion_burst_size: 22500000,  // 22.5M

        // 3000 rules
        ruler_max_rules_per_rule_group: 20,
        ruler_max_rule_groups_per_tenant: 150,

        compactor_split_and_merge_shards: 2,
        compactor_tenant_shard_size: 2,
        compactor_split_groups: 2,
      },
    },

    // if not empty, passed to overrides.yaml as another top-level field
    multi_kv_config: {},

    enable_pod_priorities: true,

    alertmanager_enabled: false,

    // Enables query-scheduler component, and reconfigures querier and query-frontend to use it.
    query_scheduler_enabled: false,

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

    gossip_member_label: 'gossip_ring_member',
    // Labels that service selectors should not use
    service_ignored_labels:: [self.gossip_member_label],
  },

  local configMap = $.core.v1.configMap,

  overrides_config:
    configMap.new($._config.overrides_configmap) +
    configMap.withData({
      'overrides.yaml': $.util.manifestYaml(
        { overrides: $._config.overrides }
        + (if std.length($._config.multi_kv_config) > 0 then { multi_kv_config: $._config.multi_kv_config } else {})
        + (if !$._config.ingester_stream_chunks_when_using_blocks then { ingester_stream_chunks_when_using_blocks: false } else {})
        + (if $._config.ingester_instance_limits != null then { ingester_limits: $._config.ingester_instance_limits } else {}),
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

  blocks_chunks_caching_config::
    (
      if $._config.memcached_index_queries_enabled then {
        'blocks-storage.bucket-store.index-cache.backend': 'memcached',
        'blocks-storage.bucket-store.index-cache.memcached.addresses': 'dnssrvnoa+memcached-index-queries.%(namespace)s.svc.cluster.local:11211' % $._config,
        'blocks-storage.bucket-store.index-cache.memcached.max-item-size': $._config.memcached_index_queries_max_item_size_mb * 1024 * 1024,
        'blocks-storage.bucket-store.index-cache.memcached.max-async-concurrency': '50',
      } else {}
    ) + (
      if $._config.memcached_chunks_enabled then {
        'blocks-storage.bucket-store.chunks-cache.backend': 'memcached',
        'blocks-storage.bucket-store.chunks-cache.memcached.addresses': 'dnssrvnoa+memcached.%(namespace)s.svc.cluster.local:11211' % $._config,
        'blocks-storage.bucket-store.chunks-cache.memcached.max-item-size': $._config.memcached_chunks_max_item_size_mb * 1024 * 1024,
        'blocks-storage.bucket-store.chunks-cache.memcached.max-async-concurrency': '50',
      } else {}
    ),

  blocks_metadata_caching_config:: if $._config.memcached_metadata_enabled then {
    'blocks-storage.bucket-store.metadata-cache.backend': 'memcached',
    'blocks-storage.bucket-store.metadata-cache.memcached.addresses': 'dnssrvnoa+memcached-metadata.%(namespace)s.svc.cluster.local:11211' % $._config,
    'blocks-storage.bucket-store.metadata-cache.memcached.max-item-size': $._config.memcached_metadata_max_item_size_mb * 1024 * 1024,
    'blocks-storage.bucket-store.metadata-cache.memcached.max-async-concurrency': '50',
  } else {},

  bucket_index_config:: if $._config.bucket_index_enabled then {
    // Bucket index is updated by compactor on each cleanup cycle.
    'blocks-storage.bucket-store.sync-interval': $._config.compactor_cleanup_interval,
  } else {
    'blocks-storage.bucket-store.bucket-index.enabled': false,
  },
}
