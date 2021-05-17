{
  _config+: {
    namespace: error 'must define namespace',
    cluster: error 'must define cluster',
    replication_factor: 3,
    external_url: error 'must define external url for cluster',

    storage_backend: error 'must specify storage backend (cassandra, gcp, aws)',
    table_prefix: $._config.namespace,
    cassandra_addresses: error 'must specify cassandra addresses',
    bigtable_instance: error 'must specify bigtable instance',
    bigtable_project: error 'must specify bigtable project',
    aws_region: error 'must specify AWS region',
    s3_bucket_name: error 'must specify S3 bucket name',

    // If false, ingesters are not unregistered on shutdown and left in the ring with
    // the LEAVING state. Setting to false prevents series resharding during ingesters rollouts,
    // but requires to:
    // 1. Either manually forget ingesters on scale down or invoke the /shutdown endpoint
    // 2. Ensure ingester ID is preserved during rollouts
    unregister_ingesters_on_shutdown: true,

    // schema is used to generate the storage schema yaml file used by
    // the Cortex chunks storage:
    // - More information: https://github.com/cortexproject/cortex/pull/1072
    // - Blocks storage doesn't support / uses the schema config.
    schema: if $._config.storage_engine != 'blocks' then
      error 'must specify a schema config'
    else
      [],

    max_chunk_idle: '15m',

    test_exporter_enabled: false,
    test_exporter_start_time: error 'must specify test exporter start time',
    test_exporter_user_id: error 'must specify test exporter used id',

    // The expectation is that if sharding is enabled, we can force more (smaller)
    // queries on the queriers. However this can't be extended too far because most queries
    // concern recent (ingester) data, which isn't sharded. Therefore, we must strike a balance
    // which allows us to process more sharded queries in parallel when requested, but not overload
    // queriers during normal queries.
    querier: {
      replicas: if $._config.queryFrontend.sharded_queries_enabled then 12 else 6,
      concurrency: if $._config.queryFrontend.sharded_queries_enabled then 16 else 8,
    },

    queryFrontend: {
      replicas: 2,
      shard_factor: 16,  // v10 schema shard factor
      sharded_queries_enabled: false,
      // Queries can technically be sharded an arbitrary number of times. Thus query_split_factor is used
      // as a coefficient to multiply the frontend tenant queues by. The idea is that this
      // yields a bit of headroom so tenant queues aren't underprovisioned. Therefore the split factor
      // should be represent the highest reasonable split factor for a query. If too low, a long query
      // (i.e. 30d) with a high split factor (i.e. 5) would result in
      // (day_splits * shard_factor * split_factor) or 30 * 16 * 5 = 2400 sharded queries, which may be
      // more than the max queue size and thus would always error.
      query_split_factor:: 3,
    },

    jaeger_agent_host: null,

    // Use the Cortex chunks storage engine by default, while giving the ability
    // to switch to blocks storage.
    storage_engine: 'chunks',  // Available options are 'chunks' or 'blocks'
    blocks_storage_backend: 'gcs',  // Available options are 'gcs', 's3', 'azure'
    blocks_storage_bucket_name: error 'must specify blocks storage bucket name',
    blocks_storage_s3_endpoint: 's3.dualstack.us-east-1.amazonaws.com',
    blocks_storage_azure_account_name: if $._config.blocks_storage_backend == 'azure' then error 'must specify azure account name' else '',
    blocks_storage_azure_account_key: if $._config.blocks_storage_backend == 'azure' then error 'must specify azure account key' else '',

    // Secondary storage engine is only used for querying.
    querier_second_storage_engine: null,

    store_gateway_replication_factor: 3,

    // By default ingesters will be run as StatefulSet with WAL.
    // If this is set to true, ingesters will use staless deployments without WAL.
    ingester_deployment_without_wal: false,

    ingester: {
      // These config options are only for the chunks storage.
      wal_dir: '/wal_data',
      statefulset_disk: '150Gi',
    },

    // Blocks storage engine doesn't require the table manager.
    // When running blocks with chunks as secondary storage engine for querier only, we need table-manager to apply
    // retention policies.
    table_manager_enabled: $._config.storage_engine == 'chunks' || $._config.querier_second_storage_engine == 'chunks',

    // Blocks storage engine doesn't support index-writes (for writes deduplication) cache.
    memcached_index_writes_enabled: $._config.storage_engine != 'blocks',
    memcached_index_writes_max_item_size_mb: 1,

    // Index and chunks caches are supported by both blocks storage engine and chunks engine.
    memcached_index_queries_enabled: true,
    memcached_index_queries_max_item_size_mb: 5,

    memcached_chunks_enabled: true,
    memcached_chunks_max_item_size_mb: 1,

    memcached_metadata_enabled: $._config.storage_engine == 'blocks',
    memcached_metadata_max_item_size_mb: 1,

    // The query-tee is an optional service which can be used to send
    // the same input query to multiple backends and make them compete
    // (comparing performances).
    query_tee_enabled: false,
    query_tee_backend_endpoints: [],
    query_tee_backend_preferred: '',

    enabledBackends: [
      backend
      for backend in std.split($._config.storage_backend, ',')
    ],

    client_configs: {
      aws:
        if std.count($._config.enabledBackends, 'aws') > 0 then {
          'dynamodb.api-limit': 10,
          'dynamodb.url': 'https://%s' % $._config.aws_region,
          's3.url': 'https://%s/%s' % [$._config.aws_region, $._config.s3_bucket_name],
        } else {},
      cassandra:
        if std.count($._config.enabledBackends, 'cassandra') > 0 then {
          'cassandra.keyspace': $._config.namespace,
          'cassandra.addresses': $._config.cassandra_addresses,
          'cassandra.replication-factor': $._config.replication_factor,
        } else {},
      gcp:
        if std.count($._config.enabledBackends, 'gcp') > 0 then {
          'bigtable.project': $._config.bigtable_project,
          'bigtable.instance': $._config.bigtable_instance,
        } else {},
    },

    storeConfig: self.storeMemcachedChunksConfig,

    storeMemcachedChunksConfig: if $._config.memcached_chunks_enabled && ($._config.storage_engine == 'chunks' || $._config.querier_second_storage_engine == 'chunks') then
      {
        'store.chunks-cache.memcached.hostname': 'memcached.%s.svc.cluster.local' % $._config.namespace,
        'store.chunks-cache.memcached.service': 'memcached-client',
        'store.chunks-cache.memcached.timeout': '3s',
      }
    else {},

    grpcConfig:: {
      'server.grpc.keepalive.min-time-between-pings': '10s',
      'server.grpc.keepalive.ping-without-stream-allowed': true,
    },

    storageConfig:
      $._config.client_configs.aws +
      $._config.client_configs.cassandra +
      $._config.client_configs.gcp +
      { 'schema-config-file': '/etc/cortex/schema/config.yaml' },

    genericBlocksStorageConfig:: {
      'store.engine': $._config.storage_engine,  // May still be chunks
    },
    queryBlocksStorageConfig:: {
      'blocks-storage.bucket-store.sync-dir': '/data/tsdb',
      'blocks-storage.bucket-store.ignore-deletion-marks-delay': '1h',

      'store-gateway.sharding-enabled': true,
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
      if $._config.storage_engine == 'blocks' || $._config.querier_second_storage_engine == 'blocks' then (
        if $._config.blocks_storage_backend == 'gcs' then $._config.gcsBlocksStorageConfig
        else if $._config.blocks_storage_backend == 's3' then $._config.s3BlocksStorageConfig
        else if $._config.blocks_storage_backend == 'azure' then $._config.azureBlocksStorageConfig
        else $._config.genericBlocksStorageConfig
      ) else {}
    ),

    // Shared between the Ruler and Querier
    queryConfig: {
      'runtime-config.file': '/etc/cortex/overrides.yaml',

      // Limit the size of the rows we read from the index.
      'store.cardinality-limit': 1e6,

      // Don't allow individual queries of longer than 32days.  Due to day query
      // splitting in the frontend, the reality is this only limits rate(foo[32d])
      // type queries. 32 days to allow for comparision over the last month (31d) and
      // then some.
      'store.max-query-length': '768h',
    } + (
      if $._config.storage_engine == 'chunks' then {
        // Don't query ingesters for older queries.
        // Chunks are held in memory for up to 6hrs right now. Additional 6h are granted for safety reasons because
        // the remote writing Prometheus may have a delay or write requests into the database are queued.
        'querier.query-ingesters-within': '12h',

        // Don't query the chunk store for data younger than max_chunk_idle.
        'querier.query-store-after': $._config.max_chunk_idle,
      } else if $._config.storage_engine == 'blocks' then {
        // Ingesters don't have data older than 13h, no need to ask them.
        'querier.query-ingesters-within': '13h',

        // No need to look at store for data younger than 12h, as ingesters have all of it.
        'querier.query-store-after': '12h',
      }
    ) + (
      if $._config.memcached_index_queries_enabled && ($._config.storage_engine == 'chunks' || $._config.querier_second_storage_engine == 'chunks') then
        {
          // Setting for index cache.
          'store.index-cache-validity': '14m',  // ingester.retain-period=15m, 1m less for safety.
          'store.index-cache-read.cache.enable-fifocache': true,
          'store.index-cache-read.fifocache.max-size-items': 102400,
          'store.index-cache-read.memcached.hostname': 'memcached-index-queries.%(namespace)s.svc.cluster.local' % $._config,
          'store.index-cache-read.memcached.service': 'memcached-client',
          'store.index-cache-read.memcached.timeout': '500ms',
          'store.cache-lookups-older-than': '36h',
        }
      else {}
    ),

    ringConfig: {
      'consul.hostname': 'consul.%s.svc.cluster.local:8500' % $._config.namespace,
      'ring.prefix': '',
    },

    // Some distributor config is shared with the querier.
    distributorConfig: {
      'distributor.replication-factor': $._config.replication_factor,
      'distributor.shard-by-all-labels': true,
      'distributor.health-check-ingesters': true,
      'ring.heartbeat-timeout': '10m',
    },

    ruler_enabled: false,
    ruler_client_type: error 'you must specify a storage backend type for the ruler (azure, gcs, s3, local)',
    ruler_s3_bucket_name: error 'you must specify the ruler S3 bucket name',
    ruler_gcs_bucket_name: error 'must specify a GCS bucket name',

    rulerClientConfig:
      {
        'ruler-storage.backend': $._config.ruler_client_type,
      } +
      {
        gcs: {
          'ruler-storage.gcs.bucket-name': $._config.ruler_gcs_bucket_name,
        },
        s3: {
          'ruler-storage.s3.region': $._config.aws_region,
          'ruler-storage.s3.bucket-name': $._config.ruler_s3_bucket_name,
        },
        'local': {
          'ruler-storage.local.directory': $._config.ruler_local_directory,
        },
      }[$._config.ruler_client_type],

    alertmanager: {
      replicas: 3,
      gossip_port: 9094,
      fallback_config: {},
    },

    alertmanager_client_type: error 'you must specify a storage backend type for the alertmanager (azure, gcs, s3, local)',
    alertmanager_s3_bucket_name: error 'you must specify the alertmanager S3 bucket name',
    alertmanager_gcs_bucket_name: error 'must specify a GCS bucket name',

    alertmanagerStorageClientConfig:
      {
        'alertmanager-storage.backend': $._config.alertmanager_client_type,
      } +
      {
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

    overrides_configmap: 'overrides',

    overrides: {
      extra_small_user:: {
        max_series_per_user: 0,  // Disabled in favour of the max global limit
        max_series_per_metric: 0,  // Disabled in favour of the max global limit

        // Our limit should be 100k, but we need some room of about ~50% to take rollouts into account
        max_global_series_per_user: 150000,
        max_global_series_per_metric: 20000,

        max_series_per_query: 100000,
        max_samples_per_query: 1000000,

        ingestion_rate: 10000,
        ingestion_burst_size: 200000,

        // 300 rules
        ruler_max_rules_per_rule_group: 15,
        ruler_max_rule_groups_per_tenant: 20,
      },

      small_user:: {
        max_series_per_metric: 0,  // Disabled in favour of the max global limit
        max_series_per_user: 0,  // Disabled in favour of the max global limit

        max_global_series_per_user: 1000000,
        max_global_series_per_metric: 100000,

        max_series_per_query: 100000,
        max_samples_per_query: 1000000,

        ingestion_rate: 100000,
        ingestion_burst_size: 1000000,

        // 450 rules
        ruler_max_rules_per_rule_group: 15,
        ruler_max_rule_groups_per_tenant: 30,
      },

      medium_user:: {
        max_series_per_metric: 0,  // Disabled in favour of the max global limit
        max_series_per_user: 0,  // Disabled in favour of the max global limit

        max_global_series_per_user: 3000000,  // 3M
        max_global_series_per_metric: 300000,  // 300K

        max_series_per_query: 100000,
        max_samples_per_query: 1000000,

        ingestion_rate: 350000,  // 350K
        ingestion_burst_size: 3500000,  // 3.5M

        // 600 rules
        ruler_max_rules_per_rule_group: 15,
        ruler_max_rule_groups_per_tenant: 40,
      },

      big_user:: {
        max_series_per_metric: 0,  // Disabled in favour of the max global limit
        max_series_per_user: 0,  // Disabled in favour of the max global limit

        max_series_per_query: 100000,
        max_samples_per_query: 1000000,

        max_global_series_per_user: 6000000,  // 6M
        max_global_series_per_metric: 600000,  // 600K

        ingestion_rate: 700000,  // 700K
        ingestion_burst_size: 7000000,  // 7M

        // 750 rules
        ruler_max_rules_per_rule_group: 15,
        ruler_max_rule_groups_per_tenant: 50,
      },

      super_user:: {
        max_series_per_metric: 0,  // Disabled in favour of the max global limit
        max_series_per_user: 0,  // Disabled in favour of the max global limit

        max_global_series_per_user: 12000000,  // 12M
        max_global_series_per_metric: 1200000,  // 1.2M

        max_series_per_query: 100000,
        max_samples_per_query: 1000000,

        ingestion_rate: 1500000,  // 1.5M
        ingestion_burst_size: 15000000,  // 15M

        // 900 rules
        ruler_max_rules_per_rule_group: 15,
        ruler_max_rule_groups_per_tenant: 60,
      },

      // This user class has limits increased by +50% compared to the previous one.
      mega_user+:: {
        max_series_per_metric: 0,  // Disabled in favour of the max global limit
        max_series_per_user: 0,  // Disabled in favour of the max global limit

        max_global_series_per_user: 16000000,  // 16M
        max_global_series_per_metric: 1600000,  // 1.6M

        max_series_per_query: 100000,
        max_samples_per_query: 1000000,

        ingestion_rate: 2250000,  // 2.25M
        ingestion_burst_size: 22500000,  // 22.5M

        // 1050 rules
        ruler_max_rules_per_rule_group: 15,
        ruler_max_rule_groups_per_tenant: 70,
      },
    },

    // if not empty, passed to overrides.yaml as another top-level field
    multi_kv_config: {},

    schemaID: std.md5(std.toString($._config.schema)),

    enable_pod_priorities: true,

    alertmanager_enabled: false,

    // Enables query-scheduler component, and reconfigures querier and query-frontend to use it.
    query_scheduler_enabled: false,

    // Enables streaming of chunks from ingesters using blocks.
    ingester_stream_chunks_when_using_blocks: true,

    // Ingester limits are put directly into runtime config, if not null. Available limits:
    //    ingester_instance_limits: {
    //      max_inflight_push_requests: 0,  // Max inflight push requests per ingester. 0 = no limit.
    //      max_ingestion_rate: 0,  // Max ingestion rate (samples/second) per ingester. 0 = no limit.
    //      max_series: 0,  // Max number of series per ingester. 0 = no limit.
    //      max_tenants: 0,  // Max number of tenants per ingester. 0 = no limit.
    //    },
    ingester_instance_limits: null,
  },

  local configMap = $.core.v1.configMap,

  overrides_config:
    configMap.new($._config.overrides_configmap) +
    configMap.withData({
      'overrides.yaml': $.util.manifestYaml(
        { overrides: $._config.overrides }
        + (if std.length($._config.multi_kv_config) > 0 then { multi_kv_config: $._config.multi_kv_config } else {})
        + (if $._config.ingester_stream_chunks_when_using_blocks then { ingester_stream_chunks_when_using_blocks: true } else {})
        + (if $._config.ingester_instance_limits != null then { ingester_limits: $._config.ingester_instance_limits } else {}),
      ),
    }),

  storage_config:
    configMap.new('schema-' + $._config.schemaID) +
    configMap.withData({
      'config.yaml': $.util.manifestYaml({
        configs: $._config.schema,
      }),
    }),

  local deployment = $.apps.v1.deployment,
  storage_config_mixin::
    deployment.mixin.spec.template.metadata.withAnnotationsMixin({ schemaID: $._config.schemaID },) +
    $.util.configVolumeMount('schema-' + $._config.schemaID, '/etc/cortex/schema'),

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
}
