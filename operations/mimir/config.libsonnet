{
  _config+: {
    namespace: error 'must define namespace',
    cluster: error 'must define cluster',
    replication_factor: 3,
    external_url: error 'must define external url for cluster',

    storage_backend: error 'must specify storage backend (cassandra, gcp)',
    table_prefix: $._config.namespace,
    cassandra_addresses: error 'must specify cassandra addresses',
    bigtable_instance: error 'must specify bigtable instance',
    bigtable_project: error 'must specify bigtable project',
    aws_region: error 'must specify AWS region',
    s3_bucket_name: error 'must specify S3 bucket name',

    // schema is used to generate the storage schema yaml file used by
    // the Cortex chunks storage:
    // - More information: https://github.com/cortexproject/cortex/pull/1072
    // - TSDB integration doesn't rely on the Cortex chunks store, so doesn't
    //   support the schema config.
    schema: if $._config.storage_engine != 'tsdb' then
      error 'must specify a schema config'
    else
      [],

    max_chunk_idle: '15m',

    test_exporter_enabled: true,
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
      ingester_streaming_enabled: true,
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
    // to switch to tsdb storage.
    storage_engine: 'chunks',
    storage_tsdb_bucket_name: error 'must specify GCS bucket name to store TSDB blocks',

    distributor_short_grpc_keepalive_enabled: false,

    // TSDB storage engine doesn't require the table manager.
    table_manager_enabled: $._config.storage_engine != 'tsdb',

    // TSDB storage engine doesn't require memcached for chunks or chunk indexes.
    memcached_index_queries_enabled: $._config.storage_engine != 'tsdb',
    memcached_index_writes_enabled: $._config.storage_engine != 'tsdb',
    memcached_chunks_enabled: $._config.storage_engine != 'tsdb',

    ingestion_rate_global_limit_enabled: false,

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

    storeMemcachedChunksConfig: if $._config.memcached_chunks_enabled then
      {
        'memcached.hostname': 'memcached.%s.svc.cluster.local' % $._config.namespace,
        'memcached.service': 'memcached-client',
        'memcached.timeout': '3s',
        'memcached.batchsize': 1024,
        'memcached.consistent-hash': true,
      }
    else {},

    storageConfig:
      $._config.client_configs.aws +
      $._config.client_configs.cassandra +
      $._config.client_configs.gcp +
      $._config.storageTSDBConfig +
      { 'config-yaml': '/etc/cortex/schema/config.yaml' },

    // TSDB blocks storage configuration, used only when 'tsdb' storage
    // engine is explicitly enabled.
    storageTSDBConfig: if $._config.storage_engine == 'tsdb' then {
      'store.engine': 'tsdb',
      'experimental.tsdb.dir': '/data/tsdb',
      'experimental.tsdb.bucket-store.sync-dir': '/data/tsdb',
      'experimental.tsdb.block-ranges-period': '2h',
      'experimental.tsdb.retention-period': '1h',
      'experimental.tsdb.ship-interval': '1m',
      'experimental.tsdb.backend': 'gcs',
      'experimental.tsdb.gcs.bucket-name': $._config.storage_tsdb_bucket_name,
    } else {},

    // Shared between the Ruler and Querier
    queryConfig: {
      // Use iterators to merge chunks, to reduce memory usage.
      'querier.ingester-streaming': $._config.querier.ingester_streaming_enabled,
      'querier.batch-iterators': true,

      // Don't query ingesters for older queries.
      // Chunks are 6hrs right now.  Add some slack for safety.
      'querier.query-ingesters-within': '12h',

      'limits.per-user-override-config': '/etc/cortex/overrides.yaml',

      // Limit the size of the rows we read from the index.
      'store.cardinality-limit': 1e6,

      // Don't allow individual queries of longer than 31days.  Due to day query
      // splitting in the frontend, the reality is this only limits rate(foo[31d])
      // type queries.
      'store.max-query-length': '744h',

      // Don't query the chunk store for data younger than max_chunk_idle.
      'querier.query-store-after': $._config.max_chunk_idle,
    } + (
      if $._config.memcached_index_queries_enabled then
        {
          // Setting for index cache.
          'store.index-cache-validity': '14m',  // ingester.retain-period=15m, 1m less for safety.
          'store.index-cache-read.cache.enable-fifocache': true,
          'store.index-cache-read.fifocache.size': 102400,
          'store.index-cache-read.memcached.hostname': 'memcached-index-queries.%(namespace)s.svc.cluster.local' % $._config,
          'store.index-cache-read.memcached.service': 'memcached-client',
          'store.index-cache-read.memcached.timeout': '500ms',
          'store.index-cache-read.memcached.consistent-hash': true,
          'store.cache-lookups-older-than': '36h',
        }
      else {}
    ),

    ringConfig: {
      'consul.hostname': 'consul.%s.svc.cluster.local:8500' % $._config.namespace,
      'consul.consistent-reads': false,
      'ring.prefix': '',
      'consul.watch-rate-limit': 1,
      'consul.watch-burst-size': 5,
    },

    // Some distributor config is shared with the querier.
    distributorConfig: {
      'distributor.replication-factor': $._config.replication_factor,
      'distributor.shard-by-all-labels': true,
      'distributor.health-check-ingesters': true,
      'ring.heartbeat-timeout': '10m',
      'consul.consistent-reads': false,
    },

    overrides: {
      // === Per-tenant usage limits. ===
      //
      // These are the defaults. Distributor limits will be 5x (#replicas) higher,
      // ingester limits are 6s (#replicas) / 3x (#replication factor) higher.
      //
      // small_user: {
      //   ingestion_rate: 10,000
      //   ingestion_burst_size: 20,000
      //
      //   max_series_per_user:   0 (disabled)
      //   max_series_per_metric: 0 (disabled)
      //
      //   max_global_series_per_user:   1,000,000
      //   max_global_series_per_metric: 100,000
      //
      //   max_series_per_query: 10,000
      //   max_samples_per_query: 100,000
      // },

      medium_user:: {
        ingestion_rate: 25000,
        ingestion_burst_size: 50000,

        max_series_per_metric: 0,  // Disabled in favour of the max global limit
        max_series_per_user: 0,  // Disabled in favour of the max global limit

        max_global_series_per_user: 3000000,  // 3M
        max_global_series_per_metric: 300000,  // 300K

        max_series_per_query: 100000,
        max_samples_per_query: 1000000,
      } + if !$._config.ingestion_rate_global_limit_enabled then {} else {
        ingestion_rate: 350000,  // 350K
        ingestion_burst_size: 3500000,  // 3.5M
      },

      big_user:: {
        ingestion_rate: 50000,
        ingestion_burst_size: 70000,

        max_series_per_metric: 0,  // Disabled in favour of the max global limit
        max_series_per_user: 0,  // Disabled in favour of the max global limit

        max_series_per_query: 100000,
        max_samples_per_query: 1000000,

        max_global_series_per_user: 6000000,  // 6M
        max_global_series_per_metric: 600000,  // 600K
      } + if !$._config.ingestion_rate_global_limit_enabled then {} else {
        ingestion_rate: 700000,  // 700K
        ingestion_burst_size: 7000000,  // 7M
      },

      super_user:: {
        ingestion_rate: 200000,
        ingestion_burst_size: 240000,

        max_series_per_metric: 0,  // Disabled in favour of the max global limit
        max_series_per_user: 0,  // Disabled in favour of the max global limit

        max_global_series_per_user: 12000000,  // 12M
        max_global_series_per_metric: 1200000,  // 1.2M

        max_series_per_query: 100000,
        max_samples_per_query: 1000000,
      } + if !$._config.ingestion_rate_global_limit_enabled then {} else {
        ingestion_rate: 1500000,  // 1.5M
        ingestion_burst_size: 15000000,  // 15M
      },
    },

    // if not empty, passed to overrides.yaml as another top-level field
    multi_kv_config: {},

    schemaID: std.md5(std.toString($._config.schema)),

    enable_pod_priorities: true,
  },

  local configMap = $.core.v1.configMap,

  overrides_config:
    configMap.new('overrides') +
    configMap.withData({
      'overrides.yaml': $.util.manifestYaml(
        {
          overrides: $._config.overrides,
        } + if std.length($._config.multi_kv_config) > 0 then {
          multi_kv_config: $._config.multi_kv_config,
        } else {}
      ),
    }),

  storage_config:
    configMap.new('schema-' + $._config.schemaID) +
    configMap.withData({
      'config.yaml': $.util.manifestYaml({
        configs: $._config.schema,
      }),
    }),

  local deployment = $.apps.v1beta1.deployment,
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
