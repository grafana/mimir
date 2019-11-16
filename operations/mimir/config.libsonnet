{
  _config+: {
    namespace: error 'must define namespace',
    cluster: error 'must define cluster',
    replication_factor: 3,

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

    max_series_per_user: 250000,
    max_series_per_metric: 10000,
    max_chunk_idle: '15m',

    test_exporter_enabled: false,
    test_exporter_start_time: error 'must specify test exporter start time',
    test_exporter_user_id: error 'must specify test exporter used id',

    querierConcurrency: 8,
    querier_ingester_streaming_enabled: $._config.storage_engine != 'tsdb',

    jaeger_agent_host: null,

    // Use the Cortex chunks storage engine by default, while giving the ability
    // to switch to tsdb storage.
    storage_engine: 'chunks',
    storage_tsdb_bucket_name: error 'must specify GCS bucket name to store TSDB blocks',

    // TSDB storage engine doesn't require the table manager.
    table_manager_enabled: $._config.storage_engine != 'tsdb',

    // TSDB storage engine doesn't require memcached for chunks or chunk indexes.
    memcached_index_queries_enabled: $._config.storage_engine != 'tsdb',
    memcached_index_writes_enabled: $._config.storage_engine != 'tsdb',
    memcached_chunks_enabled: $._config.storage_engine != 'tsdb',

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
      'experimental.tsdb.dir': '/tmp/tsdb',
      'experimental.tsdb.sync-dir': '/tmp/tsdb',
      'experimental.tsdb.block-ranges-period': '2h',
      'experimental.tsdb.retention-period': '1h',
      'experimental.tsdb.ship-interval': '1m',
      'experimental.tsdb.backend': 'gcs',
      'experimental.tsdb.gcs.bucket-name': $._config.storage_tsdb_bucket_name,
    } else {},

    // Shared between the Ruler and Querier
    queryConfig: {
      // Use iterators to merge chunks, to reduce memory usage.
      'querier.ingester-streaming': $._config.querier_ingester_streaming_enabled,
      'querier.batch-iterators': true,

      // Don't query the chunk store for data younger than max_chunk_idle.
      'store.min-chunk-age': $._config.max_chunk_idle,

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
      // These are the defaults. These are not global limits but per instance limits.
      //
      // small_user: {
      //   ingestion_rate: 10,000
      //   ingestion_burst_size: 20,000
      //
      //   max_series_per_user: 250,000
      //   max_series_per_metric: 10,000
      //
      //   max_series_per_query: 10,000
      //   max_samples_per_query: 100,000
      // },

      medium_user:: {
        ingestion_rate: 25000,
        ingestion_burst_size: 50000,

        max_series_per_metric: 100000,
        max_series_per_user: 500000,

        max_series_per_query: 100000,
        max_samples_per_query: 1000000,
      },

      big_user:: {
        ingestion_rate: 50000,
        ingestion_burst_size: 70000,

        max_series_per_metric: 100000,
        max_series_per_user: 1000000,

        max_series_per_query: 100000,
        max_samples_per_query: 1000000,
      },

      super_user:: {
        ingestion_rate: 200000,
        ingestion_burst_size: 240000,

        max_series_per_metric: 200000,
        max_series_per_user: 2000000,

        max_series_per_query: 100000,
        max_samples_per_query: 1000000,
      },
    },

    schemaID: std.md5(std.toString($._config.schema)),

    enable_pod_priorities: true,
  },

  local configMap = $.core.v1.configMap,

  overrides_config:
    configMap.new('overrides') +
    configMap.withData({
      'overrides.yaml': $.util.manifestYaml({
        overrides: $._config.overrides,
      }),
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
