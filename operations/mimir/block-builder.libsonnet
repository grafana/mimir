{
  local container = $.core.v1.container,
  local deployment = $.apps.v1.deployment,
  local volumeMount = $.core.v1.volumeMount,

  _config+:: {
    block_builder: {
      // Disabled by default: the block-builder is an experimental component.
      enabled: false,

      replicas: 1,

      data_disk_size: '25Gi',
      data_disk_class: 'standard',

      // The flag controls whether ingesters ship TSDB blocks to object storage.
      // When true (default): both ingesters and block-builder produce blocks independently.
      // When false: only the block-builder produces blocks; ingesters stop shipping.
      // Set to false only when fully migrating to block-builder architecture.
      ingester_tsdb_ship_blocks_enabled: true,
    },
  },

  // The block-builder converts ingest storage partitions into TSDB blocks.
  block_builder_args::
    $._config.commonConfig +
    $._config.usageStatsConfig +
    $._config.grpcConfig +
    $._config.storageConfig +
    $._config.blocksStorageConfig +
    $._config.ingesterRingClientConfig +
    $._config.ingesterLimitsConfig +
    $.ingest_storage_args +
    $.ingest_storage_kafka_consumer_args +
    $.ingest_storage_kafka_ingestion_args +
    {
      target: 'block-builder',

      'server.http-listen-port': $._config.server_http_port,
      'server.grpc-max-concurrent-streams': 500,

      'blocks-storage.tsdb.block-ranges-period': '2h',
      'blocks-storage.tsdb.ship-interval': '1m',
      // Spread TSDB head compaction over a wider time range.
      'blocks-storage.tsdb.head-compaction-interval': '15m',

      'block-builder.data-dir': '/data/tsdb',
      'block-builder.scheduler.address': 'block-builder-scheduler.%(namespace)s.svc.cluster.local.:9095' % $._config,

      // Reduce ingestion concurrency: block-builders are IO-bound and don't need to
      // process faster than the 1h job backlog window.
      'ingest-storage.kafka.ingestion-concurrency-max': 2,
      // Align fetch concurrency with GOMAXPROCS for better throughput / lower memory.
      'ingest-storage.kafka.fetch-concurrency-max': '4',
    } +
    $.mimirRuntimeConfigFile,

  block_builder_ports:: $.util.defaultPorts,

  block_builder_env_map:: {
    // Pin Go to 4 threads: block-builders are IO-bound during fetch so extra threads
    // add little throughput but increase context-switch cost.
    GOMAXPROCS: '4',
    GOMEMLIMIT: {
      resourceFieldRef: {
        resource: 'limits.memory',
        divisor: 1,
      },
    },
  },

  block_builder_node_affinity_matchers:: [],

  newBlockBuilderContainer(name, args, envmap={})::
    container.new(name, $._images.block_builder) +
    container.withPorts($.block_builder_ports) +
    container.withArgsMixin($.util.mapToFlags(args)) +
    container.withVolumeMountsMixin([volumeMount.new('block-builder-data', '/data')]) +
    $.util.resourcesRequests('4', '8Gi') +
    $.util.resourcesLimits(null, '12Gi') +
    $.mimirEphemeralStorageRequest +
    $.util.readinessProbe +
    $.tracing_env_mixin +
    (if std.length(envmap) > 0 then container.withEnvMap(std.prune(envmap)) else {}),

  block_builder_container::
    self.newBlockBuilderContainer('block-builder', $.block_builder_args, $.block_builder_env_map),

  newBlockBuilderDeployment(name, container)::
    deployment.new(name, $._config.block_builder.replicas, [container]) +
    $.newMimirNodeAffinityMatchers($.block_builder_node_affinity_matchers) +
    deployment.spec.template.spec.withVolumes([
      {
        name: 'block-builder-data',
        ephemeral: {
          volumeClaimTemplate: {
            spec: {
              accessModes: ['ReadWriteOnce'],
              [if $._config.block_builder.data_disk_class != null then 'storageClassName']: $._config.block_builder.data_disk_class,
              resources: {
                requests: {
                  storage: $._config.block_builder.data_disk_size,
                },
              },
            },
          },
        },
      },
    ]) +
    $.mimirVolumeMounts +
    (if !std.isObject($._config.node_selector) then {} else deployment.mixin.spec.template.spec.withNodeSelectorMixin($._config.node_selector)) +
    deployment.mixin.spec.template.spec.withTerminationGracePeriodSeconds(30 * 60) +
    deployment.mixin.spec.strategy.rollingUpdate.withMaxSurge('25%') +
    deployment.mixin.spec.strategy.rollingUpdate.withMaxUnavailable('50%'),

  block_builder_deployment: if !$._config.block_builder.enabled then null else
    self.newBlockBuilderDeployment('block-builder', $.block_builder_container),
}

{
  // Stop shipping ingester blocks when the block-builder is the sole L0 block producer.
  ingester_args+:: if !$._config.block_builder.enabled || $._config.block_builder.ingester_tsdb_ship_blocks_enabled then {} else {
    'blocks-storage.tsdb.ship-interval': 0,
    'blocks-storage.tsdb.close-idle-tsdb-when-shipping-disabled': true,
  },
}
