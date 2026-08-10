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

      autoscaling_enabled: false,
      autoscaling_min_replicas: 1,
      autoscaling_max_replicas: 10,
    },
  },

  assert !$._config.block_builder.enabled || $._config.ingest_storage_enabled : 'block-builder requires ingest storage to be enabled',

  // The block-builder converts ingest storage partitions into TSDB blocks.
  block_builder_args::
    $._config.commonConfig +
    $._config.usageStatsConfig +
    $._config.grpcConfig +
    $._config.storageConfig +
    $._config.blocksStorageConfig +
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
      'block-builder.scheduler.address': 'block-builder-scheduler.%(namespace)s.svc.%(cluster_domain)s:9095' % $._config,

      // Reduce ingestion concurrency: block-builders are IO-bound and don't need to
      // process faster than the 1h job backlog window.
      'ingest-storage.kafka.ingestion-concurrency-max': 2,
      // Align fetch concurrency with GOMAXPROCS for better throughput / lower memory.
      'ingest-storage.kafka.fetch-concurrency-max': std.min(std.ceil($.util.parseCPU($.block_builder_container.resources.requests.cpu)), 4),
    } +
    $.mimirRuntimeConfigFile,

  block_builder_ports:: $.util.defaultPorts,

  block_builder_env_map:: {
    // Pin Go threads to CPU request: block-builders are IO-bound during fetch so extra threads
    // add little throughput but increase context-switch cost.
    GOMAXPROCS: std.toString($.util.parseCPU($.block_builder_container.resources.requests.cpu)),
    // Dynamically set GOMEMLIMIT based on memory limit to protect against OOM.
    GOMEMLIMIT: std.toString(std.floor($.util.siToBytes($.block_builder_container.resources.limits.memory))),
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

  newBlockBuilderDeployment(name, container, nodeAffinityMatchers=[])::
    deployment.new(name, $._config.block_builder.replicas, [container]) +
    $.newMimirNodeAffinityMatchers(nodeAffinityMatchers) +
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
    deployment.mixin.spec.strategy.rollingUpdate.withMaxUnavailable(0),

  // Scale formula: median job completion time * outstanding jobs / target time of 1 hour.
  // Targets enough replicas to drain the outstanding backlog within a 1-hour window.
  newBlockBuilderScaledObject(service_name, min_replicas, max_replicas, target_kind, scheduler_container='block-builder-scheduler', blockbuilder_container='block-builder', scheduler_extra_matchers='', blockbuilder_extra_matchers='')::
    local blockbuilder_filter = ', container="' + blockbuilder_container + '"' + (if blockbuilder_extra_matchers != '' then ', ' + blockbuilder_extra_matchers else '');
    local scheduler_filter = ', container="' + scheduler_container + '"' + (if scheduler_extra_matchers != '' then ', ' + scheduler_extra_matchers else '');
    local config = {
      min_replica_count: min_replicas,
      max_replica_count: max_replicas,
      triggers: [
        {
          metric_name: 'cortex_%s_hpa_%s' % [std.strReplace(service_name, '-', '_'), std.strReplace($._config.namespace, '-', '_')],
          query: |||
            avg(
              histogram_avg(sum(rate(cortex_blockbuilder_consume_job_duration_seconds{success="true", namespace="%(namespace)s"%(blockbuilder_filter)s}[1h])))
            )
            * max(
                max_over_time(cortex_blockbuilder_scheduler_outstanding_jobs{namespace="%(namespace)s"%(scheduler_filter)s}[1h])
            )
            / vector(60 * 60)
          ||| % {
            namespace: $._config.namespace,
            blockbuilder_filter: blockbuilder_filter,
            scheduler_filter: scheduler_filter,
          },
          threshold: '1',
          metric_type: 'AverageValue',
        },
      ],
    };
    self.newScaledObject(service_name, $._config.namespace, config, kind=target_kind) + {
      spec+: {
        advanced: {
          horizontalPodAutoscalerConfig: {
            behavior: {
              scaleUp: {
                policies: [{ type: 'Percent', value: 25, periodSeconds: $.util.parseDuration('15m') }],
                selectPolicy: 'Min',
                stabilizationWindowSeconds: $.util.parseDuration('10m'),
              },
              scaleDown: {
                policies: [{ type: 'Percent', value: 10, periodSeconds: $.util.parseDuration('30m') }],
                selectPolicy: 'Max',
                stabilizationWindowSeconds: $.util.parseDuration('1h'),
              },
            },
          },
        },
      },
    },

  block_builder_deployment: if !$._config.block_builder.enabled then null else
    self.newBlockBuilderDeployment('block-builder', $.block_builder_container, $.block_builder_node_affinity_matchers) +
    (if !$._config.block_builder.autoscaling_enabled then {} else $.removeReplicasFromSpec),

  block_builder_scaled_object: if !$._config.block_builder.enabled || !$._config.block_builder.autoscaling_enabled then null else
    $.newBlockBuilderScaledObject(
      service_name='block-builder',
      min_replicas=$._config.block_builder.autoscaling_min_replicas,
      max_replicas=$._config.block_builder.autoscaling_max_replicas,
      target_kind='Deployment',
    ),

  block_builder_pdb: if !$._config.block_builder.enabled then null else
    $.newMimirPdb('block-builder'),

  // Stop shipping ingester blocks when the block-builder is the sole L0 block producer.
  ingester_args+:: if !$._config.block_builder.enabled || $._config.block_builder.ingester_tsdb_ship_blocks_enabled then {} else {
    'blocks-storage.tsdb.ship-interval': 0,
    'blocks-storage.tsdb.close-idle-tsdb-when-shipping-disabled': true,
  },
}
