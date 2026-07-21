{
  local container = $.core.v1.container,
  local statefulSet = $.apps.v1.statefulSet,
  local service = $.core.v1.service,

  _config+:: {
    block_builder: {
      // Disabled by default: the block-builder is an experimental component.
      enabled: false,

      // How many jobs the scheduler hands out per partition at once. A value of 1 is passed implicitly
      // (it's the block-builder-scheduler default), so only set the flag when it differs.
      scheduler_max_jobs_per_partition: 3,
    },
  },

  // The block-builder-scheduler plans block-building jobs from Kafka and leases them to block-builders.
  block_builder_scheduler_args::
    $._config.commonConfig +
    $._config.usageStatsConfig +
    $._config.grpcConfig +
    $.ingest_storage_args +
    $.ingest_storage_kafka_consumer_args +
    {
      target: 'block-builder-scheduler',

      [if $._config.block_builder.scheduler_max_jobs_per_partition != 1 then 'block-builder-scheduler.max-jobs-per-partition']:
        $._config.block_builder.scheduler_max_jobs_per_partition,

      'server.http-listen-port': $._config.server_http_port,
      'server.grpc-max-concurrent-streams': 500,
    } +
    $.mimirRuntimeConfigFile,

  block_builder_scheduler_ports:: $.util.defaultPorts,

  // Pin the Go runtime to the container's own limits, keeping scheduling overheads and heap usage bounded.
  block_builder_scheduler_env_map:: {
    GOMAXPROCS: {
      resourceFieldRef: {
        resource: 'limits.cpu',
        divisor: 1,
      },
    },
    GOMEMLIMIT: {
      resourceFieldRef: {
        resource: 'limits.memory',
        divisor: 1,
      },
    },
  },

  block_builder_scheduler_node_affinity_matchers:: [],

  newBlockBuilderSchedulerContainer(name, args, envmap={})::
    container.new(name, $._images.block_builder_scheduler) +
    container.withPorts($.block_builder_scheduler_ports) +
    container.withArgsMixin($.util.mapToFlags(args)) +
    $.util.resourcesRequests('0.1', '512Mi') +
    $.util.resourcesLimits('0.25', '1Gi') +
    $.mimirEphemeralStorageRequest +
    $.util.readinessProbe +
    $.tracing_env_mixin +
    (if std.length(envmap) > 0 then container.withEnvMap(std.prune(envmap)) else {}),

  block_builder_scheduler_container::
    self.newBlockBuilderSchedulerContainer('block-builder-scheduler', $.block_builder_scheduler_args, $.block_builder_scheduler_env_map),

  // The scheduler keeps its state in memory and rebuilds it on startup, so its StatefulSet is storage-less.
  newBlockBuilderSchedulerStatefulSet(name, container, nodeAffinityMatchers=[])::
    $.newMimirStatefulSet(name, 1, container, []) +
    $.newMimirNodeAffinityMatchers(nodeAffinityMatchers) +
    $.mimirVolumeMounts,

  block_builder_scheduler_statefulset: if !$._config.block_builder.enabled then null else
    self.newBlockBuilderSchedulerStatefulSet('block-builder-scheduler', $.block_builder_scheduler_container, $.block_builder_scheduler_node_affinity_matchers),

  newBlockBuilderSchedulerService(name, sts)::
    $.util.serviceFor(sts, $._config.service_ignored_labels) +
    service.mixin.spec.withSelector({
      name: name,
      // Select only the first replica, in case somehow multiple are running.
      'statefulset.kubernetes.io/pod-name': '%s-0' % name,
    }),

  block_builder_scheduler_service: if !$._config.block_builder.enabled then null else
    self.newBlockBuilderSchedulerService('block-builder-scheduler', $.block_builder_scheduler_statefulset),

  block_builder_scheduler_pdb: if !$._config.block_builder.enabled then null else
    $.newMimirPdb('block-builder-scheduler'),
}
