{
  local container = $.core.v1.container,
  local pvc = $.core.v1.persistentVolumeClaim,
  local statefulSet = $.apps.v1.statefulSet,
  local volumeMount = $.core.v1.volumeMount,

  ingester_args::
    $._config.usageStatsConfig +
    $._config.grpcConfig +
    $._config.storageConfig +
    $._config.blocksStorageConfig +
    $._config.ingesterRingClientConfig +
    $._config.ingesterLimitsConfig +
    {
      target: 'ingester',

      'server.http-listen-port': $._config.server_http_port,

      // Ring config.
      'ingester.ring.num-tokens': 512,
      'ingester.ring.unregister-on-shutdown': $._config.unregister_ingesters_on_shutdown,

      // Limits config.
      'server.grpc-max-concurrent-streams': 10000,

      // Blocks storage.
      'blocks-storage.tsdb.dir': '/data/tsdb',
      'blocks-storage.tsdb.block-ranges-period': '2h',
      'blocks-storage.tsdb.ship-interval': '1m',

      // Persist ring tokens so that when the ingester will be restarted
      // it will pick the same tokens
      'ingester.ring.tokens-file-path': '/data/tokens',
    } + $.mimirRuntimeConfigFile,

  ingester_ports:: $.util.defaultPorts,

  local name = 'ingester',

  ingester_container::
    container.new(name, $._images.ingester) +
    container.withPorts($.ingester_ports) +
    container.withArgsMixin($.util.mapToFlags($.ingester_args)) +
    $.util.resourcesRequests('4', '15Gi') +
    $.util.resourcesLimits(null, '25Gi') +
    $.util.readinessProbe +
    $.jaeger_mixin,

  // The ingesters should persist TSDB blocks and WAL on a persistent
  // volume in order to be crash resilient.
  local ingester_data_pvc =
    pvc.new() +
    pvc.mixin.spec.resources.withRequests({ storage: $._config.ingester_data_disk_size }) +
    pvc.mixin.spec.withAccessModes(['ReadWriteOnce']) +
    pvc.mixin.spec.withStorageClassName($._config.ingester_data_disk_class) +
    pvc.mixin.metadata.withName('ingester-data'),

  newIngesterStatefulSet(name, container, with_anti_affinity=true)::
    local ingesterContainer = container + $.core.v1.container.withVolumeMountsMixin([
      volumeMount.new('ingester-data', '/data'),
    ]);

    $.newMimirStatefulSet(name, 3, ingesterContainer, ingester_data_pvc) +
    // When the ingester needs to flush blocks to the storage, it may take quite a lot of time.
    // For this reason, we grant an high termination period (80 minutes).
    statefulSet.mixin.spec.template.spec.withTerminationGracePeriodSeconds(1200) +
    $.mimirVolumeMounts +
    $.util.podPriority('high') +
    (if with_anti_affinity then $.util.antiAffinity else {}),

  ingester_statefulset: if !$._config.is_microservices_deployment_mode then null else
    self.newIngesterStatefulSet('ingester', $.ingester_container, !$._config.ingester_allow_multiple_replicas_on_same_node),

  ingester_service: if !$._config.is_microservices_deployment_mode then null else
    $.util.serviceFor($.ingester_statefulset, $._config.service_ignored_labels),

  newIngesterPdb(ingesterName)::
    $.newMimirPdb(ingesterName),

  ingester_pdb: if !$._config.is_microservices_deployment_mode then null else
    self.newIngesterPdb(name),
}
