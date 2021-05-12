{
  ingester_args::
    $._config.grpcConfig +
    $._config.ringConfig +
    $._config.storeConfig +
    $._config.storageConfig +
    $._config.blocksStorageConfig +
    $._config.distributorConfig +  // This adds the distributor ring flags to the ingester.
    {
      target: 'ingester',

      // Ring config.
      'ingester.num-tokens': 512,
      'ingester.join-after': '30s',
      'ingester.max-transfer-retries': 60,  // Each retry is backed off by 5s, so 5mins for new ingester to come up.
      'ingester.heartbeat-period': '15s',
      'ingester.max-stale-chunk-idle': '5m',
      'ingester.unregister-on-shutdown': $._config.unregister_ingesters_on_shutdown,

      // Chunk building/flushing config.
      'ingester.chunk-encoding': 3,  // Bigchunk encoding
      'ingester.retain-period': '15m',
      'ingester.max-chunk-age': '6h',

      // Limits config.
      'ingester.max-chunk-idle': $._config.max_chunk_idle,
      'ingester.max-series-per-user': $._config.limits.max_series_per_user,
      'ingester.max-series-per-metric': $._config.limits.max_series_per_metric,
      'ingester.max-global-series-per-user': $._config.limits.max_global_series_per_user,
      'ingester.max-global-series-per-metric': $._config.limits.max_global_series_per_metric,
      'ingester.max-series-per-query': $._config.limits.max_series_per_query,
      'ingester.max-samples-per-query': $._config.limits.max_samples_per_query,
      'runtime-config.file': '/etc/cortex/overrides.yaml',
      'server.grpc-max-concurrent-streams': 100000,
    } + (
      if $._config.memcached_index_writes_enabled then
        {
          // Setup index write deduping.
          'store.index-cache-write.memcached.hostname': 'memcached-index-writes.%(namespace)s.svc.cluster.local' % $._config,
          'store.index-cache-write.memcached.service': 'memcached-client',
        }
      else {}
    ),

  ingester_statefulset_args::
    $._config.grpcConfig
    {
      'ingester.wal-enabled': true,
      'ingester.checkpoint-enabled': true,
      'ingester.recover-from-wal': true,
      'ingester.wal-dir': $._config.ingester.wal_dir,
      'ingester.checkpoint-duration': '15m',
      '-log.level': 'info',
      'ingester.tokens-file-path': $._config.ingester.wal_dir + '/tokens',
    },

  ingester_ports:: $.util.defaultPorts,

  local name = 'ingester',
  local container = $.core.v1.container,

  ingester_container::
    container.new(name, $._images.ingester) +
    container.withPorts($.ingester_ports) +
    container.withArgsMixin($.util.mapToFlags($.ingester_args)) +
    $.util.resourcesRequests('4', '15Gi') +
    $.util.resourcesLimits(null, '25Gi') +
    $.util.readinessProbe +
    $.jaeger_mixin,

  local volumeMount = $.core.v1.volumeMount,

  ingester_statefulset_container::
    $.ingester_container +
    container.withArgsMixin($.util.mapToFlags($.ingester_statefulset_args)) +
    container.withVolumeMountsMixin([
      volumeMount.new('ingester-pvc', $._config.ingester.wal_dir),
    ]),

  ingester_deployment_labels:: {},

  local pvc = $.core.v1.persistentVolumeClaim,
  local volume = $.core.v1.volume,
  local statefulSet = $.apps.v1.statefulSet,

  local ingester_pvc =
    pvc.new('ingester-pvc') +
    pvc.mixin.spec.resources.withRequests({ storage: $._config.ingester.statefulset_disk }) +
    pvc.mixin.spec.withAccessModes(['ReadWriteOnce']) +
    pvc.mixin.spec.withStorageClassName('fast'),

  statefulset_storage_config_mixin::
    statefulSet.mixin.spec.template.metadata.withAnnotationsMixin({ schemaID: $._config.schemaID },) +
    $.util.configVolumeMount('schema-' + $._config.schemaID, '/etc/cortex/schema'),

  ingester_statefulset:
    if $._config.ingester_deployment_without_wal == false then
      statefulSet.new('ingester', 3, [$.ingester_statefulset_container], ingester_pvc) +
      statefulSet.mixin.spec.withServiceName('ingester') +
      statefulSet.mixin.spec.template.spec.withVolumes([volume.fromPersistentVolumeClaim('ingester-pvc', 'ingester-pvc')]) +
      statefulSet.mixin.metadata.withNamespace($._config.namespace) +
      statefulSet.mixin.metadata.withLabels({ name: 'ingester' }) +
      statefulSet.mixin.spec.template.metadata.withLabels({ name: 'ingester' } + $.ingester_deployment_labels) +
      statefulSet.mixin.spec.selector.withMatchLabels({ name: 'ingester' }) +
      statefulSet.mixin.spec.template.spec.securityContext.withRunAsUser(0) +
      statefulSet.mixin.spec.template.spec.withTerminationGracePeriodSeconds(4800) +
      statefulSet.mixin.spec.updateStrategy.withType('RollingUpdate') +
      $.statefulset_storage_config_mixin +
      $.util.configVolumeMount($._config.overrides_configmap, '/etc/cortex') +
      $.util.podPriority('high') +
      $.util.antiAffinityStatefulSet
    else null,

  local deployment = $.apps.v1.deployment,

  ingester_deployment:
    if $._config.ingester_deployment_without_wal then
      deployment.new(name, 3, [$.ingester_container], $.ingester_deployment_labels) +
      $.util.antiAffinity +
      $.util.configVolumeMount($._config.overrides_configmap, '/etc/cortex') +
      deployment.mixin.metadata.withLabels({ name: name }) +
      deployment.mixin.spec.withMinReadySeconds(60) +
      deployment.mixin.spec.strategy.rollingUpdate.withMaxSurge(0) +
      deployment.mixin.spec.strategy.rollingUpdate.withMaxUnavailable(1) +
      deployment.mixin.spec.template.spec.withTerminationGracePeriodSeconds(4800) +
      $.storage_config_mixin +
      $.util.podPriority('high')
    else null,

  ingester_service_ignored_labels:: [],

  ingester_service:
    if $._config.ingester_deployment_without_wal then
      $.util.serviceFor($.ingester_deployment, $.ingester_service_ignored_labels)
    else
      $.util.serviceFor($.ingester_statefulset, $.ingester_service_ignored_labels),

  local podDisruptionBudget = $.policy.v1beta1.podDisruptionBudget,

  newIngesterPdb(pdbName, ingesterName)::
    podDisruptionBudget.new() +
    podDisruptionBudget.mixin.metadata.withName(pdbName) +
    podDisruptionBudget.mixin.metadata.withLabels({ name: pdbName }) +
    podDisruptionBudget.mixin.spec.selector.withMatchLabels({ name: ingesterName }) +
    podDisruptionBudget.mixin.spec.withMaxUnavailable(1),

  ingester_pdb: self.newIngesterPdb('ingester-pdb', name),
}
