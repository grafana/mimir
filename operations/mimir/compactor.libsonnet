{
  local pvc = $.core.v1.persistentVolumeClaim,
  local volumeMount = $.core.v1.volumeMount,
  local container = $.core.v1.container,
  local statefulSet = $.apps.v1.statefulSet,

  compactor_args::
    $._config.grpcConfig +
    $._config.storageConfig +
    $._config.blocksStorageConfig +
    $._config.compactorLimitsConfig +
    {
      target: 'compactor',

      // Compactor config.
      'compactor.block-ranges': '2h,12h,24h',
      'compactor.data-dir': '/data',
      'compactor.compaction-interval': '30m',
      'compactor.compaction-concurrency': $._config.cortex_compactor_max_concurrency,
      'compactor.cleanup-interval': $._config.cortex_compactor_cleanup_interval,

      // Enable sharding.
      'compactor.sharding-enabled': true,
      'compactor.ring.store': 'consul',
      'compactor.ring.consul.hostname': 'consul.%s.svc.cluster.local:8500' % $._config.namespace,
      'compactor.ring.prefix': '',

      // Limits config.
      'runtime-config.file': '/etc/cortex/overrides.yaml',
    },

  // The compactor runs a statefulset with a single replica, because
  // it does not support horizontal scalability yet.
  local compactor_data_pvc =
    pvc.new() +
    pvc.mixin.spec.resources.withRequests({ storage: $._config.cortex_compactor_data_disk_size }) +
    pvc.mixin.spec.withAccessModes(['ReadWriteOnce']) +
    pvc.mixin.spec.withStorageClassName($._config.cortex_compactor_data_disk_class) +
    pvc.mixin.metadata.withName('compactor-data'),

  compactor_ports:: $.util.defaultPorts,

  compactor_container::
    container.new('compactor', $._images.compactor) +
    container.withPorts($.compactor_ports) +
    container.withArgsMixin($.util.mapToFlags($.compactor_args)) +
    container.withVolumeMountsMixin([volumeMount.new('compactor-data', '/data')]) +
    // Do not limit compactor CPU and request enough cores to honor configured max concurrency.
    $.util.resourcesRequests($._config.cortex_compactor_max_concurrency, '6Gi') +
    $.util.resourcesLimits(null, '6Gi') +
    $.util.readinessProbe +
    $.jaeger_mixin,

  newCompactorStatefulSet(name, container)::
    statefulSet.new(name, 1, [container], compactor_data_pvc) +
    statefulSet.mixin.spec.withServiceName(name) +
    statefulSet.mixin.metadata.withNamespace($._config.namespace) +
    statefulSet.mixin.metadata.withLabels({ name: name }) +
    statefulSet.mixin.spec.template.metadata.withLabels({ name: name }) +
    statefulSet.mixin.spec.selector.withMatchLabels({ name: name }) +
    statefulSet.mixin.spec.template.spec.securityContext.withRunAsUser(0) +
    statefulSet.mixin.spec.updateStrategy.withType('RollingUpdate') +
    statefulSet.mixin.spec.template.spec.withTerminationGracePeriodSeconds(900) +
    // Parallelly scale up/down compactor instances instead of starting them
    // one by one. This does NOT affect rolling updates: they will continue to be
    // rolled out one by one (the next pod will be rolled out once the previous is
    // ready).
    statefulSet.mixin.spec.withPodManagementPolicy('Parallel') +
    $.util.configVolumeMount($._config.overrides_configmap, '/etc/cortex'),

  compactor_statefulset:
    $.newCompactorStatefulSet('compactor', $.compactor_container),
}
