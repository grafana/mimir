{
  local container = $.core.v1.container,

  ingester_args::
    $._config.ringConfig +
    $._config.storeConfig +
    $._config.storageConfig +
    $._config.distributorConfig +  // This adds the distributor ring flags to the ingester.
    {
      target: 'ingester',

      // Ring config.
      'ingester.num-tokens': 512,
      'ingester.join-after': '30s',
      'ingester.max-transfer-retries': 60,  // Each retry is backed off by 5s, so 5mins for new ingester to come up.
      'ingester.heartbeat-period': '15s',
      'ingester.max-stale-chunk-idle': '5m',

      // Chunk building/flushing config.
      'ingester.chunk-encoding': 3,  // Bigchunk encoding
      'ingester.retain-period': '15m',
      'ingester.max-chunk-age': '6h',

      // Limits config.
      'ingester.max-chunk-idle': $._config.max_chunk_idle,
      'ingester.max-global-series-per-user': 1000000,  // 1M
      'ingester.max-global-series-per-metric': 100000,  // 100K
      'ingester.max-series-per-user': 0,  // Disabled in favour of the max global limit
      'ingester.max-series-per-metric': 0,  // Disabled in favour of the max global limit
      'limits.per-user-override-config': '/etc/cortex/overrides.yaml',
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

  local deployment = $.apps.v1.deployment,

  ingester_deployment_labels:: {},

  ingester_deployment:
    deployment.new(name, 3, [$.ingester_container], $.ingester_deployment_labels) +
    $.util.antiAffinity +
    $.util.configVolumeMount('overrides', '/etc/cortex') +
    deployment.mixin.metadata.withLabels({ name: name }) +
    deployment.mixin.spec.withMinReadySeconds(60) +
    deployment.mixin.spec.strategy.rollingUpdate.withMaxSurge(0) +
    deployment.mixin.spec.strategy.rollingUpdate.withMaxUnavailable(1) +
    deployment.mixin.spec.template.spec.withTerminationGracePeriodSeconds(4800) +
    $.storage_config_mixin +
    $.util.podPriority('high'),

  ingester_service_ignored_labels:: [],

  ingester_service:
    $.util.serviceFor($.ingester_deployment, $.ingester_service_ignored_labels),

  local podDisruptionBudget = $.policy.v1beta1.podDisruptionBudget,

  newIngesterPdb(pdbName, ingesterName)::
    podDisruptionBudget.new() +
    podDisruptionBudget.mixin.metadata.withName(pdbName) +
    podDisruptionBudget.mixin.metadata.withLabels({ name: pdbName }) +
    podDisruptionBudget.mixin.spec.selector.withMatchLabels({ name: ingesterName }) +
    podDisruptionBudget.mixin.spec.withMaxUnavailable(1),

  ingester_pdb: self.newIngesterPdb('ingester-pdb', name),
}
