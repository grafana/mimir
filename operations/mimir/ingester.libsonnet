{
  ingester_args::
    $._config.grpcConfig +
    $._config.ringConfig +
    $._config.storageConfig +
    $._config.blocksStorageConfig +
    $._config.distributorConfig +  // This adds the distributor ring flags to the ingester.
    $._config.ingesterLimitsConfig +
    {
      target: 'ingester',

      // Ring config.
      'ingester.num-tokens': 512,
      'ingester.join-after': '30s',
      'ingester.heartbeat-period': '15s',
      'ingester.unregister-on-shutdown': $._config.unregister_ingesters_on_shutdown,

      'ingester.chunk-encoding': 3,

      // Limits config.
      'runtime-config.file': '/etc/cortex/overrides.yaml',
      'server.grpc-max-concurrent-streams': 10000,
      'server.grpc-max-send-msg-size-bytes': 10 * 1024 * 1024,
      'server.grpc-max-recv-msg-size-bytes': 10 * 1024 * 1024,
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

  ingester_deployment_labels:: {},

  ingester_service_ignored_labels:: [],

  local podDisruptionBudget = $.policy.v1beta1.podDisruptionBudget,

  newIngesterPdb(pdbName, ingesterName)::
    podDisruptionBudget.new() +
    podDisruptionBudget.mixin.metadata.withName(pdbName) +
    podDisruptionBudget.mixin.metadata.withLabels({ name: pdbName }) +
    podDisruptionBudget.mixin.spec.selector.withMatchLabels({ name: ingesterName }) +
    podDisruptionBudget.mixin.spec.withMaxUnavailable(1),

  ingester_pdb: self.newIngesterPdb('ingester-pdb', name),
}
