{
  local container = $.core.v1.container,
  local containerPort = $.core.v1.containerPort,

  distributor_args::
    $._config.usageStatsConfig +
    $._config.grpcConfig +
    $._config.grpcIngressConfig +
    $._config.ingesterRingClientConfig +
    $._config.distributorLimitsConfig +
    {
      target: 'distributor',

      'distributor.ha-tracker.enable': true,
      'distributor.ha-tracker.enable-for-all-users': true,
      'distributor.ha-tracker.store': 'etcd',
      'distributor.ha-tracker.etcd.endpoints': 'etcd-client.%s.svc.cluster.local.:2379' % $._config.namespace,
      'distributor.ha-tracker.prefix': 'prom_ha/',

      'server.http-listen-port': $._config.server_http_port,

      // The ingestion rate global limit requires the distributors to form a ring.
      'distributor.ring.store': 'consul',
      'distributor.ring.consul.hostname': 'consul.%s.svc.cluster.local:8500' % $._config.namespace,
      'distributor.ring.prefix': '',
    } + $.mimirRuntimeConfigFile,

  distributor_ports:: $.util.defaultPorts,

  distributor_env_map:: {
    // Dynamically set GOMEMLIMIT based on memory request.
    GOMEMLIMIT: std.toString(std.floor($.util.siToBytes($.distributor_container.resources.requests.memory))),

    // We don't want to run GC unless GOMEMLIMIT is reached. We could use GOGC=off, but we use numeric value instead to
    // run GC regularly even when GOMEMLIMIT is not reached yet.
    GOGC: '1000',

    // We set GOMAXPROCS as otherwise Go runtime may use 50% cores on the node to run GC when memory reaches GOMEMLIMIT.
    // On nodes with lot of cores that seems unnecessary.
    // We want to allow for some spikes, hence the *2 factor.
    GOMAXPROCS: std.toString(
      std.ceil($.util.parseCPU($.distributor_container.resources.requests.cpu) * 2,)
    ),
  },

  distributor_container::
    container.new('distributor', $._images.distributor) +
    container.withPorts($.distributor_ports) +
    container.withArgsMixin($.util.mapToFlags($.distributor_args)) +
    (if std.length($.distributor_env_map) > 0 then container.withEnvMap($.distributor_env_map) else {}) +
    $.util.resourcesRequests('2', '2Gi') +
    $.util.resourcesLimits(null, '4Gi') +
    $.util.readinessProbe +
    $.jaeger_mixin,

  local deployment = $.apps.v1.deployment,

  distributor_deployment: if !$._config.is_microservices_deployment_mode then null else
    deployment.new('distributor', 3, [$.distributor_container]) +
    $.newMimirSpreadTopology('distributor', $._config.distributor_topology_spread_max_skew) +
    $.mimirVolumeMounts +
    (if !std.isObject($._config.node_selector) then {} else deployment.mixin.spec.template.spec.withNodeSelectorMixin($._config.node_selector)) +
    deployment.mixin.spec.strategy.rollingUpdate.withMaxSurge(5) +
    deployment.mixin.spec.strategy.rollingUpdate.withMaxUnavailable(1),

  local service = $.core.v1.service,

  distributor_service: if !$._config.is_microservices_deployment_mode then null else
    $.util.serviceFor($.distributor_deployment, $._config.service_ignored_labels) +
    service.mixin.spec.withClusterIp('None'),
}
