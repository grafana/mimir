{
  local container = $.core.v1.container,
  local containerPort = $.core.v1.containerPort,

  distributor_args::
    $._config.commonConfig +
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
      'distributor.ha-tracker.etcd.endpoints': 'etcd-client.%(namespace)s.svc.%(cluster_domain)s:2379' % $._config,
      'distributor.ha-tracker.prefix': 'prom_ha/',

      // The memory requests are 2G, and we barely use 100M.
      // By adding a ballast of 1G, we can drastically reduce GC, but also keep the usage at
      // around 1.25G, reducing the 99%ile.
      'mem-ballast-size-bytes': 1 << 30,  // 1GB

      'server.http-listen-port': $._config.server_http_port,

      // The ingestion rate global limit requires the distributors to form a ring.
      'distributor.ring.store': 'consul',
      'distributor.ring.consul.hostname': 'consul.%(namespace)s.svc.%(cluster_domain)s:8500' % $._config,
      'distributor.ring.prefix': '',
    } + $.mimirRuntimeConfigFile,

  distributor_ports:: $.util.defaultPorts,

  distributor_env_map:: {
    // Dynamically set GOMAXPROCS based on CPU request.
    GOMAXPROCS: std.toString(
      std.ceil(
        std.max(
          8,  // Always run on at least 8 gothreads, so that at least 2 of them (25%) are dedicated to GC.
          $.util.parseCPU($.distributor_container.resources.requests.cpu) * 2
        ),
      )
    ),
  },

  distributor_container::
    container.new('distributor', $._images.distributor) +
    container.withPorts($.distributor_ports) +
    container.withArgsMixin($.util.mapToFlags($.distributor_args)) +
    (if std.length($.distributor_env_map) > 0 then container.withEnvMap(std.prune($.distributor_env_map)) else {}) +
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
    deployment.mixin.spec.strategy.rollingUpdate.withMaxSurge('15%') +
    deployment.mixin.spec.strategy.rollingUpdate.withMaxUnavailable(0),

  local service = $.core.v1.service,

  distributor_service: if !$._config.is_microservices_deployment_mode then null else
    $.util.serviceFor($.distributor_deployment, $._config.service_ignored_labels) +
    service.mixin.spec.withClusterIp('None'),

  distributor_pdb: if !$._config.is_microservices_deployment_mode then null else
    $.newMimirPdb('distributor'),
}
