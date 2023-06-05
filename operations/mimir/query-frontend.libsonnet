{
  local container = $.core.v1.container,

  query_frontend_args::
    $._config.usageStatsConfig +
    $._config.grpcConfig +
    $._config.querySchedulerRingClientConfig +
    $.query_frontend_caching_config +
    {
      target: 'query-frontend',

      'server.http-listen-port': $._config.server_http_port,
      'query-frontend.align-queries-with-step': false,

      // Limit queries to 500 days; allow this to be overridden on a per-user basis.
      'query-frontend.max-total-query-length': '12000h',  // 500 days
    } + $.mimirRuntimeConfigFile,

  query_frontend_ports:: $.util.defaultPorts,

  newQueryFrontendContainer(name, args)::
    container.new(name, $._images.query_frontend) +
    container.withPorts($.query_frontend_ports) +
    container.withArgsMixin($.util.mapToFlags(args)) +
    $.jaeger_mixin +
    $.util.readinessProbe +
    $.util.resourcesRequests('2', '600Mi') +
    $.util.resourcesLimits(null, '1200Mi'),

  query_frontend_container::
    self.newQueryFrontendContainer('query-frontend', $.query_frontend_args),

  local deployment = $.apps.v1.deployment,

  newQueryFrontendDeployment(name, container)::
    deployment.new(name, 2, [container]) +
    $.mimirVolumeMounts +
    $.newMimirSpreadTopology(name, $._config.query_frontend_topology_spread_max_skew) +
    (if !std.isObject($._config.node_selector) then {} else deployment.mixin.spec.template.spec.withNodeSelectorMixin($._config.node_selector)) +
    deployment.mixin.spec.strategy.rollingUpdate.withMaxSurge(1) +
    deployment.mixin.spec.strategy.rollingUpdate.withMaxUnavailable(1),

  query_frontend_deployment: if !$._config.is_microservices_deployment_mode then null else
    self.newQueryFrontendDeployment('query-frontend', $.query_frontend_container),

  query_frontend_service: if !$._config.is_microservices_deployment_mode then null else
    $.util.serviceFor($.query_frontend_deployment, $._config.service_ignored_labels),

  query_frontend_discovery_service: if !$._config.is_microservices_deployment_mode || $._config.query_scheduler_enabled then null else
    // Make sure that query frontend worker, running in the querier, do resolve
    // each query-frontend pod IP and NOT the service IP. To make it, we do NOT
    // use the service cluster IP so that when the service DNS is resolved it
    // returns the set of query-frontend IPs.
    $.newMimirDiscoveryService('query-frontend-discovery', $.query_frontend_deployment),
}
