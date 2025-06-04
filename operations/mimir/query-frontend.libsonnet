{
  local container = $.core.v1.container,

  // Fine-tune the shutdown delay so that we let the client make 2 DNS resolutions before shutting down.
  local max_connection_age_seconds = 30,
  local max_dns_propagation_delay = 30,
  local shutdown_delay_seconds = (2 * max_connection_age_seconds) + max_dns_propagation_delay,

  query_frontend_args::
    $._config.commonConfig +
    $._config.usageStatsConfig +
    $._config.grpcConfig +
    $._config.querySchedulerRingClientConfig +
    $.query_frontend_caching_config +
    {
      target: 'query-frontend',

      'server.http-listen-port': $._config.server_http_port,
      'query-frontend.query-sharding-target-series-per-shard': if $.query_frontend_enable_cardinality_estimation then '2500' else '0',

      // Limit queries to 500 days; allow this to be overridden on a per-user basis.
      'query-frontend.max-total-query-length': '12000h',  // 500 days

      // Prolong query-frontend shutdown to allow any GRPC clients to receive the DNS update.
      'shutdown-delay': '%ds' % shutdown_delay_seconds,

      // Allow DNS changes to propagate before killing off query-frontends,
      // to avoid connection failures in ruler and cortex-gw and therefore 5xx reads.
      'server.grpc.keepalive.max-connection-age': '%ds' % max_connection_age_seconds,
    } + $.mimirRuntimeConfigFile,

  query_frontend_ports:: $.util.defaultPorts,

  newQueryFrontendContainer(name, args, envmap={})::
    container.new(name, $._images.query_frontend) +
    container.withPorts($.query_frontend_ports) +
    container.withArgsMixin($.util.mapToFlags(args)) +
    (if std.length(envmap) > 0 then container.withEnvMap(std.prune(envmap)) else {}) +
    $.jaeger_mixin +
    $.util.readinessProbe +
    $.util.resourcesRequests('2', '600Mi') +
    $.util.resourcesLimits(null, '1200Mi'),

  query_frontend_env_map:: {
    JAEGER_REPORTER_MAX_QUEUE_SIZE: '5000',
  },

  query_frontend_node_affinity_matchers:: [],

  query_frontend_container::
    self.newQueryFrontendContainer('query-frontend', $.query_frontend_args, $.query_frontend_env_map),

  local deployment = $.apps.v1.deployment,

  newQueryFrontendDeployment(name, container, nodeAffinityMatchers=[])::
    deployment.new(name, 2, [container]) +
    $.mimirVolumeMounts +
    $.newMimirSpreadTopology(name, $._config.query_frontend_topology_spread_max_skew) +
    $.newMimirNodeAffinityMatchers(nodeAffinityMatchers) +
    (if !std.isObject($._config.node_selector) then {} else deployment.mixin.spec.template.spec.withNodeSelectorMixin($._config.node_selector)) +
    deployment.mixin.spec.strategy.rollingUpdate.withMaxSurge('15%') +
    deployment.mixin.spec.strategy.rollingUpdate.withMaxUnavailable(0) +

    // Leave enough time to finish serving a 5m query after the shutdown delay expired.
    deployment.mixin.spec.template.spec.withTerminationGracePeriodSeconds(shutdown_delay_seconds + 300),

  query_frontend_deployment: if !$._config.is_microservices_deployment_mode then null else
    self.newQueryFrontendDeployment('query-frontend', $.query_frontend_container, $.query_frontend_node_affinity_matchers),

  query_frontend_service: if !$._config.is_microservices_deployment_mode then null else
    $.util.serviceFor($.query_frontend_deployment, $._config.service_ignored_labels),

  query_frontend_discovery_service: if !$._config.is_microservices_deployment_mode || $._config.query_scheduler_enabled then null else
    // Make sure that query frontend worker, running in the querier, do resolve
    // each query-frontend pod IP and NOT the service IP. To make it, we do NOT
    // use the service cluster IP so that when the service DNS is resolved it
    // returns the set of query-frontend IPs.
    $.newMimirDiscoveryService('query-frontend-discovery', $.query_frontend_deployment),

  query_frontend_pdb: if !$._config.is_microservices_deployment_mode then null else
    $.newMimirPdb('query-frontend'),
}
