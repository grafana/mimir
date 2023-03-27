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
      'query-frontend.cache-results': true,

      // So that exporters like cloudwatch can still send in data and be un-cached.
      'query-frontend.max-cache-freshness': '10m',

      // Limit queries to 500 days; allow this to be overridden on a per-user basis.
      'query-frontend.max-total-query-length': '12000h',  // 500 days
    } +
    // results-cache using memcached
    (
      if $._config.cache_results_backend == 'memcached' then {
        'query-frontend.results-cache.backend': 'memcached',
        'query-frontend.results-cache.memcached.addresses': 'dnssrvnoa+%(cache_frontend_backend)s-frontend.%(namespace)s.svc.cluster.local:11211' % $._config,
        'query-frontend.results-cache.memcached.timeout': '500ms',
      } + if $._config.memcached_frontend_mtls_enabled then {
        'query-frontend.results-cache.memcached.addresses': 'dnssrvnoa+%(cache_frontend_backend)s-frontend.%(namespace)s.svc.cluster.local:11212' % $._config,
        'query-frontend.results-cache.memcached.connect-timeout': '1s',
        'query-frontend.results-cache.memcached.tls-enabled': true,
        'query-frontend.results-cache.memcached.tls-ca-path': $._config.memcached_ca_cert_path + $._config.memcached_mtls_ca_cert_secret + '.pem',
        'query-frontend.results-cache.memcached.tls-key-path': $._config.memcached_client_key_path + $._config.memcached_mtls_client_key_secret + '.pem',
        'query-frontend.results-cache.memcached.tls-cert-path': $._config.memcached_client_cert_path + $._config.memcached_mtls_client_cert_secret + '.pem',
        'query-frontend.results-cache.memcached.tls-server-name': if $._config.memcached_mtls_server_name != null then $._config.memcached_mtls_server_name else null,
      } else {}
      else {}
    ) +
    // results-cache using redis
    (
      if $._config.cache_results_backend == 'redis' then {
        'query-frontend.results-cache.backend': 'redis',
        'query-frontend.results-cache.redis.endpoint': '%(cache_results_name)s-redis-master.%(namespace)s.svc.cluster.local:6379' % $._config,
      } else {}
    ) + $.mimirRuntimeConfigFile,

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
    deployment.new(name, $._config.queryFrontend.replicas, [container]) +
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
