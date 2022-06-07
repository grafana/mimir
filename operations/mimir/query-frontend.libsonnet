{
  local container = $.core.v1.container,

  query_frontend_args::
    $._config.grpcConfig
    {
      target: 'query-frontend',

      'server.http-listen-port': $._config.server_http_port,

      // Increase HTTP server response write timeout, as we were seeing some
      // queries that return a lot of data timeing out.
      'server.http-write-timeout': '1m',

      // Cache query results.
      'query-frontend.align-querier-with-step': false,
      'query-frontend.cache-results': true,
      'query-frontend.results-cache.backend': 'memcached',
      'query-frontend.results-cache.memcached.addresses': 'dnssrvnoa+memcached-frontend.%(namespace)s.svc.cluster.local:11211' % $._config,
      'query-frontend.results-cache.memcached.timeout': '500ms',

      // So that exporters like cloudwatch can still send in data and be un-cached.
      'query-frontend.max-cache-freshness': '10m',

      // Limit queries to 500 days, allow this to be override per-user.
      'store.max-query-length': '12000h',  // 500 Days
      'runtime-config.file': '%s/overrides.yaml' % $._config.overrides_configmap_mountpoint,
    },

  newQueryFrontendContainer(name, args)::
    container.new(name, $._images.query_frontend) +
    container.withPorts($.util.defaultPorts) +
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
    $.util.configVolumeMount($._config.overrides_configmap, $._config.overrides_configmap_mountpoint) +
    (if $._config.query_frontend_allow_multiple_replicas_on_same_node then {} else $.util.antiAffinity) +
    (if !std.isObject($._config.node_selector) then {} else deployment.mixin.spec.template.spec.withNodeSelectorMixin($._config.node_selector)) +
    deployment.mixin.spec.strategy.rollingUpdate.withMaxSurge(1) +
    deployment.mixin.spec.strategy.rollingUpdate.withMaxUnavailable(1),

  query_frontend_deployment: self.newQueryFrontendDeployment('query-frontend', $.query_frontend_container),

  query_frontend_service:
    $.util.serviceFor($.query_frontend_deployment, $._config.service_ignored_labels),

  query_frontend_discovery_service:
    // Make sure that query frontend worker, running in the querier, do resolve
    // each query-frontend pod IP and NOT the service IP. To make it, we do NOT
    // use the service cluster IP so that when the service DNS is resolved it
    // returns the set of query-frontend IPs.
    $.newMimirDiscoveryService('query-frontend-discovery', $.query_frontend_deployment),
}
