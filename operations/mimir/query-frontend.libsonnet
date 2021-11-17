{
  local container = $.core.v1.container,

  query_frontend_args::
    $._config.grpcConfig
    {
      target: 'query-frontend',

      // Need log.level=debug so all queries are logged, needed for analyse.py.
      'log.level': 'debug',

      // Increase HTTP server response write timeout, as we were seeing some
      // queries that return a lot of data timeing out.
      'server.http-write-timeout': '1m',

      // Split long queries up into multiple day-long queries.
      'querier.split-queries-by-interval': '24h',

      // Cache query results.
      'querier.align-querier-with-step': true,
      'querier.cache-results': true,
      'frontend.memcached.hostname': 'memcached-frontend.%s.svc.cluster.local' % $._config.namespace,
      'frontend.memcached.service': 'memcached-client',
      'frontend.memcached.timeout': '500ms',

      // So that exporters like cloudwatch can still send in data and be un-cached.
      'frontend.max-cache-freshness': '10m',

      // Use GZIP compression for API responses; improves latency for very big results and slow
      // connections.
      'api.response-compression-enabled': true,

      // So it can receive big responses from the querier.
      'server.grpc-max-recv-msg-size-bytes': 100 << 20,

      // Limit queries to 500 days, allow this to be override per-user.
      'store.max-query-length': '12000h',  // 500 Days
      'runtime-config.file': '/etc/cortex/overrides.yaml',
    },

  query_frontend_container::
    container.new('query-frontend', $._images.query_frontend) +
    container.withPorts($.util.defaultPorts) +
    container.withArgsMixin($.util.mapToFlags($.query_frontend_args)) +
    $.jaeger_mixin +
    $.util.readinessProbe +
    $.util.resourcesRequests('2', '600Mi') +
    $.util.resourcesLimits(null, '1200Mi'),

  local deployment = $.apps.v1.deployment,

  newQueryFrontendDeployment(name, container)::
    deployment.new(name, $._config.queryFrontend.replicas, [container]) +
    $.util.configVolumeMount($._config.overrides_configmap, '/etc/cortex') +
    (if $._config.cortex_query_frontend_allow_multiple_replicas_on_same_node then {} else $.util.antiAffinity),

  query_frontend_deployment: self.newQueryFrontendDeployment('query-frontend', $.query_frontend_container),

  local service = $.core.v1.service,

  query_frontend_service:
    $.util.serviceFor($.query_frontend_deployment),

  query_frontend_discovery_service:
    $.util.serviceFor($.query_frontend_deployment) +
    // Make sure that query frontend worker, running in the querier, do resolve
    // each query-frontend pod IP and NOT the service IP. To make it, we do NOT
    // use the service cluster IP so that when the service DNS is resolved it
    // returns the set of query-frontend IPs.
    service.mixin.spec.withPublishNotReadyAddresses(true) +
    service.mixin.spec.withClusterIp('None') +
    service.mixin.metadata.withName('query-frontend-discovery'),
}
