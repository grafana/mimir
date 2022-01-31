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

      // Split long queries up into multiple day-long queries.
      'frontend.split-queries-by-interval': '24h',

      // Cache query results.
      'frontend.align-querier-with-step': false,
      'frontend.cache-results': true,
      'frontend.results-cache.backend': 'memcached',
      'frontend.results-cache.memcached.addresses': 'dnssrvnoa+memcached-frontend.%(namespace)s.svc.cluster.local:11211' % $._config,
      'frontend.results-cache.memcached.timeout': '500ms',

      // So that exporters like cloudwatch can still send in data and be un-cached.
      'frontend.max-cache-freshness': '10m',

      // Use GZIP compression for API responses; improves latency for very big results and slow
      // connections.
      'api.response-compression-enabled': true,

      // So it can receive big responses from the querier.
      'server.grpc-max-recv-msg-size-bytes': 100 << 20,

      // Limit queries to 500 days, allow this to be override per-user.
      'store.max-query-length': '12000h',  // 500 Days
      'runtime-config.file': '%s/overrides.yaml' % $._config.overrides_configmap_mountpoint,
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

  query_frontend_deployment_labels:: {
    'app.kubernetes.io/component': 'query-frontend',
    'app.kubernetes.io/part-of': $._config.kubernetes_part_of,
  },

  query_frontend_service_ignored_labels:: ['app.kubernetes.io/component', 'app.kubernetes.io/part-of'],

  newQueryFrontendDeployment(name, container)::
    deployment.new(name, $._config.queryFrontend.replicas, [container]) +
    $.util.configVolumeMount($._config.overrides_configmap, $._config.overrides_configmap_mountpoint) +
    (if $._config.query_frontend_allow_multiple_replicas_on_same_node then {} else $.util.antiAffinity) +
    deployment.mixin.spec.strategy.rollingUpdate.withMaxSurge(1) +
    deployment.mixin.spec.strategy.rollingUpdate.withMaxUnavailable(1) +
    deployment.spec.template.metadata.withLabelsMixin($.query_frontend_deployment_labels),

  query_frontend_deployment: self.newQueryFrontendDeployment('query-frontend', $.query_frontend_container),

  local service = $.core.v1.service,

  query_frontend_service:
    $.util.serviceFor($.query_frontend_deployment, $.query_frontend_service_ignored_labels),

  query_frontend_discovery_service:
    $.util.serviceFor($.query_frontend_deployment, $.query_frontend_service_ignored_labels) +
    // Make sure that query frontend worker, running in the querier, do resolve
    // each query-frontend pod IP and NOT the service IP. To make it, we do NOT
    // use the service cluster IP so that when the service DNS is resolved it
    // returns the set of query-frontend IPs.
    service.mixin.spec.withPublishNotReadyAddresses(true) +
    service.mixin.spec.withClusterIp('None') +
    service.mixin.metadata.withName('query-frontend-discovery'),
}
