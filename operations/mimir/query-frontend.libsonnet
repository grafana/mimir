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
    } + (
      if $._config.queryFrontend.sharded_queries_enabled then
        {
          'querier.parallelise-shardable-queries': 'true',

          // in process tenant queues on frontends. We divide by the number of frontends; 2 in this case in order to apply the global limit in aggregate.
          // basically base * shard_factor * query_split_factor / num_frontends where
          'querier.max-outstanding-requests-per-tenant': std.floor(200 * $._config.queryFrontend.shard_factor * $._config.queryFrontend.query_split_factor / $._config.queryFrontend.replicas),

          'querier.query-ingesters-within': $._config.queryConfig['querier.query-ingesters-within'],
        } + $._config.storageConfig
      else {}
    ),

  query_frontend_container::
    container.new('query-frontend', $._images.query_frontend) +
    container.withPorts($.util.defaultPorts) +
    container.withArgsMixin($.util.mapToFlags($.query_frontend_args)) +
    $.jaeger_mixin +
    $.util.readinessProbe +
    if $._config.queryFrontend.sharded_queries_enabled then
      $.util.resourcesRequests('2', '2Gi') +
      $.util.resourcesLimits(null, '6Gi') +
      container.withEnvMap({
        JAEGER_REPORTER_MAX_QUEUE_SIZE: '5000',
      })
    else
      $.util.resourcesRequests('2', '600Mi') +
      $.util.resourcesLimits(null, '1200Mi'),

  local deployment = $.apps.v1.deployment,

  query_frontend_deployment:
    deployment.new('query-frontend', $._config.queryFrontend.replicas, [$.query_frontend_container]) +
    $.util.configVolumeMount($._config.overrides_configmap, '/etc/cortex') +
    $.util.antiAffinity +
    // inject storage schema in order to know what/how to shard
    if $._config.queryFrontend.sharded_queries_enabled then
      $.storage_config_mixin
    else {},

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
