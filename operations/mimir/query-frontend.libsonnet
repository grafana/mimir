{
  local container = $.core.v1.container,
  local deployment = $.apps.v1beta1.deployment,
  local service = $.core.v1.service,

  query_frontend_params:: {
    replicas: 2, // number of frontends to run
    shard_factor: 16,  // v10 schema shard factor

    // Queries can technically be sharded an arbitrary number of times. Thus query_split_factor is used
    // as a coefficient to multiply the frontend tenant queues by. The idea is that this
    // yields a bit of headroom so tenant queues aren't underprovisioned. Therefore the split factor
    // should be represent the highest reasonable split factor for a query. If too low, a long query
    // (i.e. 30d) with a high split factor (i.e. 5) would result in
    // (day_splits * shard_factor * split_factor) or 30 * 16 * 5 = 2400 sharded queries, which may be
    // more than the max queue size and thus would always error.
    query_split_factor:: 3,
  },

  query_frontend_args:: {
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
    'frontend.memcached.consistent-hash': true,

    // So that exporters like cloudwatch can still send in data and be un-cached.
    'frontend.max-cache-freshness': '10m',

    // Compress HTTP responses; improves latency for very big results and slow
    // connections.
    'querier.compress-http-responses': true,

    // So it can recieve big responses from the querier.
    'server.grpc-max-recv-msg-size-bytes': 100 << 20,

    // Limit queries to 500 days, allow this to be override per-user.
    'store.max-query-length': '12000h',  // 500 Days
    'limits.per-user-override-config': '/etc/cortex/overrides.yaml',
  } + if $._config.sharded_queries_enabled then {
    'querier.parallelise-shardable-queries': 'true',

    // in process tenant queues on frontends. We divide by the number of frontends; 2 in this case in order to apply the global limit in aggregate.
    // basically base * shard_factor * query_split_factor / num_frontends where
    'querier.max-outstanding-requests-per-tenant': std.floor(200 * $.query_frontend_params.shard_factor * $.query_frontend_params.query_split_factor / $.query_frontend_params.replicas),

    'querier.query-ingesters-within': $._config.queryConfig['querier.query-ingesters-within'],
  } + $._config.storageConfig
  else {},

  query_frontend_container::
    container.new('query-frontend', $._images.query_frontend) +
    container.withPorts($.util.defaultPorts) +
    container.withArgsMixin($.util.mapToFlags($.query_frontend_args)) +
    $.jaeger_mixin +
    if $._config.sharded_queries_enabled then
    $.util.resourcesRequests('2', '2Gi') +
    $.util.resourcesLimits(null, '6Gi') +
    container.withEnvMap({
      JAEGER_REPORTER_MAX_QUEUE_SIZE: '5000',
    })
    else $.util.resourcesRequests('2', '600Mi') +
    $.util.resourcesLimits(null, '1200Mi'),

  query_frontend_deployment:
    deployment.new('query-frontend', self.query_frontend_params.replicas, [$.query_frontend_container]) +
    $.util.configVolumeMount('overrides', '/etc/cortex') +
    $.util.antiAffinity,

  query_frontend_service:
    $.util.serviceFor($.query_frontend_deployment) +
    // Make sure that query frontend worker, running in the querier, do resolve
    // each query-frontend pod IP and NOT the service IP. To make it, we do NOT
    // use the service cluster IP so that when the service DNS is resolved it
    // returns the set of query-frontend IPs.
    service.mixin.spec.withClusterIp('None'),
}
