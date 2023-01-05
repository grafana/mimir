{
  local container = $.core.v1.container,

  querier_args::
    $._config.usageStatsConfig +
    $._config.grpcConfig +
    $._config.storageConfig +
    $._config.blocksStorageConfig +
    $._config.queryConfig +
    $._config.queryEngineConfig +
    $._config.ingesterRingClientConfig +
    $._config.queryBlocksStorageConfig +
    $._config.querySchedulerRingClientConfig +
    $.blocks_metadata_caching_config +
    $.bucket_index_config
    {
      target: 'querier',

      'server.http-listen-port': $._config.server_http_port,

      // Limit query concurrency to prevent multi large queries causing an OOM.
      'querier.max-concurrent': $._config.querier.concurrency,

      'querier.frontend-address': 'query-frontend-discovery.%(namespace)s.svc.cluster.local:9095' % $._config,
      'querier.frontend-client.grpc-max-send-msg-size': 100 << 20,

      // We request high memory but the Go heap is typically very low (< 100MB) and this causes
      // the GC to trigger continuously. Setting a ballast of 256MB reduces GC.
      'mem-ballast-size-bytes': 1 << 28,  // 256M
    },

  querier_ports:: $.util.defaultPorts,

  querier_env_map:: {
    JAEGER_REPORTER_MAX_QUEUE_SIZE: '1024',  // Default is 100.
  },

  newQuerierContainer(name, args)::
    container.new(name, $._images.querier) +
    container.withPorts($.querier_ports) +
    container.withArgsMixin($.util.mapToFlags(args)) +
    $.jaeger_mixin +
    $.util.readinessProbe +
    container.withEnvMap($.querier_env_map) +
    $.util.resourcesRequests('1', '12Gi') +
    $.util.resourcesLimits(null, '24Gi'),

  querier_container::
    self.newQuerierContainer('querier', $.querier_args),

  local deployment = $.apps.v1.deployment,

  newQuerierDeployment(name, container)::
    deployment.new(name, $._config.querier.replicas, [container]) +
    $.newMimirSpreadTopology(name, $._config.querier_topology_spread_max_skew) +
    $.mimirVolumeMounts +
    (if !std.isObject($._config.node_selector) then {} else deployment.mixin.spec.template.spec.withNodeSelectorMixin($._config.node_selector)) +
    deployment.mixin.spec.strategy.rollingUpdate.withMaxSurge(5) +
    deployment.mixin.spec.strategy.rollingUpdate.withMaxUnavailable(1),

  querier_deployment: if !$._config.is_microservices_deployment_mode then null else
    self.newQuerierDeployment('querier', $.querier_container),

  local service = $.core.v1.service,

  querier_service: if !$._config.is_microservices_deployment_mode then null else
    $.util.serviceFor($.querier_deployment, $._config.service_ignored_labels),
}
