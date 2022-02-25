{
  local container = $.core.v1.container,

  querier_args::
    $._config.grpcConfig +
    $._config.storageConfig +
    $._config.blocksStorageConfig +
    $._config.queryConfig +
    $._config.queryEngineConfig +
    $._config.ingesterRingClientConfig +
    $._config.queryBlocksStorageConfig +
    $.blocks_metadata_caching_config +
    $.bucket_index_config
    {
      target: 'querier',

      'server.http-listen-port': $._config.server_http_port,

      // Increase HTTP server response write timeout, as we were seeing some
      // queries that return a lot of data timeing out.
      'server.http-write-timeout': '1m',

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

  querier_container::
    container.new('querier', $._images.querier) +
    container.withPorts($.querier_ports) +
    container.withArgsMixin($.util.mapToFlags($.querier_args)) +
    $.jaeger_mixin +
    $.util.readinessProbe +
    container.withEnvMap($.querier_env_map) +
    $.util.resourcesRequests('1', '12Gi') +
    $.util.resourcesLimits(null, '24Gi'),

  local deployment = $.apps.v1.deployment,

  newQuerierDeployment(name, container)::
    deployment.new(name, $._config.querier.replicas, [container]) +
    (if $._config.querier_allow_multiple_replicas_on_same_node then {} else $.util.antiAffinity) +
    $.util.configVolumeMount($._config.overrides_configmap, $._config.overrides_configmap_mountpoint) +
    deployment.mixin.spec.strategy.rollingUpdate.withMaxSurge(5) +
    deployment.mixin.spec.strategy.rollingUpdate.withMaxUnavailable(1),

  querier_deployment:
    self.newQuerierDeployment('querier', $.querier_container),

  local service = $.core.v1.service,

  querier_service:
    $.util.serviceFor($.querier_deployment, $._config.service_ignored_labels),
}
