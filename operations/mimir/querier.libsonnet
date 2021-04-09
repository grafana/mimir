{
  local container = $.core.v1.container,

  querier_args::
    $._config.grpcConfig +
    $._config.ringConfig +
    $._config.storeConfig +
    $._config.storageConfig +
    $._config.blocksStorageConfig +
    $._config.queryConfig +
    $._config.distributorConfig +
    {
      target: 'querier',

      // Increase HTTP server response write timeout, as we were seeing some
      // queries that return a lot of data timeing out.
      'server.http-write-timeout': '1m',

      // Limit query concurrency to prevent multi large queries causing an OOM.
      'querier.max-concurrent': $._config.querier.concurrency,

      // Limit to N/2 worker threads per frontend, as we have two frontends.
      'querier.worker-parallelism': $._config.querier.concurrency / $._config.queryFrontend.replicas,
      'querier.frontend-address': 'query-frontend-discovery.%(namespace)s.svc.cluster.local:9095' % $._config,
      'querier.frontend-client.grpc-max-send-msg-size': 100 << 20,

      'querier.second-store-engine': $._config.querier_second_storage_engine,

      'log.level': 'debug',
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
    if $._config.queryFrontend.sharded_queries_enabled then
      $.util.resourcesRequests('3', '12Gi') +
      $.util.resourcesLimits(null, '24Gi')
    else
      $.util.resourcesRequests('1', '12Gi') +
      $.util.resourcesLimits(null, '24Gi'),

  local deployment = $.apps.v1.deployment,

  querier_deployment_labels: {},

  querier_deployment:
    deployment.new('querier', $._config.querier.replicas, [$.querier_container], $.querier_deployment_labels) +
    $.util.antiAffinity +
    $.util.configVolumeMount($._config.overrides_configmap, '/etc/cortex') +
    $.storage_config_mixin,

  local service = $.core.v1.service,

  querier_service_ignored_labels:: [],

  querier_service:
    $.util.serviceFor($.querier_deployment, $.querier_service_ignored_labels),
}
