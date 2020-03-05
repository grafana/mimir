{
  local container = $.core.v1.container,

  querier_args::
    $._config.ringConfig +
    $._config.storeConfig +
    $._config.storageConfig +
    $._config.queryConfig +
    $._config.distributorConfig +
    {
      target: 'querier',

      // Increase HTTP server response write timeout, as we were seeing some
      // queries that return a lot of data timeing out.
      'server.http-write-timeout': '1m',

      // Limit query concurrency to prevent multi large queries causing an OOM.
      'querier.max-concurrent': $._config.querierConcurrency,

      // Limit to N/2 worker threads per frontend, as we have two frontends.
      'querier.worker-parallelism': $._config.querierConcurrency / 2,
      'querier.frontend-address': 'query-frontend.%(namespace)s.svc.cluster.local:9095' % $._config,
      'querier.frontend-client.grpc-max-send-msg-size': 100 << 20,

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
    $.util.resourcesRequests('1', '12Gi') +
    $.util.resourcesLimits(null, '24Gi') +
    $.jaeger_mixin +
    container.withEnvMap($.querier_env_map),

  local deployment = $.apps.v1beta1.deployment,

  querier_deployment_labels: {},

  querier_deployment:
    deployment.new('querier', 3, [$.querier_container], $.querier_deployment_labels) +
    $.util.antiAffinity +
    $.util.configVolumeMount('overrides', '/etc/cortex') +
    $.storage_config_mixin,

  local service = $.core.v1.service,

  querier_service_ignored_labels:: [],

  querier_service:
    $.util.serviceFor($.querier_deployment, $.querier_service_ignored_labels) +
    service.mixin.spec.withSelector({ name: 'query-frontend' }),
}
