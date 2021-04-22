// Query-scheduler is optional service. When query-scheduler.libsonnet is added to Cortex, querier and frontend
// are reconfigured to use query-scheduler service.
{
  local container = $.core.v1.container,
  local deployment = $.apps.v1.deployment,
  local service = $.core.v1.service,

  query_scheduler_args+::
    $._config.grpcConfig
    {
      target: 'query-scheduler',
      'log.level': 'debug',
      'query-scheduler.max-outstanding-requests-per-tenant': 100,
    },

  query_scheduler_container::
    container.new('query-scheduler', $._images.query_scheduler) +
    container.withPorts($.util.defaultPorts) +
    container.withArgsMixin($.util.mapToFlags($.query_scheduler_args)) +
    $.jaeger_mixin +
    $.util.readinessProbe +
    $.util.resourcesRequests('2', '1Gi') +
    $.util.resourcesLimits(null, '2Gi'),


  query_scheduler_deployment: if !$._config.query_scheduler_enabled then {} else
    deployment.new('query-scheduler', 2, [$.query_scheduler_container]) +
    $.util.configVolumeMount('overrides', '/etc/cortex') +
    $.util.antiAffinity,

  query_scheduler_service: if !$._config.query_scheduler_enabled then {} else
    $.util.serviceFor($.query_scheduler_deployment),

  // Headless to make sure resolution gets IP address of target pods, and not service IP.
  query_scheduler_discovery_service: if !$._config.query_scheduler_enabled then {} else
    $.util.serviceFor($.query_scheduler_deployment) +
    service.mixin.spec.withPublishNotReadyAddresses(true) +
    service.mixin.spec.withClusterIp('None') +
    service.mixin.metadata.withName('query-scheduler-discovery'),

  // Reconfigure querier and query-frontend to use scheduler.
  querier_args+:: if !$._config.query_scheduler_enabled then {} else {
    'querier.worker-match-max-concurrent': 'true',
    'querier.worker-parallelism': null,  // Disabled since we set worker-match-max-concurrent.
    'querier.frontend-address': null,
    'querier.scheduler-address': 'query-scheduler-discovery.%(namespace)s.svc.cluster.local:9095' % $._config,
  },

  query_frontend_args+:: if !$._config.query_scheduler_enabled then {} else {
    'frontend.scheduler-address': 'query-scheduler-discovery.%(namespace)s.svc.cluster.local:9095' % $._config,
  },
}
