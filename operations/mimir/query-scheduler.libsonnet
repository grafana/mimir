// Query-scheduler is optional service. When query-scheduler.libsonnet is added to Mimir, querier and frontend
// are reconfigured to use query-scheduler service.
{
  local container = $.core.v1.container,
  local deployment = $.apps.v1.deployment,
  local service = $.core.v1.service,

  query_scheduler_args+::
    $._config.grpcConfig
    {
      target: 'query-scheduler',
      'server.http-listen-port': $._config.server_http_port,
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

  newQuerySchedulerDeployment(name, container)::
    deployment.new(name, 2, [container]) +
    $.util.configVolumeMount($._config.overrides_configmap, $._config.overrides_configmap_mountpoint) +
    $.util.antiAffinity +
    // Do not run more query-schedulers than expected.
    deployment.mixin.spec.strategy.rollingUpdate.withMaxSurge(0) +
    deployment.mixin.spec.strategy.rollingUpdate.withMaxUnavailable(1),

  query_scheduler_deployment: if !$._config.query_scheduler_enabled then {} else
    self.newQuerySchedulerDeployment('query-scheduler', $.query_scheduler_container),

  query_scheduler_service: if !$._config.query_scheduler_enabled then {} else
    $.util.serviceFor($.query_scheduler_deployment, $._config.service_ignored_labels),

  // Headless to make sure resolution gets IP address of target pods, and not service IP.
  query_scheduler_discovery_service: if !$._config.query_scheduler_enabled then {} else
    $.util.serviceFor($.query_scheduler_deployment, $._config.service_ignored_labels) +
    service.mixin.spec.withPublishNotReadyAddresses(true) +
    service.mixin.spec.withClusterIp('None') +
    service.mixin.metadata.withName('query-scheduler-discovery'),

  // Reconfigure querier and query-frontend to use scheduler.
  querier_args+:: if !$._config.query_scheduler_enabled then {} else {
    'querier.frontend-address': null,
    'querier.scheduler-address': 'query-scheduler-discovery.%(namespace)s.svc.cluster.local:9095' % $._config,
  },

  query_frontend_args+:: if !$._config.query_scheduler_enabled then {} else {
    'query-frontend.scheduler-address': 'query-scheduler-discovery.%(namespace)s.svc.cluster.local:9095' % $._config,
  },
}
