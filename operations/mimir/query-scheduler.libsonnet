// Query-scheduler is optional service. When query-scheduler.libsonnet is added to Mimir, querier and frontend
// are reconfigured to use query-scheduler service.
{
  local container = $.core.v1.container,
  local deployment = $.apps.v1.deployment,

  query_scheduler_args+::
    $._config.grpcConfig
    {
      target: 'query-scheduler',
      'server.http-listen-port': $._config.server_http_port,
      'query-scheduler.max-outstanding-requests-per-tenant': 100,
    },

  newQuerySchedulerContainer(name, args)::
    container.new(name, $._images.query_scheduler) +
    container.withPorts($.util.defaultPorts) +
    container.withArgsMixin($.util.mapToFlags(args)) +
    $.jaeger_mixin +
    $.util.readinessProbe +
    $.util.resourcesRequests('2', '1Gi') +
    $.util.resourcesLimits(null, '2Gi'),

  query_scheduler_container::
    self.newQuerySchedulerContainer('query-scheduler', $.query_scheduler_args),

  newQuerySchedulerDeployment(name, container)::
    deployment.new(name, 2, [container]) +
    $.mimirVolumeMounts +
    (if !std.isObject($._config.node_selector) then {} else deployment.mixin.spec.template.spec.withNodeSelectorMixin($._config.node_selector)) +
    $.util.antiAffinity +
    // Do not run more query-schedulers than expected.
    deployment.mixin.spec.strategy.rollingUpdate.withMaxSurge(0) +
    deployment.mixin.spec.strategy.rollingUpdate.withMaxUnavailable(1),

  query_scheduler_deployment: if !$._config.query_scheduler_enabled then {} else
    self.newQuerySchedulerDeployment('query-scheduler', $.query_scheduler_container),

  query_scheduler_service: if !$._config.query_scheduler_enabled then {} else
    $.util.serviceFor($.query_scheduler_deployment, $._config.service_ignored_labels),

  local discoveryServiceName(prefix) = '%s-discovery' % prefix,

  // Headless to make sure resolution gets IP address of target pods, and not service IP.
  newQuerySchedulerDiscoveryService(name, deployment)::
    $.newMimirDiscoveryService(discoveryServiceName(name), deployment),

  query_scheduler_discovery_service: if !$._config.query_scheduler_enabled then {} else
    self.newQuerySchedulerDiscoveryService('query-scheduler', $.query_scheduler_deployment),

  // Reconfigure querier and query-frontend to use scheduler.

  local querySchedulerAddress(name) =
    '%s.%s.svc.cluster.local:9095' % [discoveryServiceName(name), $._config.namespace],

  querierUseQuerySchedulerArgs(name):: {
    'querier.frontend-address': null,
    'querier.scheduler-address': querySchedulerAddress(name),
  },

  queryFrontendUseQuerySchedulerArgs(name):: {
    'query-frontend.scheduler-address': querySchedulerAddress(name),
  },

  querier_args+:: if !$._config.query_scheduler_enabled then {} else
    self.querierUseQuerySchedulerArgs('query-scheduler'),

  query_frontend_args+:: if !$._config.query_scheduler_enabled then {} else
    self.queryFrontendUseQuerySchedulerArgs('query-scheduler'),
}
