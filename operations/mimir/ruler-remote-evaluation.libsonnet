// Deployment of a dedicated query path for ruler remote evaluation.
{
  _config+:: {
    ruler_remote_evaluation_enabled: false,
    ruler_remote_evaluation_migration_enabled: false,

    // Note: There is no option to disable ruler-query-scheduler.
  },

  local rulerQuerySchedulerName = 'ruler-query-scheduler',
  local useRulerQueryFrontend = $._config.ruler_remote_evaluation_enabled && !$._config.ruler_remote_evaluation_migration_enabled,

  ruler_args+:: if !useRulerQueryFrontend then {} else {
    'ruler.query-frontend.address': 'dns:///ruler-query-frontend.%(namespace)s.svc.cluster.local:9095' % $._config,
  },

  local container = $.core.v1.container,
  local deployment = $.apps.v1.deployment,
  local service = $.core.v1.service,

  local queryFrontendDisableCacheArgs =
    {
      // Query cache is of no benefit to rule evaluation.
      'query-frontend.cache-results': false,
      'query-frontend.results-cache.backend': null,
      'query-frontend.results-cache.memcached.addresses': null,
      'query-frontend.results-cache.memcached.timeout': null,
    },

  //
  // Querier
  //

  ruler_querier_args+::
    $.querier_args +
    $.querierUseQuerySchedulerArgs(rulerQuerySchedulerName),

  ruler_querier_container::
    $.newQuerierContainer('ruler-querier', $.ruler_querier_args),

  ruler_querier_deployment: if !$._config.ruler_remote_evaluation_enabled then {} else
    $.newQuerierDeployment('ruler-querier', $.ruler_querier_container),

  ruler_querier_service: if !$._config.ruler_remote_evaluation_enabled then {} else
    $.util.serviceFor($.ruler_querier_deployment, $._config.service_ignored_labels),

  //
  // Query Frontend
  //

  ruler_query_frontend_args+::
    // The ruler remote evaluation connects to ruler-query-frontend via gRPC.
    $._config.grpcIngressConfig +
    $.query_frontend_args +
    $.queryFrontendUseQuerySchedulerArgs(rulerQuerySchedulerName) +
    queryFrontendDisableCacheArgs,

  ruler_query_frontend_container::
    $.newQueryFrontendContainer('ruler-query-frontend', $.ruler_query_frontend_args),

  ruler_query_frontend_deployment: if !$._config.ruler_remote_evaluation_enabled then {} else
    $.newQueryFrontendDeployment('ruler-query-frontend', $.ruler_query_frontend_container),

  ruler_query_frontend_service: if !$._config.ruler_remote_evaluation_enabled then {} else
    $.util.serviceFor($.ruler_query_frontend_deployment, $._config.service_ignored_labels) +
    // Note: We use a headless service because the ruler uses gRPC load balancing.
    service.mixin.spec.withClusterIp('None'),

  //
  // Query Scheduler
  //

  ruler_query_scheduler_args+::
    $.query_scheduler_args +
    (
      // If the ruler-query-schedulers form a ring then they need to build a different
      // ring than the standard query-schedulers.
      if $._config.query_scheduler_service_discovery_mode != 'ring' then {} else {
        'query-scheduler.ring.prefix': '%s/' % rulerQuerySchedulerName,
      }
    ),

  ruler_query_scheduler_container::
    $.newQuerySchedulerContainer(rulerQuerySchedulerName, $.ruler_query_scheduler_args),

  ruler_query_scheduler_deployment: if !$._config.ruler_remote_evaluation_enabled then {} else
    $.newQuerySchedulerDeployment(rulerQuerySchedulerName, $.ruler_query_scheduler_container),

  ruler_query_scheduler_service: if !$._config.ruler_remote_evaluation_enabled then {} else
    $.util.serviceFor($.ruler_query_scheduler_deployment, $._config.service_ignored_labels),

  ruler_query_scheduler_discovery_service: if !$._config.ruler_remote_evaluation_enabled then {} else
    $.newQuerySchedulerDiscoveryService(rulerQuerySchedulerName, $.ruler_query_scheduler_deployment),
}
