// Deployment of a dedicated query path for ruler remote evaluation.
{
  _config+:: {
    ruler_remote_evaluation_enabled: false,
    ruler_remote_evaluation_migration_enabled: false,

    // The maximum size (in bytes) supported for a rule evaluation query response executed through
    // the ruler's dedicated read-path.
    ruler_remote_evaluation_max_query_response_size_bytes: 100 * 1024 * 1024,

    // Note: There is no option to disable ruler-query-scheduler.
  },

  local rulerQuerySchedulerName = 'ruler-query-scheduler',
  local useRulerQueryFrontend = $._config.ruler_remote_evaluation_enabled && !$._config.ruler_remote_evaluation_migration_enabled,

  ruler_args+:: if !useRulerQueryFrontend then {} else {
    'ruler.query-frontend.address': 'dns:///ruler-query-frontend.%(namespace)s.svc.%(cluster_domain)s:9095' % $._config,

    // The ruler send a query request to the ruler-query-frontend.
    'ruler.query-frontend.grpc-client-config.grpc-max-recv-msg-size': $._config.ruler_remote_evaluation_max_query_response_size_bytes,
  },

  local container = $.core.v1.container,
  local deployment = $.apps.v1.deployment,
  local service = $.core.v1.service,

  //
  // Querier
  //

  ruler_querier_args+::
    $.querier_args +
    $.querierUseQuerySchedulerArgs(rulerQuerySchedulerName) + {
      'querier.max-concurrent': $._config.ruler_querier_max_concurrency,
    } + if !useRulerQueryFrontend then {} else {
      // The ruler-querier sends a query response back to the ruler-query-frontend
      'querier.frontend-client.grpc-max-send-msg-size': $._config.ruler_remote_evaluation_max_query_response_size_bytes,
    },

  ruler_querier_env_map:: $.querier_env_map {
    // Do not dynamically set GOMAXPROCS for ruler-querier. We don't expect ruler-querier resources
    // utilization to be spiky, and we want to reduce the risk rule evaluations are getting delayed.
    GOMAXPROCS: null,
  },

  ruler_querier_node_affinity_matchers:: $.querier_node_affinity_matchers,

  ruler_querier_container::
    $.newQuerierContainer('ruler-querier', $.ruler_querier_args, $.ruler_querier_env_map),

  ruler_querier_deployment: if !$._config.ruler_remote_evaluation_enabled then {} else
    $.newQuerierDeployment('ruler-querier', $.ruler_querier_container, $.ruler_querier_node_affinity_matchers),

  ruler_querier_service: if !$._config.ruler_remote_evaluation_enabled then {} else
    $.util.serviceFor($.ruler_querier_deployment, $._config.service_ignored_labels),

  ruler_querier_pdb: if !$._config.ruler_remote_evaluation_enabled then null else
    $.newMimirPdb('ruler-querier'),

  //
  // Query Frontend
  //

  ruler_query_frontend_args+::
    // The ruler remote evaluation connects to ruler-query-frontend via gRPC.
    $._config.grpcIngressConfig +
    $.query_frontend_args +
    $.queryFrontendUseQuerySchedulerArgs(rulerQuerySchedulerName) +
    {
      // Result caching is of no benefit to rule evaluation, but the cache can be used for storing cardinality estimates.
      'query-frontend.cache-results': false,

      // The ruler-query-frontend receives the query response back from the ruler-querier.
      'server.grpc-max-recv-msg-size-bytes': $._config.ruler_remote_evaluation_max_query_response_size_bytes,
      // The ruler-query-frontend sends the query response back to the ruler.
      'server.grpc-max-send-msg-size-bytes': $._config.ruler_remote_evaluation_max_query_response_size_bytes,
    },

  ruler_query_frontend_env_map:: $.query_frontend_env_map,

  ruler_query_frontend_node_affinity_matchers:: $.query_frontend_node_affinity_matchers,

  ruler_query_frontend_container::
    $.newQueryFrontendContainer('ruler-query-frontend', $.ruler_query_frontend_args, $.ruler_query_frontend_env_map),

  ruler_query_frontend_deployment: if !$._config.ruler_remote_evaluation_enabled then {} else
    $.newQueryFrontendDeployment('ruler-query-frontend', $.ruler_query_frontend_container, $.ruler_query_frontend_node_affinity_matchers),

  ruler_query_frontend_service: if !$._config.ruler_remote_evaluation_enabled then {} else
    $.util.serviceFor($.ruler_query_frontend_deployment, $._config.service_ignored_labels) +
    // Note: We use a headless service because the ruler uses gRPC load balancing.
    service.mixin.spec.withClusterIp('None'),

  ruler_query_frontend_pdb: if !$._config.ruler_remote_evaluation_enabled then null else
    $.newMimirPdb('ruler-query-frontend'),

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

  ruler_query_scheduler_env_map:: $.query_scheduler_env_map,

  ruler_query_scheduler_node_affinity_matchers:: $.query_scheduler_node_affinity_matchers,

  ruler_query_scheduler_container::
    $.newQuerySchedulerContainer(rulerQuerySchedulerName, $.ruler_query_scheduler_args, $.ruler_query_scheduler_env_map),

  ruler_query_scheduler_deployment: if !$._config.ruler_remote_evaluation_enabled then {} else
    $.newQuerySchedulerDeployment(rulerQuerySchedulerName, $.ruler_query_scheduler_container, $.ruler_query_scheduler_node_affinity_matchers),

  ruler_query_scheduler_service: if !$._config.ruler_remote_evaluation_enabled then {} else
    $.util.serviceFor($.ruler_query_scheduler_deployment, $._config.service_ignored_labels),

  ruler_query_scheduler_discovery_service: if !$._config.ruler_remote_evaluation_enabled then {} else
    $.newQuerySchedulerDiscoveryService(rulerQuerySchedulerName, $.ruler_query_scheduler_deployment),

  ruler_query_scheduler_pdb: if !$._config.ruler_remote_evaluation_enabled then null else
    $.newMimirPdb('ruler-query-scheduler'),
}
