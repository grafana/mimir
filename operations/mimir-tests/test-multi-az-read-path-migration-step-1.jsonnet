// Migration step 1:
// - Deploy all multi-AZ read path components, alongside the single-AZ ones, but exclude the rulers, store-gateways and ingesters.
(import 'test-multi-az-read-path-migration-step-0.jsonnet') {
  _config+:: {
    single_zone_query_frontend_enabled: true,
    single_zone_query_scheduler_enabled: true,
    single_zone_querier_enabled: true,
    single_zone_memcached_enabled: true,
    single_zone_ruler_remote_evaluation_enabled: true,

    multi_zone_query_frontend_enabled: true,
    multi_zone_query_scheduler_enabled: true,
    multi_zone_querier_enabled: true,
    multi_zone_memcached_enabled: true,
    multi_zone_ruler_remote_evaluation_enabled: true,

    // Do not route requests yet.
    multi_zone_query_frontend_routing_enabled: false,
    multi_zone_ruler_routing_enabled: false,
    multi_zone_memcached_routing_enabled: false,

    // Enable multi-az config for the ingester zone-a to prevent a rollout
    // of zone-a when `multi_zone_ingester_multi_az_enabled` is set to true.
    multi_zone_ingester_zone_a_multi_az_enabled: true,
  },
}
