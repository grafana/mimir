// Migration step 8:
// - Decommission remaining single-zone deployments.
(import 'test-multi-az-read-path-migration-step-7.jsonnet') {
  _config+:: {
    single_zone_query_frontend_enabled: false,
    single_zone_query_scheduler_enabled: false,
    single_zone_querier_enabled: false,
    single_zone_memcached_enabled: false,
    single_zone_ruler_remote_evaluation_enabled: false,
  },
}
