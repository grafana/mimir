// Based on test-multi-az-read-path.jsonnet.
// Tests migration scenario where both single-zone and multi-zone read path components are deployed.
(import 'test-multi-az-read-path.jsonnet') {
  _config+:: {
    single_zone_memcached_enabled: true,
    single_zone_querier_enabled: true,
    single_zone_query_frontend_enabled: true,
    single_zone_query_scheduler_enabled: true,
    single_zone_ruler_enabled: true,
    single_zone_ruler_remote_evaluation_enabled: true,
  },
}
