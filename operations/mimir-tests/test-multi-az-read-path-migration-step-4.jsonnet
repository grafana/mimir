// Migration step 4:
// - Route traffic to multi-zone deployments, excluding ruler (it has not been deployed to multi-zone yet).
(import 'test-multi-az-read-path-migration-step-3.jsonnet') {
  _config+:: {
    multi_zone_query_frontend_routing_enabled: true,
    multi_zone_memcached_routing_enabled: true,
  },
}
