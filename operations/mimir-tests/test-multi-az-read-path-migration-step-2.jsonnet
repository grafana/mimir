// Migration step 2:
// - Migrate store-gateways to multi-zone deployment.
(import 'test-multi-az-read-path-migration-step-1.jsonnet') {
  _config+:: {
    multi_zone_store_gateway_multi_az_enabled: true,
  },
}
