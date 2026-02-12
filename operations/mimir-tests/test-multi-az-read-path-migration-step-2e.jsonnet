// Migration step 2:
// - Migrate store-gateways to multi-zone deployment - Decommission - store-gateway-zone-c
(import 'test-multi-az-read-path-migration-step-2d.jsonnet') {
  local statefulSet = $.apps.v1.statefulSet,

  _config+:: {
    multi_zone_store_gateway_zone_c_enabled: false,
  },

  store_gateway_zone_b_statefulset+: { spec+: { persistentVolumeClaimRetentionPolicy+:: {} } },
}
