// Migration step 2:
// - Migrate store-gateways to multi-zone deployment - Decommission - store-gateway-zone-c
(import 'test-multi-az-read-path-migration-step-2e.jsonnet') {
  local statefulSet = $.apps.v1.statefulSet,

  _config+:: {
    multi_zone_store_gateway_zone_c_enabled: false,
  },

  // Remove the following:
  // store_gateway_zone_b_statefulset+:
  //   statefulSet.spec.persistentVolumeClaimRetentionPolicy.withWhenScaled('Retain'),
  //
  // store_gateway_zone_c_statefulset+:
  //   statefulSet.mixin.metadata.withAnnotationsMixin({
  //     'grafana.com/rollout-downscale-leader': 'store-gateway-zone-a',
  //   }),

  store_gateway_zone_b_statefulset+: { spec+: { persistentVolumeClaimRetentionPolicy+:: {} } },
  // store_gateway_zone_c_statefulset will be removed by setting ..._enabled: false above.
}
