// Migration step 2:
// - Migrate store-gateways to multi-zone deployment - Re-deploy store-gateway-zone-b to new AZ.
(import 'test-multi-az-read-path-migration-step-2c.jsonnet') {
  local statefulSet = $.apps.v1.statefulSet,

  _config+:: {
    multi_zone_store_gateway_zone_b_multi_az_enabled: true,

    store_gateway_deletion_protection_enabled: false,

    // Remove the following:
    // store_gateway_automated_downscale_zone_b_enabled: false,

    // default:
    store_gateway_automated_downscale_zone_b_enabled: $._config.store_gateway_automated_downscale_enabled,
  },

  // Remove the following:
  // store_gateway_zone_b_statefulset+:
  //   statefulSet.mixin.spec.withReplicas(0),
  //
  // store_gateway_zone_b_args+:: {
  //   'store-gateway.sharding-ring.auto-forget-enabled': false,
  // },
  store_gateway_zone_b_statefulset+:
    statefulSet.mixin.spec.withReplicas(1),  // set to normal replica count
  store_gateway_zone_b_args+:: {
    'store-gateway.sharding-ring.auto-forget-enabled': null,
  },

  // Keep the following:
  // store_gateway_zone_b_statefulset+:
  //   statefulSet.spec.persistentVolumeClaimRetentionPolicy.withWhenScaled('Retain'),
}
