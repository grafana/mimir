// Migration step 2:
// - Migrate store-gateways to multi-zone deployment - Prepare store-gateway-zone-b scale down.
(import 'test-multi-az-read-path-migration-step-2a.jsonnet') {
  _config+:: {
    // Configure store-gateway-zone-b to not follow zone-a.
    store_gateway_automated_downscale_zone_b_enabled: false,
  },

  local statefulSet = $.apps.v1.statefulSet,

  store_gateway_zone_b_statefulset+:
    statefulSet.spec.persistentVolumeClaimRetentionPolicy.withWhenScaled('Retain') +
    statefulSet.mixin.spec.withReplicas(1),  // Set to the current number of replicas.

  store_gateway_zone_b_args+:: {
    'store-gateway.sharding-ring.auto-forget-enabled': false,
  },

  // Utility used to override a field only if exists in super.
  local overrideSuperIfExists(name, override) = if !( name in super) || super[name] == null || super[name] == {} then null else
    super[name] + override,

  // Configure store-gateway-zone-c to temporarily follow zone-a.
  store_gateway_zone_c_statefulset: overrideSuperIfExists(
    'store_gateway_zone_c_statefulset',
    statefulSet.mixin.metadata.withAnnotationsMixin({
      'grafana.com/rollout-downscale-leader': 'store-gateway-zone-a',
    }),
  ),
}
