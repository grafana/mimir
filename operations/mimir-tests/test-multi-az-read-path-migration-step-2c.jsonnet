// Migration step 2:
// - Migrate store-gateways to multi-zone deployment - Scale down store-gateway-zone-b to zero
(import 'test-multi-az-read-path-migration-step-2b.jsonnet') {
  store_gateway_zone_b_statefulset+:
    local statefulSet = $.apps.v1.statefulSet;
    statefulSet.mixin.spec.withReplicas(0),
}
