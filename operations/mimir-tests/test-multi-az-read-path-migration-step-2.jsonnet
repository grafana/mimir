// Migration step 2:
// - Migrate store-gateways to multi-zone deployment.
(import 'test-multi-az-read-path-migration-step-1.jsonnet') {
  _config+:: {
    // Prepare for RF 4
    store_gateway_replication_factor: 4,

    multi_zone_store_gateway_backup_zones_enabled: true,
    store_gateway_automated_downscale_zone_a_backup_enabled: false,
    store_gateway_automated_downscale_zone_b_backup_enabled: false,

    multi_zone_store_gateway_zone_a_backup_multi_az_enabled: true,
    multi_zone_store_gateway_zone_b_backup_multi_az_enabled: true,
  },

  store_gateway_rollout_pdb+:
    local podDisruptionBudget = $.policy.v1.podDisruptionBudget;
    podDisruptionBudget.mixin.spec.withMaxUnavailable(0),
}
