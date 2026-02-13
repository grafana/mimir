// Migration step 2:
// - Migrate store-gateways to multi-zone deployment - Deploy store-gateway-zone-b-backup.
(import 'test-multi-az-read-path-migration-step-2e.jsonnet') {
  _config+:: {
    multi_zone_store_gateway_multi_az_enabled: true,
    store_gateway_automated_downscale_zone_b_backup_enabled: $._config.store_gateway_automated_downscale_enabled,
    multi_zone_store_gateway_zone_a_backup_multi_az_enabled: $._config.multi_zone_store_gateway_multi_az_enabled,
    multi_zone_store_gateway_zone_b_backup_multi_az_enabled: $._config.multi_zone_store_gateway_multi_az_enabled,
    multi_zone_store_gateway_zone_b_multi_az_enabled: $._config.multi_zone_store_gateway_multi_az_enabled,
    store_gateway_deletion_protection_enabled: false,  // default.
  },

  store_gateway_rollout_pdb+:
    local podDisruptionBudget = $.policy.v1.podDisruptionBudget;
    podDisruptionBudget.mixin.spec.withMaxUnavailable($._config.multi_zone_store_gateway_max_unavailable),
}
