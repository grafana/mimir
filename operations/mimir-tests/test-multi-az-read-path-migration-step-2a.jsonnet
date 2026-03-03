// Migration step 2:
// - Migrate store-gateways to multi-zone deployment - Deploy store-gateway-zone-a-backup.
(import 'test-multi-az-read-path-migration-step-2.jsonnet') {
  _config+:: {
    store_gateway_automated_downscale_zone_a_backup_enabled: $._config.store_gateway_automated_downscale_enabled,
  },
}
