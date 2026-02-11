// At this point the migration has been completed. We just want to make sure there's
// no diff between the previous step and this step. This step builds on the initial
// setup and shows the full config changes expected at the end of the migration.
(import 'test-multi-az-read-path-migration-step-0.jsonnet') {
  _config+:: {
    // Remove the following:
    // multi_zone_store_gateway_multi_az_enabled: true,

    // default:
    multi_zone_store_gateway_multi_az_enabled: $._config.multi_zone_read_path_multi_az_enabled,

    // Final config:
    multi_zone_read_path_enabled: true,
    multi_zone_read_path_multi_az_enabled: true,

    // TODO: These are required to ensure no-diff between step-0 plus this file = step-8.
    multi_zone_store_gateway_zone_c_enabled: false,  // not required at end.
    multi_zone_store_gateway_backup_zones_enabled: true,  // defaults to false.
    store_gateway_replication_factor: 4,  // defaults to 3.
  },
}
