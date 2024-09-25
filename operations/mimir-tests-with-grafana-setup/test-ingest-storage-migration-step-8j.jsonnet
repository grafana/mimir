// Migration step 8:
// Deploy ingester-zone-b with correct annotations.
(import 'test-ingest-storage-migration-step-8i.jsonnet') {
  _config+:: {
    ingest_storage_migration_partition_ingester_zone_c_enabled: false,

    // We're not going to deploy ingester-zone-c.
    ingest_storage_ingester_zones: 2,
  },
}
