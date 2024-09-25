// Migration step 8:
// - Decomission ingester-zone-b-partition.
(import 'test-ingest-storage-migration-step-8f.jsonnet') {
  _config+:: {
    ingest_storage_migration_partition_ingester_zone_b_enabled: false,
  },
}
