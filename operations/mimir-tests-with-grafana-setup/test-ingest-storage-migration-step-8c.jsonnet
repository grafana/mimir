// Migration step 8:
// - Decomission ingester-zone-a-partition.
(import 'test-ingest-storage-migration-step-8b.jsonnet') {
  _config+:: {
    ingest_storage_migration_partition_ingester_zone_a_enabled: false,
  },
}
