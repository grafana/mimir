// Migration step 8:
// - switch primary zone to ingester-zone-a.
(import 'test-ingest-storage-migration-step-8d.jsonnet') {
  _config+:: {
    ingest_storage_ingester_migration_autoscaling_ingester_primary_zone: 'ingester-zone-a',
  },
}
