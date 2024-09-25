// Migration step 8:
// Deploy ingester-zone-b with correct annotations.
(import 'test-ingest-storage-migration-step-8h.jsonnet') {
  _config+:: {
    ingest_storage_ingester_migration_autoscaling_ingester_annotations_partition_zone_c_enabled: false,
  },
}
