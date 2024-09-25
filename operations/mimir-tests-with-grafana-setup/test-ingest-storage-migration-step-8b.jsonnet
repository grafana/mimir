// Migration step 8:
// Scale down ingester-zone-a-partition.
(import 'test-ingest-storage-migration-step-8a.jsonnet') {
  _config+:: {
    ingest_storage_ingester_migration_autoscaling_ingester_annotations_partition_zone_a_enabled: false,
  },
}
