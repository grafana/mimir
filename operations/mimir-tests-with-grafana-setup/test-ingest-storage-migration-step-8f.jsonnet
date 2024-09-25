// Migration step 8:
// - Remove scaling annotations from ingester-zone-b-partition, and scale it down to 0 replicas.
(import 'test-ingest-storage-migration-step-8e.jsonnet') {
  _config+:: {
    ingest_storage_ingester_migration_autoscaling_ingester_annotations_partition_zone_b_enabled: false,
  },
}
