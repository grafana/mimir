// Add replicas in ingester-zone-b-partition.
(import 'test-ingest-storage-migration-step-1a.jsonnet') + {
  _config+:: {
    ingest_storage_ingester_migration_autoscaling_ingester_annotations_partition_zone_b_enabled: true,
  },
}
