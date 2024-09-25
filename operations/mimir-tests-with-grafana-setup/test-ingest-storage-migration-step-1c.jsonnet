// Add replicas in ingester-zone-c-partition.
(import 'test-ingest-storage-migration-step-1b.jsonnet') + {
  _config+:: {
    ingest_storage_ingester_migration_autoscaling_ingester_annotations_partition_zone_c_enabled: true,
  },
}
