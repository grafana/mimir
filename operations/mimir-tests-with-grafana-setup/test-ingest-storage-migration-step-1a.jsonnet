// Add replicas in ingester-zone-X-partitions. They will be scaled from existing ReplicaTemplate/ingester-zone-a.
//
// Note: this imports new file, to support this migration.
(import 'test-ingest-storage-migration-step-1.jsonnet') + (import 'ingest-store-migration-autoscaling.libsonnet') + {
  _config+:: {
    // Enable scaling of partition-ingesters in zone-a, using existing ReplicaTemplate used for ingester_automated_downscale_v2.
    // This replica template can also be controlled by HPA.
    ingest_storage_ingester_migration_autoscaling_ingester_annotations_partition_zone_a_enabled: true,
  },
}
