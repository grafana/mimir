// Migration step 10:
// - Deploy ingesters autoscaling HPA and ReplicaTemplates.
(import 'test-ingest-storage-migration-step-8j.jsonnet') {
  _config+:: {
    // Already set previously.
    ingest_storage_ingester_migration_autoscaling_ingester_annotations_partition_zone_a_enabled: false,
    ingest_storage_ingester_migration_autoscaling_ingester_annotations_partition_zone_b_enabled: false,
    ingest_storage_ingester_migration_autoscaling_ingester_annotations_partition_zone_c_enabled: false,

    // Disable setting of annotations via ingest-store-migration-autoscaling.libsonnet.
    ingest_storage_ingester_migration_autoscaling_ingester_annotations_ingester_zone_a_enabled: false,
    ingest_storage_ingester_migration_autoscaling_ingester_annotations_ingester_zone_b_enabled: false,
    ingest_storage_ingester_migration_autoscaling_ingester_annotations_ingester_zone_c_enabled: false,

    // Don't remove downscaling annotations from ingester-zone-[ab], and don't scale them down.
    ingest_storage_ingester_migration_classic_ingesters_remove_downscaling_annotations: false,
    ingest_storage_ingester_migration_classic_ingesters_scale_down: false,

    // Disable other ingester scaling features -- all replaced with ingest-storage autoscaling.
    multi_zone_ingester_replicas: 0,
    new_ingester_hpa_enabled: false,
    ingester_automated_downscale_enabled: false,
    ingester_automated_downscale_v2_enabled: false,

    // The following config is not required anymore.
    ingest_storage_migration_classic_ingesters_no_scale_down_delay: false,

    // Enable ingest-storage autoscaling, and configure minimum replicas to match current value.
    ingest_storage_ingester_autoscaling_enabled: true,
    ingest_storage_ingester_autoscaling_min_replicas_per_zone: 10,
    ingest_storage_ingester_autoscaling_max_replicas_per_zone: 20,
    // Use same label-selector as ReplicaTemplate in ingester_automated_downscale_v2_enabled.
    ingest_storage_replica_template_label_selector: 'name=ingester-zone-a',
  },
}
