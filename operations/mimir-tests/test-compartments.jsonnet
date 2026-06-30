// Renders Mimir with the experimental compartments architecture enabled.
// Based on test-ingest-storage-autoscaling-one-trigger.jsonnet.
(import 'test-ingest-storage-autoscaling-one-trigger.jsonnet') {
  _config+:: {
    multi_zone_availability_zones: ['us-east-2a', 'us-east-2b'],
    ingest_storage_ingester_zones: 2,

    // Multi-AZ write path.
    multi_zone_ingester_multi_az_enabled: true,
    multi_zone_write_path_enabled: true,

    // Exercise the per-compartment distributor scaled objects.
    autoscaling_distributor_enabled: true,
    autoscaling_distributor_min_replicas_per_zone: 2,
    autoscaling_distributor_max_replicas_per_zone: 10,

    // Compartments.
    compartments_enabled: true,
    compartments_read_count: 2,
    compartments_write_count: 2,

    compactor_scheduler_enabled: true,
    autoscaling_compactor_enabled: true,
    autoscaling_compactor_min_replicas: 2,
    autoscaling_compactor_max_replicas: 30,
    cortex_compactor_concurrent_rollout_enabled: true,
    enable_pvc_auto_deletion_for_compactors: true,
    enable_pvc_auto_deletion_for_ingesters: true,
  },
}
