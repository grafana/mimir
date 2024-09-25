// Migration final step:
// - Cleanup jsonnet.
//
// This is initial state + final state, without importing ingest-store-migration-autoscaling.libsonnet file.
(import 'test-ingest-storage-migration-step-0.jsonnet') {
  _config+:: {

    // Disable other scaling features.
    multi_zone_ingester_replicas: 0,
    new_ingester_hpa_enabled: false,
    ingester_automated_downscale_enabled: false,
    ingester_automated_downscale_v2_enabled: false,

    // Ingest storage configuration
    ingest_storage_enabled: true,
    ingest_storage_ingester_instance_ring_dedicated_prefix_enabled: true,

    // Enable ingest-storage autoscaling, and configure minimum replicas to match original number of ingesters.
    ingest_storage_ingester_autoscaling_enabled: true,
    ingest_storage_ingester_autoscaling_min_replicas_per_zone: 10,
    ingest_storage_ingester_autoscaling_max_replicas_per_zone: 20,
    // Use same label-selector as ReplicaTemplate in ingester_automated_downscale_v2_enabled.
    ingest_storage_replica_template_label_selector: 'name=ingester-zone-a',

    // We're not going to deploy ingester-zone-c.
    ingest_storage_ingester_zones: 2,
  },
}
