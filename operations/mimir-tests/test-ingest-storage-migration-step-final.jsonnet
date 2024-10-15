// Migration final step:
// - Cleanup jsonnet.
//
// This step is building on step 0, so that we apply the definitive configuration. We expect no K8S manifests difference
// between the previous migration step and this one.
(import 'test-ingest-storage-migration-step-0.jsonnet') {
  _config+:: {
    ingest_storage_enabled: true,
    ingest_storage_ingester_instance_ring_dedicated_prefix_enabled: true,

    ingest_storage_ingester_autoscaling_enabled: true,
    ingest_storage_ingester_autoscaling_min_replicas_per_zone: 2,
    ingest_storage_ingester_autoscaling_max_replicas_per_zone: 15,

    multi_zone_ingester_replicas: 0,
    ingester_automated_downscale_enabled: false,

    shuffle_sharding+:: {
      ingest_storage_partitions_enabled: true,
    },
  },
}
