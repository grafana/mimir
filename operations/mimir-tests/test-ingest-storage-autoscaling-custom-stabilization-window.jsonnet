// This is the initial state of Mimir namespace, before getting migrated to ingest storage.
(import 'test-multi-zone.jsonnet') {
  _config+:: {
    cluster: 'test-cluster',

    ruler_enabled: true,
    ruler_storage_bucket_name: 'rules-bucket',

    alertmanager_enabled: true,
    alertmanager_storage_bucket_name: 'alerts-bucket',

    // Configure features required by ingest storage migration.
    ruler_remote_evaluation_enabled: true,

    ingest_storage_enabled: true,
    ingest_storage_ingester_instance_ring_dedicated_prefix_enabled: true,

    ingest_storage_ingester_autoscaling_enabled: true,
    ingest_storage_ingester_autoscaling_min_replicas_per_zone: 2,
    ingest_storage_ingester_autoscaling_max_replicas_per_zone: 15,
    ingest_storage_ingester_autoscaling_index_metrics: true,
    ingest_storage_ingester_autoscaling_scale_up_stabilization_window_seconds: $.util.parseDuration('10m'),
    ingest_storage_ingester_autoscaling_scale_down_stabilization_window_seconds: $.util.parseDuration('30m'),

    multi_zone_ingester_replicas: 0,
    ingester_automated_downscale_enabled: false,

    shuffle_sharding+:: {
      ingester_write_path_enabled: true,
      ingester_read_path_enabled: true,
      querier_enabled: true,
      ruler_enabled: true,
      store_gateway_enabled: true,
      ingest_storage_partitions_enabled: true,
    },
  },
}
