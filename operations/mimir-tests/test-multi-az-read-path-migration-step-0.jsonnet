// This is the initial state of Mimir namespace, before getting migrated to ingest storage.
(import 'test-multi-zone.jsonnet') {
  _config+:: {
    local availabilityZones = ['us-east-2a', 'us-east-2b'],
    multi_zone_availability_zones: availabilityZones,

    // Assume write path has already been migrated to multi-zone.
    multi_zone_write_path_enabled: true,

    // Assume memberlist bridge is already configured with zone-aware routing.
    multi_zone_memberlist_bridge_enabled: true,
    memberlist_zone_aware_routing_enabled: true,

    // Enable features required by multi-zone deployment.
    ruler_remote_evaluation_enabled: true,
    query_scheduler_service_discovery_mode: 'ring',

    // Enable autoscaling for some components to test the autoscaling configuration.
    autoscaling_querier_enabled: true,
    autoscaling_querier_min_replicas_per_zone: 3,
    autoscaling_querier_max_replicas_per_zone: 30,

    autoscaling_query_frontend_enabled: true,
    autoscaling_query_frontend_min_replicas_per_zone: 2,
    autoscaling_query_frontend_max_replicas_per_zone: 20,

    autoscaling_ruler_enabled: true,
    autoscaling_ruler_min_replicas_per_zone: 2,
    autoscaling_ruler_max_replicas_per_zone: 10,

    autoscaling_ruler_querier_enabled: true,
    autoscaling_ruler_querier_min_replicas_per_zone: 2,
    autoscaling_ruler_querier_max_replicas_per_zone: 10,

    autoscaling_ruler_query_frontend_enabled: true,
    autoscaling_ruler_query_frontend_min_replicas_per_zone: 2,
    autoscaling_ruler_query_frontend_max_replicas_per_zone: 10,

    // Enable shuffle sharding for a more comprehensive testing.
    shuffle_sharding+:: {
      ingester_write_path_enabled: true,
      ingester_read_path_enabled: true,
      querier_enabled: true,
      ruler_enabled: true,
      store_gateway_enabled: true,
    },
  },
}
