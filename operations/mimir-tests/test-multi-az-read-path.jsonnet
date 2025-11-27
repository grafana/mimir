// Based on test-multi-zone.jsonnet.
// Enables multi-zone and multi-AZ deployment for all read path components.
(import 'test-multi-zone.jsonnet') {
  _config+:: {
    local availabilityZones = ['us-east-2a', 'us-east-2b'],
    multi_zone_availability_zones: availabilityZones,

    // Enable multi-AZ for write path components.
    multi_zone_ingester_multi_az_enabled: true,
    multi_zone_store_gateway_multi_az_enabled: true,

    // Enable multi-zone for read path components.
    multi_zone_memcached_enabled: true,
    multi_zone_querier_enabled: true,
    multi_zone_query_frontend_enabled: true,
    multi_zone_query_scheduler_enabled: true,
    multi_zone_ruler_enabled: true,
    multi_zone_ruler_remote_evaluation_enabled: true,

    // Enable memberlist bridge for zone-aware routing.
    multi_zone_memberlist_bridge_enabled: true,
    memberlist_zone_aware_routing_enabled: true,

    // Required for ruler remote evaluation.
    ruler_remote_evaluation_enabled: true,

    // Query scheduler must use ring mode for multi-zone.
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

    // Test different memcached replica counts per zone.
    // Zone A: 2 replicas, Zone B: 3 replicas
    memcached_frontend_zone_a_replicas: 2,
    memcached_frontend_zone_b_replicas: 3,

    memcached_index_queries_zone_a_replicas: 2,
    memcached_index_queries_zone_b_replicas: 3,

    memcached_chunks_zone_a_replicas: 2,
    memcached_chunks_zone_b_replicas: 3,

    memcached_metadata_zone_a_replicas: 2,
    memcached_metadata_zone_b_replicas: 3,
  },
}
