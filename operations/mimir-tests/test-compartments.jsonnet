// Renders Mimir with the experimental compartments architecture enabled, deployed multi-AZ across all
// components (write and read path). This matches the production topology (e.g. mimir-dev-26): the
// per-compartment memcached caches are zonal (memcached-zone-<zone>-rc-<id>) and each per-compartment
// store-gateway zone routes to its zonal cache, exercising the 2-AZ zone-c->zone-a fallback.
// Based on test-ingest-storage-autoscaling-one-trigger.jsonnet.
(import 'test-ingest-storage-autoscaling-one-trigger.jsonnet') {
  _config+:: {
    multi_zone_availability_zones: ['us-east-2a', 'us-east-2b'],
    ingest_storage_ingester_zones: 2,

    // Multi-AZ write and read path: distributors, ingesters, queriers, query-frontends, query-schedulers,
    // rulers, store-gateways and memcached all become zonal/AZ-pinned (multi_zone_read_path_multi_az_enabled
    // also drives the ingester multi-AZ deployment that the compartments ingester requires).
    multi_zone_write_path_enabled: true,
    multi_zone_read_path_enabled: true,
    multi_zone_read_path_multi_az_enabled: true,
    multi_zone_memberlist_bridge_enabled: true,
    memberlist_zone_aware_routing_enabled: true,
    query_scheduler_service_discovery_mode: 'ring',

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

    // Exercise the per-compartment store-gateways (zones a/b/c, no backup zones).
    multi_zone_store_gateway_enabled: true,
    multi_zone_store_gateway_replicas: 3,
    autoscaling_store_gateway_enabled: true,
    autoscaling_store_gateway_min_replicas_per_zone: 1,
    autoscaling_store_gateway_max_replicas_per_zone: 6,
    autoscaling_store_gateway_min_replicas_per_compartment_zone: 1,
    autoscaling_store_gateway_max_replicas_per_compartment_zone: 3,
    enable_pvc_auto_deletion_for_store_gateways: true,
  },
}
