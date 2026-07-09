// Renders Mimir with the experimental compartments architecture enabled, deployed multi-AZ across all
// components (write and read path).
local env = (import 'test-ingest-storage-autoscaling-one-trigger.jsonnet') {
  _config+:: {
    multi_zone_availability_zones: ['us-east-2a', 'us-east-2b'],
    ingest_storage_ingester_zones: 2,

    // Multi-AZ write and read path.
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
};

local rulerDistributorAddress(zone) =
  'dns:///distributor-zone-%s-compartments.%s.svc.%s:9095' % [zone, env._config.namespace, env._config.cluster_domain];

assert env.distributor_zone_a_compartments_service.metadata.name == 'distributor-zone-a-compartments' :
       'expected zone-a compartments distributor service name';
assert env.distributor_zone_a_compartments_service.spec.selector['mimir-service'] == 'distributor-zone-a' :
       'expected zone-a compartments distributor service to select compartment distributor pods';
assert env.distributor_zone_a_compartments_service.spec.clusterIP == 'None' :
       'expected zone-a compartments distributor service to be headless';
assert env.ruler_args['ingest-storage.kafka.address'] == env._config.compartments_ingest_storage_kafka_address :
       'expected single-zone ruler to use the compartments Kafka address template';
assert env.ruler_args['ruler.distributor.address'] == rulerDistributorAddress('a') :
       'expected single-zone ruler to write to the zone-a compartments distributor service';
assert env.ruler_zone_a_args['ruler.distributor.address'] == rulerDistributorAddress('a') :
       'expected zone-a ruler to write to the zone-a compartments distributor service';
assert env.ruler_zone_b_args['ruler.distributor.address'] == rulerDistributorAddress('b') :
       'expected zone-b ruler to write to the zone-b compartments distributor service';
assert env.ruler_zone_b_args['ingest-storage.kafka.address'] == env._config.compartments_ingest_storage_kafka_address :
       'expected zone-b ruler to use the compartments Kafka address template';
assert !std.objectHas(env.ruler_args, 'distributor.write-compartment-id') :
       'rulers must stay global and must not set distributor.write-compartment-id';
assert std.length(std.findSubstr('<read-compartment-id>', env.ruler_args[env.mimirBlocksStorageBucketNameFlag])) == 0 :
       'rulers must keep the normal ruler blocks bucket instead of a per-read-compartment bucket';

env
