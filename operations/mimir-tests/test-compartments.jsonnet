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
  'dns:///distributor-zone-%s.%s.svc.%s:9095' % [zone, env._config.namespace, env._config.cluster_domain];

local rulerBlocksBucket(args) = args[env.mimirBlocksStorageBucketNameFlag];

local coexistenceEnv = env {
  _config+:: {
    no_compartments_distributor_enabled: true,
  },
};

local routedCoexistenceEnv = coexistenceEnv {
  _config+:: {
    compartments_distributor_routing_enabled: true,
  },
};

assert env._config.compartments_distributor_routing_enabled :
       'expected compartments-only deployments to route to compartment distributors';
assert env.distributor_zone_a_service.metadata.name == 'distributor-zone-a' :
       'expected the stable zone-a distributor service name';
assert env.distributor_zone_a_service.spec.selector == { 'mimir-service': 'distributor-zone-a' } :
       'expected the zone-a distributor service to select only compartment distributor pods';
assert env.distributor_zone_a_service.spec.clusterIP == 'None' :
       'expected the zone-a distributor service to be headless';
assert !coexistenceEnv._config.compartments_distributor_routing_enabled :
       'expected coexistence to route to the no-compartments distributor by default';
assert coexistenceEnv.distributor_zone_a_service.spec.selector == { name: 'distributor-zone-a' } :
       'expected coexistence to keep routing to the no-compartments distributor';
assert routedCoexistenceEnv.distributor_zone_a_service.spec.selector == { 'mimir-service': 'distributor-zone-a' } :
       'expected the routing option to switch the stable service to compartment distributor pods';
assert coexistenceEnv.ruler_args['ruler.distributor.address'] == rulerDistributorAddress('a') :
       'expected the ruler address to stay stable before routing to compartment distributors';
assert routedCoexistenceEnv.ruler_args['ruler.distributor.address'] == rulerDistributorAddress('a') :
       'expected the ruler address to stay stable after routing to compartment distributors';
assert env.ruler_args['ingest-storage.kafka.address'] == env._config.compartments_ingest_storage_kafka_address :
       'expected single-zone ruler to use the compartments Kafka address template';
assert env.ruler_args['ruler.distributor.address'] == rulerDistributorAddress('a') :
       'expected single-zone ruler to write to the stable zone-a distributor service';
assert env.ruler_zone_a_args['ruler.distributor.address'] == rulerDistributorAddress('a') :
       'expected zone-a ruler to write to the stable zone-a distributor service';
assert env.ruler_zone_b_args['ruler.distributor.address'] == rulerDistributorAddress('b') :
       'expected zone-b ruler to write to the stable zone-b distributor service';
assert env.ruler_zone_b_args['ingest-storage.kafka.address'] == env._config.compartments_ingest_storage_kafka_address :
       'expected zone-b ruler to use the compartments Kafka address template';
assert !std.objectHas(env.ruler_args, 'distributor.write-compartment-id') :
       'rulers must stay global and must not set distributor.write-compartment-id';
assert rulerBlocksBucket(env.ruler_args) == env._config.compartments_blocks_storage_bucket_name :
       'expected single-zone ruler to use the parametrised blocks bucket';
assert rulerBlocksBucket(env.ruler_zone_a_args) == env._config.compartments_blocks_storage_bucket_name :
       'expected zone-a ruler to use the parametrised blocks bucket';
assert rulerBlocksBucket(env.ruler_zone_b_args) == env._config.compartments_blocks_storage_bucket_name :
       'expected zone-b ruler to use the parametrised blocks bucket';

local schedulerAffinityMatchers = [
  { key: 'topology.kubernetes.io/zone', operator: 'In', values: ['us-east-2a'] },
];
local schedulerAffinityEnv = env {
  compactor_scheduler_node_affinity_matchers:: schedulerAffinityMatchers,
};
local compartmentSchedulerNodeSelectorTerms(compartmentIdx) =
  local podSpec = schedulerAffinityEnv.compactor_scheduler_statefulsets['compartment_%d' % compartmentIdx].spec.template.spec;
  if std.objectHas(podSpec, 'affinity')
  then podSpec.affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms
  else [];
assert compartmentSchedulerNodeSelectorTerms(0) == [{ matchExpressions: schedulerAffinityMatchers }] :
       'expected per-compartment compactor-schedulers to inherit compactor_scheduler_node_affinity_matchers';
assert compartmentSchedulerNodeSelectorTerms(1) == [{ matchExpressions: schedulerAffinityMatchers }] :
       'expected per-compartment compactor-schedulers to inherit compactor_scheduler_node_affinity_matchers';

// The hidden compactor_containers map is the per-compartment patch point for downstream resource
// overrides: patches layered onto it must propagate into the rendered StatefulSets, per compartment.
local compactorResourcesEnv = env {
  compactor_containers+:: {
    compartment_1+: { resources+: { requests+: { cpu: '7' } } },
  },
};
assert std.objectFields(env.compactor_containers) == ['compartment_0', 'compartment_1'] :
       'expected one compactor_containers entry per read compartment';
assert compactorResourcesEnv.compactor_statefulsets.compartment_1.spec.template.spec.containers[0].resources.requests.cpu == '7' :
       'expected a compactor_containers patch to propagate into its compartment StatefulSet';
assert compactorResourcesEnv.compactor_statefulsets.compartment_0 == env.compactor_statefulsets.compartment_0 :
       'expected other compartments to be unaffected by a compactor_containers patch';

// Per-compartment KEDA autoscaling disable: a disabled compartment loses exactly its ScaledObject
// (and ReplicaTemplate, for the ingester), runs the explicit static replica count, and drops the
// autoscaling annotations; sibling compartments are unaffected.
local disabledCompartmentsEnv = env {
  _config+:: {
    autoscaling_compactor_disabled_compartments: [1],
    compactor_compartment_static_replicas: { compartment_1: 3 },
    autoscaling_distributor_disabled_compartments: [0],
    distributor_compartment_static_replicas: { compartment_0: 2 },
    autoscaling_store_gateway_disabled_compartments: [1],
    store_gateway_compartment_static_replicas: { compartment_1: 4 },
    ingest_storage_ingester_autoscaling_disabled_compartments: [1],
    ingester_compartment_static_replicas: { compartment_1: 5 },
  },
};
local annotationsOf(resource) = std.get(resource.metadata, 'annotations', {});
local labelsOf(resource) = std.get(resource.metadata, 'labels', {});

// With the knobs at their defaults, every compartment is autoscaled.
assert std.objectFields(env.compactor_scaled_objects) == ['compartment_0', 'compartment_1'] :
       'expected all compactor compartments to have a ScaledObject by default';
assert std.objectFields(env.ingest_storage_ingester_primary_zone_scalings) == ['compartment_0', 'compartment_1'] :
       'expected all ingester compartments to have a ScaledObject by default';

// Exactly the disabled compartment's autoscaling objects are gone.
assert std.objectFields(disabledCompartmentsEnv.compactor_scaled_objects) == ['compartment_0'] :
       'expected the disabled compactor compartment to lose its ScaledObject';
assert std.objectFields(disabledCompartmentsEnv.distributor_zone_a_scaled_objects) == ['compartment_1'] &&
       std.objectFields(disabledCompartmentsEnv.distributor_zone_b_scaled_objects) == ['compartment_1'] :
       'expected the disabled distributor compartment to lose its per-zone ScaledObjects';
assert std.objectFields(disabledCompartmentsEnv.store_gateway_zone_a_scaled_objects) == ['compartment_0'] :
       'expected the disabled store-gateway compartment to lose its leader ScaledObject';
assert std.objectFields(disabledCompartmentsEnv.ingest_storage_ingester_primary_zone_scalings) == ['compartment_0'] &&
       std.objectFields(disabledCompartmentsEnv.ingester_primary_zone_replica_templates) == ['compartment_0'] :
       'expected the disabled ingester compartment to lose its ScaledObject and ReplicaTemplate';

// The disabled compartment's workloads run the static replica count (per zone where zonal).
assert disabledCompartmentsEnv.compactor_statefulsets.compartment_1.spec.replicas == 3 :
       'expected the disabled compactor compartment to run static replicas';
assert disabledCompartmentsEnv.distributor_zone_a_deployments.compartment_0.spec.replicas == 2 &&
       disabledCompartmentsEnv.distributor_zone_b_deployments.compartment_0.spec.replicas == 2 :
       'expected the disabled distributor compartment to run static replicas in every zone';
assert disabledCompartmentsEnv.store_gateway_zone_a_statefulsets.compartment_1.spec.replicas == 4 &&
       disabledCompartmentsEnv.store_gateway_zone_b_statefulsets.compartment_1.spec.replicas == 4 &&
       disabledCompartmentsEnv.store_gateway_zone_c_statefulsets.compartment_1.spec.replicas == 4 :
       'expected the disabled store-gateway compartment to run static replicas in every zone';
assert disabledCompartmentsEnv.ingester_zone_a_statefulsets.compartment_1.spec.replicas == 5 &&
       disabledCompartmentsEnv.ingester_zone_b_statefulsets.compartment_1.spec.replicas == 5 :
       'expected the disabled ingester compartment to run static replicas in every zone';

// The disabled compartment drops the autoscaling annotations, labels and args.
assert !std.objectHas(annotationsOf(disabledCompartmentsEnv.store_gateway_zone_b_statefulsets.compartment_1), 'grafana.com/rollout-downscale-leader') &&
       !std.objectHas(labelsOf(disabledCompartmentsEnv.store_gateway_zone_b_statefulsets.compartment_1), 'grafana.com/prepare-downscale') :
       'expected the disabled store-gateway compartment to drop the autoscaling annotations and labels';
assert !std.objectHas(disabledCompartmentsEnv.store_gateway_zone_a_compartments_args.compartment_1, 'store-gateway.sharding-ring.auto-forget-enabled') :
       'expected the disabled store-gateway compartment to keep the default auto-forget behavior';
assert !std.objectHas(annotationsOf(disabledCompartmentsEnv.ingester_zone_a_statefulsets.compartment_1), 'grafana.com/rollout-mirror-replicas-from-resource-name') &&
       !std.objectHas(annotationsOf(disabledCompartmentsEnv.ingester_zone_b_statefulsets.compartment_1), 'grafana.com/rollout-downscale-leader') :
       'expected the disabled ingester compartment to drop the leader/follower autoscaling annotations';

// Sibling compartments keep the autoscaled shape: replicas stripped, annotations and args intact.
assert !std.objectHas(disabledCompartmentsEnv.compactor_statefulsets.compartment_0.spec, 'replicas') :
       'expected the autoscaled compactor compartment to keep its replicas stripped';
assert !std.objectHas(disabledCompartmentsEnv.distributor_zone_a_deployments.compartment_1.spec, 'replicas') :
       'expected the autoscaled distributor compartment to keep its replicas stripped';
assert annotationsOf(disabledCompartmentsEnv.store_gateway_zone_b_statefulsets.compartment_0)['grafana.com/rollout-downscale-leader'] == 'store-gateway-zone-a-rc-0' :
       'expected the autoscaled store-gateway compartment to keep following its leader';
assert std.objectHas(annotationsOf(disabledCompartmentsEnv.ingester_zone_a_statefulsets.compartment_0), 'grafana.com/rollout-mirror-replicas-from-resource-name') :
       'expected the autoscaled ingester compartment to keep mirroring its ReplicaTemplate';
assert disabledCompartmentsEnv.store_gateway_zone_a_compartments_args.compartment_0['store-gateway.sharding-ring.auto-forget-enabled'] == false :
       'expected the autoscaled store-gateway compartment to keep auto-forget disabled';

env
