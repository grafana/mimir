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

    // Exercise the global overrides-exporter workload.
    overrides_exporter_enabled: true,
  },
};

local rulerDistributorAddress(zone) =
  'dns:///distributor-zone-%s.%s.svc.%s:9095' % [zone, env._config.namespace, env._config.cluster_domain];

local rulerBlocksBucket(args) = args[env.mimirBlocksStorageBucketNameFlag];

local all(values) = std.foldl(function(acc, value) acc && value, values, true);
local any(values) = std.foldl(function(acc, value) acc || value, values, false);

local hasCommonArgs(args) = all([
  std.objectHas(args, flag) && args[flag] == env.mimirCompartmentsCommonArgs[flag]
  for flag in std.objectFields(env.mimirCompartmentsCommonArgs)
]);

local globalComponentArgs = {
  alertmanager: env.alertmanager_args,
  query_scheduler: env.query_scheduler_args,
  ruler_query_scheduler: env.ruler_query_scheduler_args,
  overrides_exporter: env.overrides_exporter_args,
  memberlist_bridge: env.memberlist_bridge_args,
};

local resourceContainers(resource) =
  if resource == null ||
     !std.objectHas(resource, 'spec') ||
     !std.objectHas(resource.spec, 'template') ||
     !std.objectHas(resource.spec.template, 'spec') ||
     !std.objectHas(resource.spec.template.spec, 'containers') then
    []
  else
    resource.spec.template.spec.containers;

local rootFlagNames = [
  '-compartments.enabled',
  '-compartments.read.num-compartments',
  '-compartments.write.num-compartments',
];

local hasAnyRootFlag(resource) = any([
  std.isString(arg) && any([std.startsWith(arg, flag + '=') for flag in rootFlagNames])
  for container in resourceContainers(resource)
  if std.objectHas(container, 'args')
  for arg in container.args
]);

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

local dataPathCoexistenceEnv = env {
  _config+:: {
    no_compartments_distributor_enabled: true,
    no_compartments_ingester_enabled: true,
    no_compartments_store_gateway_enabled: true,
    no_compartments_compactor_enabled: true,
  },
};

local dataPathCoexistenceResources = {
  distributor: dataPathCoexistenceEnv.distributor_zone_a_deployment,
  ingester: dataPathCoexistenceEnv.ingester_zone_a_statefulset,
  store_gateway: dataPathCoexistenceEnv.store_gateway_zone_a_statefulset,
  compactor: dataPathCoexistenceEnv.compactor_statefulset,
  compactor_scheduler: dataPathCoexistenceEnv.compactor_scheduler_statefulset,
};

local missingCommonArgs = [name for name in std.objectFields(globalComponentArgs) if !hasCommonArgs(globalComponentArgs[name])];
assert std.length(missingCommonArgs) == 0 :
       'expected global Mimir component args to inherit compartments defaults; missing on: %s' % std.join(', ', missingCommonArgs);

local missingCoexistenceResources = [name for name in std.objectFields(dataPathCoexistenceResources) if std.length(resourceContainers(dataPathCoexistenceResources[name])) == 0];
assert std.length(missingCoexistenceResources) == 0 :
       'expected legacy no-compartments coexistence resources to render; missing: %s' % std.join(', ', missingCoexistenceResources);

local coexistenceResourcesWithRootFlags = [name for name in std.objectFields(dataPathCoexistenceResources) if hasAnyRootFlag(dataPathCoexistenceResources[name])];
assert std.length(coexistenceResourcesWithRootFlags) == 0 :
       'expected legacy no-compartments coexistence resources to omit compartments root flags; found on: %s' % std.join(', ', coexistenceResourcesWithRootFlags);

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

env
