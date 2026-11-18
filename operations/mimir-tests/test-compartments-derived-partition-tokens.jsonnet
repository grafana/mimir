local derivedTokensTest = import 'test-ingest-storage-derived-partition-tokens.jsonnet';

local env = (import 'test-compartments.jsonnet') + derivedTokensTest.derivedTokensConfig;
local errors = derivedTokensTest.derivedTokensErrors(env);
assert std.length(errors) == 0 : std.join('; ', errors);

local compartmentMaxReplicas =
  [scaledObject.spec.maxReplicaCount for scaledObject in std.objectValues(env.ingest_storage_ingester_primary_zone_scalings)]
  + std.objectValues(env._config.ingester_compartment_static_replicas);
assert env.validateMaxDerivedTokenPartitions('compartments ingesters', compartmentMaxReplicas, 10) == null;
assert env.validateMaxDerivedTokenPartitions('compartments ingesters', compartmentMaxReplicas, 9) == 'compartments ingesters must not have more max replicas than ingest_storage_partition_ring_max_derived_token_partitions (9)';

{}
