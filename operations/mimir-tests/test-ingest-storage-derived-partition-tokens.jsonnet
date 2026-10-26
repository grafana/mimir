local derivedTokensConfig = {
  _config+:: {
    ingest_storage_partition_ring_max_derived_token_partitions: 32,
    ingest_storage_partition_ring_create_partitions_with_derived_tokens: true,
  },
};

local isWorkload(resource) = std.isObject(resource) && std.member(['Deployment', 'StatefulSet'], std.get(resource, 'kind'));

// Returns the Deployments and StatefulSets of env, including the ones in per-compartment maps.
local workloads(env) = std.flattenArrays([
  local resource = env[field];
  if isWorkload(resource) then [resource]
  else if std.isObject(resource) then [resource[k] for k in std.objectFields(resource) if isWorkload(resource[k])]
  else []
  for field in std.objectFields(env)
]);

local containerArgs(workload) = std.flattenArrays([std.get(c, 'args', []) for c in workload.spec.template.spec.containers]);

// Returns the errors found in the derived tokens config of the workloads reading the partition ring.
local derivedTokensErrors(env) =
  local readers = [w for w in workloads(env) if std.member(containerArgs(w), '-ingester.partition-ring.prefix=')];
  local ingesters = [w for w in readers if std.startsWith(w.metadata.name, 'ingester-')];
  (if std.length(ingesters) > 0 then [] else ['no ingester reads the partition ring']) + [
    '%s: missing the derived tokens maximum' % w.metadata.name
    for w in readers
    if !std.member(containerArgs(w), '-ingester.partition-ring.max-derived-token-partitions=32')
  ] + [
    '%s: creates partitions with derived tokens: %s' % [w.metadata.name, std.member(containerArgs(w), '-ingester.partition-ring.create-partitions-with-derived-tokens=true')]
    for w in readers
    if std.member(containerArgs(w), '-ingester.partition-ring.create-partitions-with-derived-tokens=true') != std.member(ingesters, w)
  ];

local env = (import 'test-ingest-storage-autoscaling-one-trigger.jsonnet') + derivedTokensConfig;
local errors = derivedTokensErrors(env);
assert std.length(errors) == 0 : std.join('; ', errors);

env {
  derivedTokensConfig:: derivedTokensConfig,
  derivedTokensErrors:: derivedTokensErrors,
}
