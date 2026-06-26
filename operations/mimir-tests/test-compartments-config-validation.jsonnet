// Tests the validateMimirCompartmentsConfig() assertions.
//
// Strategy: build one valid compartments-enabled mimir environment, capture real per-compartment
// distributor Deployments and ingester StatefulSets from it, then mutate the manifest (CLI flag or
// metadata.name) and run the validator on a synthetic root. Each test asserts a non-null error
// matching an expected substring; any mismatch fails this jsonnet build.

local compartmentsCommon = import 'mimir/compartments-common.libsonnet';
local env = import 'test-compartments.jsonnet';

// Returns the flag name from an arg string like "-foo=bar" -> "-foo".
local flagName(arg) =
  local eq = std.findSubstr('=', arg);
  if std.length(eq) > 0 then std.substr(arg, 0, eq[0]) else arg;

// Overrides container args on every container of the resource. For each entry in "overrides", any
// existing arg with the same flag name is removed and the override is appended. Other args are preserved.
local overrideContainerArgs(resource, overrides) =
  local overrideNames = [flagName(o) for o in overrides];
  resource {
    spec+: { template+: { spec+: {
      containers: [
        c { args: std.filter(function(arg) !std.member(overrideNames, flagName(arg)), c.args) + overrides }
        for c in resource.spec.template.spec.containers
      ],
    } } },
  };

// Removes every arg with the given flag name from every container of the resource.
local removeContainerArg(resource, flag) =
  resource {
    spec+: { template+: { spec+: {
      containers: [
        c { args: std.filter(function(arg) flagName(arg) != flag, c.args) }
        for c in resource.spec.template.spec.containers
      ],
    } } },
  };

local isError(err, needle) = err != null && std.length(std.findSubstr(needle, err)) > 0;

// Synthetic root carrying validateMimirCompartmentsConfig and the resource fields under test. Built
// from compartments-common alone so the production assertions next to real callers don't fire on the
// deliberately broken manifests.
local validate(resources, resourceNames) =
  (compartmentsCommon { _config+: env._config } + resources).validateMimirCompartmentsConfig(resourceNames);

local distributor = env.distributor_zone_a_deployments.compartment_0;  // distributor-zone-a-wc-0
local ingester = env.ingester_zone_a_statefulsets.compartment_1;  // ingester-zone-a-rc-1

// 1. Write compartment whose "distributor.write-compartment-id" doesn't match its name.
local err1 = validate(
  { dist: overrideContainerArgs(distributor, ['-distributor.write-compartment-id=1']) },
  ['dist'],
);
assert isError(err1, '-distributor.write-compartment-id=1') && isError(err1, 'must match the compartment') :
       'case 1: expected write-compartment-id mismatch error, got: %s' % err1;

// 2. Read compartment whose "ingester.read-compartment-id" doesn't match its name.
local err2 = validate(
  { ing: overrideContainerArgs(ingester, ['-ingester.read-compartment-id=0']) },
  ['ing'],
);
assert isError(err2, '-ingester.read-compartment-id=0') && isError(err2, 'must match the compartment') :
       'case 2: expected read-compartment-id mismatch error, got: %s' % err2;

// 3. Resource set: one good entry, one bad. Validator must walk the map.
local err3 = validate(
  { dists: {
    compartment_0: distributor,
    compartment_1: overrideContainerArgs(env.distributor_zone_a_deployments.compartment_1, ['-distributor.write-compartment-id=0']),
  } },
  ['dists'],
);
assert isError(err3, '-distributor.write-compartment-id=0') && isError(err3, 'must match the compartment') :
       'case 3: expected write-compartment-id mismatch error from resource set, got: %s' % err3;

// 4. Correctly configured compartments produce no error.
local err4 = validate(
  { dist: distributor, ing: ingester },
  ['dist', 'ing'],
);
assert err4 == null :
       'case 4: expected no error for correctly configured compartments, got: %s' % err4;

// 5. Compartment-id flag absent: the jsonnet validator tolerates it.
local err5 = validate(
  { dist: removeContainerArg(distributor, '-distributor.write-compartment-id') },
  ['dist'],
);
assert err5 == null :
       'case 5: expected no error when the compartment-id flag is absent, got: %s' % err5;

// 6. validateAddress: write compartment pointed at another compartment's Kafka cluster.
local err6 = validate(
  { dist: overrideContainerArgs(distributor, ['-ingest-storage.kafka.address=kafka-wc-1.default.svc.cluster.local:9092']) },
  ['dist'],
);
assert isError(err6, '-ingest-storage.kafka.address') && isError(err6, 'must target its own Kafka cluster') :
       'case 6: expected Kafka address error, got: %s' % err6;

// 7. validateTopic: read compartment whose topic keeps the unresolved placeholder.
local err7 = validate(
  { ing: overrideContainerArgs(ingester, ['-ingest-storage.kafka.topic=ingest-rc-<read-compartment-id>']) },
  ['ing'],
);
assert isError(err7, '-ingest-storage.kafka.topic') && isError(err7, 'consumes a single topic') :
       'case 7: expected Kafka topic error, got: %s' % err7;

// 8. Resource name without a compartment marker is treated as a global component (e.g. query-frontend),
// which must read from every write compartment and so must use the Kafka address placeholder rather than
// a concrete per-compartment address.
local err8 = validate(
  { dist: distributor { metadata+: { name: 'distributor' } } },
  ['dist'],
);
assert isError(err8, '-ingest-storage.kafka.address') && isError(err8, 'a global deployment reads from every write compartment') :
       'case 8: expected global-deployment placeholder error, got: %s' % err8;

{}
