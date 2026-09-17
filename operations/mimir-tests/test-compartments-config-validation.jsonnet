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

// 9. A write-compartment-id flag on a read compartment must be rejected even when its value numerically
// matches the read compartment id, because the flag belongs only on a write compartment.
local err9 = validate(
  { ing: overrideContainerArgs(ingester, ['-distributor.write-compartment-id=1']) },
  ['ing'],
);
assert isError(err9, '-distributor.write-compartment-id=1') && isError(err9, 'does not belong to a write compartment') :
       'case 9: expected misplaced write-compartment-id error, got: %s' % err9;

// 10. A compartment-id flag on a global resource must produce a clean error rather than aborting the
// jsonnet evaluation (the global compartment carries no id to compare against). The Kafka address is
// reset to the placeholder so the address validator passes and the compartment-id validator is reached.
local err10 = validate(
  { gf: overrideContainerArgs(
    distributor { metadata+: { name: 'distributor' } },
    ['-ingest-storage.kafka.address=' + env._config.compartments_ingest_storage_kafka_address],
  ) },
  ['gf'],
);
assert isError(err10, '-distributor.write-compartment-id=0') && isError(err10, 'does not belong to a write compartment') :
       'case 10: expected global-resource compartment-id error, got: %s' % err10;

// 11. validateBlocksBucket: read compartment pointed at another compartment's bucket.
local err11 = validate(
  { ing: overrideContainerArgs(ingester, ['-blocks-storage.gcs.bucket-name=blocks-bucket-rc-0']) },
  ['ing'],
);
assert isError(err11, '-blocks-storage.gcs.bucket-name=blocks-bucket-rc-0') && isError(err11, 'uses its own blocks-storage bucket') :
       'case 11: expected blocks-storage bucket mismatch error, got: %s' % err11;

// 12. validateBlocksBucket: read compartment pointed at the shared non-compartment bucket.
local err12 = validate(
  { ing: overrideContainerArgs(ingester, ['-blocks-storage.gcs.bucket-name=blocks-bucket']) },
  ['ing'],
);
assert isError(err12, '-blocks-storage.gcs.bucket-name=blocks-bucket') && isError(err12, 'uses its own blocks-storage bucket') :
       'case 12: expected blocks-storage shared-bucket error, got: %s' % err12;

// 13. validateBlocksBucket: read compartment keeping the parametrised placeholder is accepted.
local err13 = validate(
  { ing: overrideContainerArgs(ingester, ['-blocks-storage.gcs.bucket-name=blocks-bucket-rc-<read-compartment-id>']) },
  ['ing'],
);
assert err13 == null :
       'case 13: expected no error for a parametrised blocks-storage bucket, got: %s' % err13;

// 14. validateBlocksBucket: read compartment with its own concrete bucket is accepted.
local err14 = validate(
  { ing: overrideContainerArgs(ingester, ['-blocks-storage.gcs.bucket-name=blocks-bucket-rc-1']) },
  ['ing'],
);
assert err14 == null :
       'case 14: expected no error for the matching per-compartment bucket, got: %s' % err14;

// 15. validateBlocksBucket: write compartment with a concrete per-compartment bucket must be rejected,
// because only read compartments own dedicated blocks-storage buckets.
local err15 = validate(
  { dist: overrideContainerArgs(distributor, ['-blocks-storage.gcs.bucket-name=blocks-bucket-rc-0']) },
  ['dist'],
);
assert isError(err15, '-blocks-storage.gcs.bucket-name=blocks-bucket-rc-0') && isError(err15, 'only a read compartment owns a dedicated blocks-storage bucket') :
       'case 15: expected write-compartment blocks-storage bucket error, got: %s' % err15;

// A global deployment (no compartment marker) built from the distributor: the Kafka address is reset to the
// placeholder and the write-compartment-id flag removed, so only the blocks-storage bucket is under test.
local globalWithBucket(bucket) =
  overrideContainerArgs(
    removeContainerArg(distributor { metadata+: { name: 'global' } }, '-distributor.write-compartment-id'),
    [
      '-ingest-storage.kafka.address=' + env._config.compartments_ingest_storage_kafka_address,
      '-blocks-storage.gcs.bucket-name=' + bucket,
    ],
  );

// 16. validateBlocksBucket: global deployment with a concrete per-compartment bucket must be rejected,
// because one bucket can't serve every read compartment.
local err16 = validate({ gf: globalWithBucket('blocks-bucket-rc-0') }, ['gf']);
assert isError(err16, '-blocks-storage.gcs.bucket-name=blocks-bucket-rc-0') && isError(err16, 'only a read compartment owns a dedicated blocks-storage bucket') :
       'case 16: expected global-deployment blocks-storage bucket error, got: %s' % err16;

// 17. validateBlocksBucket: global deployment keeping the parametrised placeholder is accepted.
local err17 = validate({ gf: globalWithBucket('blocks-bucket-rc-<read-compartment-id>') }, ['gf']);
assert err17 == null :
       'case 17: expected no error for a parametrised blocks-storage bucket on a global deployment, got: %s' % err17;

//
// validateMimirCompartmentsAutoscalingDisabledKnobs() — the paired per-compartment autoscaling
// disable knobs. The validator is a pure function of its arguments, so it's called directly.
//

local validateKnobs(disabledCompartments, staticReplicas, numCompartments=2, autoscalingEnabled=true) =
  (compartmentsCommon { _config+: env._config }).validateMimirCompartmentsAutoscalingDisabledKnobs(
    'autoscaling_foo_disabled_compartments',
    disabledCompartments,
    'foo_compartment_static_replicas',
    staticReplicas,
    numCompartments,
    'autoscaling_foo_enabled',
    autoscalingEnabled,
  );

// 18. A valid pair produces no error; so do the empty defaults.
local err18a = validateKnobs([1], { compartment_1: 3 });
assert err18a == null : 'case 18a: expected no error for a valid knob pair, got: %s' % err18a;
local err18b = validateKnobs([], {});
assert err18b == null : 'case 18b: expected no error for the empty defaults, got: %s' % err18b;

// 19. Malformed knob shapes.
local err19a = validateKnobs(0, {});
assert isError(err19a, 'must be a list of compartment indexes') :
       'case 19a: expected a shape error for a non-list disable knob, got: %s' % err19a;
local err19b = validateKnobs([], [1]);
assert isError(err19b, 'must be an object mapping') :
       'case 19b: expected a shape error for a non-object static-replicas knob, got: %s' % err19b;

// 20. Non-integer and out-of-range compartment indexes.
local err20a = validateKnobs([1.5], { compartment_1: 3 });
assert isError(err20a, 'compartment indexes must be integers') :
       'case 20a: expected a non-integer index error, got: %s' % err20a;
local err20b = validateKnobs(['1'], { compartment_1: 3 });
assert isError(err20b, 'compartment indexes must be integers') :
       'case 20b: expected a non-integer index error for a string index, got: %s' % err20b;
local err20c = validateKnobs([2], { compartment_2: 3 });
assert isError(err20c, 'only compartments [0, 2) exist') :
       'case 20c: expected an out-of-range index error, got: %s' % err20c;

// 21. Duplicate compartment indexes.
local err21 = validateKnobs([1, 1], { compartment_1: 3 });
assert isError(err21, 'duplicate compartment indexes') :
       'case 21: expected a duplicate-index error, got: %s' % err21;

// 22. A non-empty disable list has no effect while the component-level autoscaling knob is off.
local err22 = validateKnobs([1], { compartment_1: 3 }, autoscalingEnabled=false);
assert isError(err22, 'autoscaling_foo_enabled is false') && isError(err22, 'has no effect') :
       'case 22: expected a component-knob pairing error, got: %s' % err22;

// 23. Every disabled compartment needs a static replica count, and it must be a positive integer.
local err23a = validateKnobs([1], {});
assert isError(err23a, 'set an explicit count via foo_compartment_static_replicas: { compartment_1: <replicas> }') :
       'case 23a: expected a missing static-replicas error, got: %s' % err23a;
local err23b = validateKnobs([1], { compartment_1: 0 });
assert isError(err23b, 'foo_compartment_static_replicas.compartment_1 must be an integer greater than 0') :
       'case 23b: expected a non-positive replicas error, got: %s' % err23b;
local err23c = validateKnobs([1], { compartment_1: 2.5 });
assert isError(err23c, 'foo_compartment_static_replicas.compartment_1 must be an integer greater than 0') :
       'case 23c: expected a non-integer replicas error, got: %s' % err23c;

// 24. Stale or malformed static-replicas keys (compartment not disabled, or not a compartment key).
local err24a = validateKnobs([1], { compartment_1: 3, compartment_0: 2 });
assert isError(err24a, 'has entry "compartment_0"') && isError(err24a, 'not listed in autoscaling_foo_disabled_compartments') :
       'case 24a: expected a stale static-replicas key error, got: %s' % err24a;
local err24b = validateKnobs([1], { compartment_1: 3, foo: 2 });
assert isError(err24b, 'has entry "foo"') :
       'case 24b: expected a malformed static-replicas key error, got: %s' % err24b;

{}
