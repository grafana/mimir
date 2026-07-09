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
local ruler = env.ruler_zone_a_deployment;

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
    removeContainerArg(distributor { metadata+: { name: 'querier' } }, '-distributor.write-compartment-id'),
    [
      '-target=querier',
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

// 18. Compartment distributors need at least one AZ so the ruler can route to a zone-a service.
local err18 = (compartmentsCommon {
                 _config+: env._config {
                   compartments_distributor_enabled: true,
                   multi_zone_availability_zones: [],
                 },
               }).validateMimirCompartmentsDistributorZones();
assert isError(err18, 'at least one configured multi_zone_availability_zones entry') :
       'case 18: expected distributor zone validation error, got: %s' % err18;

// 19. Compartmented rulers must have a query-frontend address so they don't fall back to local queries.
local err19 = validate(
  { ruler: removeContainerArg(ruler, '-ruler.query-frontend.address') },
  ['ruler'],
);
assert isError(err19, '-ruler.query-frontend.address') && isError(err19, 'non-empty') :
       'case 19: expected missing ruler query-frontend address error, got: %s' % err19;

// 20. An empty ruler query-frontend address is equivalent to disabling remote evaluation at runtime.
local err20 = validate(
  { ruler: overrideContainerArgs(ruler, ['-ruler.query-frontend.address=']) },
  ['ruler'],
);
assert isError(err20, '-ruler.query-frontend.address') && isError(err20, 'non-empty') :
       'case 20: expected empty ruler query-frontend address error, got: %s' % err20;

{}
