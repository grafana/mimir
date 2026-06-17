// Tests the validateMimirMultiZoneConfig() assertions.
//
// Strategy: build one valid multi-zone mimir environment, capture real distributor and
// querier deployments from it, then mutate the manifest (CLI flag or metadata.name) and
// run the validator on a synthetic root. Each test asserts a non-null error matching an
// expected substring; any mismatch fails this jsonnet build.

local mimir = import 'mimir/mimir.libsonnet';
local multiZoneCommon = import 'mimir/multi-zone-common.libsonnet';

// Multi-zone-enabled mimir environment. Its own top-level asserts (including the
// production validateMimirMultiZoneConfig assertions) implicitly cover the happy path.
local env = mimir {
  _config+:: {
    namespace: 'default',
    external_url: 'http://test',

    storage_backend: 'gcs',
    blocks_storage_bucket_name: 'blocks-bucket',

    multi_zone_availability_zones: ['us-east-1a', 'us-east-1b', 'us-east-1c'],
    multi_zone_write_path_enabled: true,
    multi_zone_read_path_enabled: true,
    query_scheduler_service_discovery_mode: 'ring',
  },
};

// Returns the flag name from an arg string like "-foo=bar" -> "-foo".
local flagName(arg) =
  local eq = std.findSubstr('=', arg);
  if std.length(eq) > 0 then std.substr(arg, 0, eq[0]) else arg;

// Overrides container args on every container of the deployment. For each entry in
// "overrides", any existing arg with the same flag name is removed and the override is
// appended. Other args are preserved.
local overrideContainerArgs(deployment, overrides) =
  local overrideNames = [flagName(o) for o in overrides];
  deployment {
    spec+: { template+: { spec+: {
      containers: [
        c { args: std.filter(function(arg) !std.member(overrideNames, flagName(arg)), c.args) + overrides }
        for c in deployment.spec.template.spec.containers
      ],
    } } },
  };

local isError(err, needle) = err != null && std.length(std.findSubstr(needle, err)) > 0;

// Synthetic root carrying validateMimirMultiZoneConfig and the deployment fields under
// test. Built from multi-zone-common alone so the production assertions next to real
// callers don't fire on the deliberately broken manifests.
local validate(deployments, deploymentNames) =
  (multiZoneCommon { _config+: env._config } + deployments).validateMimirMultiZoneConfig(deploymentNames);

// 1. Single deployment, cross-zone CLI address.
local err1 = validate(
  { distributor: overrideContainerArgs(env.distributor_zone_a_deployment, ['-foo.address=svc.zone-b.cluster.local']) },
  ['distributor'],
);
assert isError(err1, 'non-matching zone') :
       'case 1: expected non-matching zone error, got: %s' % err1;

// 2. Single deployment, name without any zone marker.
local err2 = validate(
  { distributor: env.distributor_zone_a_deployment { metadata+: { name: 'distributor' } } },
  ['distributor'],
);
assert isError(err2, 'Unable to extract zone letter') :
       'case 2: expected zone-letter extraction error, got: %s' % err2;

// 3. Single deployment, embedded-form name ("distributor-zone-a-set-0"), cross-zone CLI
//    address.
local err3 = validate(
  { distributor: overrideContainerArgs(env.distributor_zone_a_deployment { metadata+: { name: 'distributor-zone-a-set-0' } }, ['-foo.address=svc.zone-b.cluster.local']) },
  ['distributor'],
);
assert isError(err3, 'non-matching zone') :
       'case 3: expected non-matching zone error from embedded-form name, got: %s' % err3;

// 4. Deployment set: one good entry, one bad. Validator must walk the map.
local err4 = validate(
  { deployments: {
    set_0: env.distributor_zone_a_deployment,
    set_1: overrideContainerArgs(env.distributor_zone_a_deployment, ['-foo.address=svc.zone-b.cluster.local']),
  } },
  ['deployments'],
);
assert isError(err4, 'non-matching zone') :
       'case 4: expected non-matching zone error from deployment set, got: %s' % err4;

// 5. Deployment set with only null entries. Dispatcher must tolerate it (parity with
//    single-deployment behaviour, which returns null when the field resolves to null).
local err5 = validate(
  { deployments: { set_0: null, set_1: null } },
  ['deployments'],
);
assert err5 == null :
       'case 5: expected null for null-entry deployment set, got: %s' % err5;

// 6. Querier with a wrong-zone "querier.prefer-availability-zones" flag value.
//    Exercises validateContainerPreferAvailabilityZones(), which only fires for
//    deployments named querier-* or ruler-querier-*.
local err6 = validate(
  { querier: overrideContainerArgs(env.querier_zone_a_deployment, ['-querier.prefer-availability-zones=zone-b']) },
  ['querier'],
);
assert isError(err6, '-querier.prefer-availability-zones') && isError(err6, 'invalid zones') :
       'case 6: expected invalid prefer-availability-zones error, got: %s' % err6;

// 7. Querier missing the "querier.prefer-availability-zones" flag entirely.
local err7 = validate(
  { querier: env.querier_zone_a_deployment {
    spec+: { template+: { spec+: {
      containers: [
        c { args: std.filter(function(arg) !std.startsWith(arg, '-querier.prefer-availability-zones='), c.args) }
        for c in env.querier_zone_a_deployment.spec.template.spec.containers
      ],
    } } },
  } },
  ['querier'],
);
assert isError(err7, 'missing the required CLI flag "-querier.prefer-availability-zones"') :
       'case 7: expected missing-flag error, got: %s' % err7;

{}
