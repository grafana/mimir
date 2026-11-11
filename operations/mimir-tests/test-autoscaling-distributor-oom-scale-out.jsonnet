// Based on test-autoscaling.jsonnet. Renders the distributor ScaledObject with the OOM protection and OOM
// scale-out triggers. Both need the distributor memory trigger to be disabled.
local env = (import 'test-autoscaling.jsonnet') {
  _config+:: {
    distributor_gomemlimit_enabled: true,
    autoscaling_oom_protection_enabled: true,
    autoscaling_distributor_oom_scale_out_enabled: true,
  },
};

local oomScaleOutQueries(scaledObject) = [
  trigger.metadata.query
  for trigger in scaledObject.spec.triggers
  if std.length(std.findSubstr('_oom_scale_out_hpa_', trigger.name)) > 0
];
local contains(haystack, needle) = std.length(std.findSubstr(needle, haystack)) > 0;

// Consumers can override spec.minReplicaCount after rendering, for example with per-cell scaling configuration.
// The trigger floors its replica counts at the min replica count, so the floor must be the final one, not the one
// the ScaledObject was rendered with. Otherwise a sample below the final min replica count keeps the trigger's cap
// too low for up to 2 hours.
local overriddenMinReplicas = 145;
local overriddenQueries = oomScaleOutQueries(env.distributor_scaled_object { spec+: { minReplicaCount: overriddenMinReplicas } });
assert env.distributor_scaled_object.spec.minReplicaCount != overriddenMinReplicas :
       'test precondition: expected the rendered min replica count to differ from the overridden one';
// The base count ends with its 30-minute window. The cap's floor follows its 1-minute fallback.
assert std.length(overriddenQueries) == 1
       && contains(overriddenQueries[0], '[30m])), %d)' % overriddenMinReplicas)
       && contains(overriddenQueries[0], '[1m])), %d))' % overriddenMinReplicas) :
       'expected the OOM scale-out trigger to floor its replica counts at the overridden min replica count';

// Zone distributor ScaledObjects get pod matchers for the whole zone, which also match the pods of other
// Deployments in the zone, such as compartments. The trigger must count only the pods of its own Deployment.
local zoneQueries = oomScaleOutQueries(env.newDistributorScaledObject('distributor-zone-a', 'pod=~"distributor-zone-a.*"'));
assert std.length(zoneQueries) == 1
       && contains(zoneQueries[0], 'pod=~"distributor-zone-a-[a-z0-9]+-[a-z0-9]+"')
       && contains(zoneQueries[0], 'deployment="distributor-zone-a"')
       && !contains(zoneQueries[0], 'pod=~"distributor-zone-a.*"') :
       'expected the OOM scale-out trigger of a zone distributor to match only the pods of its own Deployment';

env
