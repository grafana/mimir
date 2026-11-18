{
  _config+:: {
    // Controls whether the multi (virtual) zone usage-tracker should also be deployed multi-AZ.
    multi_zone_usage_tracker_multi_az_enabled: $._config.multi_zone_distributor_enabled,

    // Kafka is a single cluster shared across availability zones, see ingest-storage.libsonnet.
    multi_zone_config_validation_excluded_args+: if !$._config.usage_tracker_enabled then [] else [
      '-usage-tracker.events-storage.writer.address',
      '-usage-tracker.events-storage.reader.address',
      '-usage-tracker.snapshots-metadata.writer.address',
      '-usage-tracker.snapshots-metadata.reader.address',
    ],
  },

  local statefulSet = $.apps.v1.statefulSet,

  local isMultiZoneEnabled = $._config.multi_zone_usage_tracker_multi_az_enabled,
  local isZoneAEnabled = isMultiZoneEnabled && std.length($._config.multi_zone_availability_zones) >= 1,
  local isZoneBEnabled = isMultiZoneEnabled && std.length($._config.multi_zone_availability_zones) >= 2,

  usage_tracker_zone_a_node_affinity_matchers+:: if isZoneAEnabled then [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[0])] else [],
  usage_tracker_zone_b_node_affinity_matchers+:: if isZoneBEnabled then [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[1])] else [],

  usage_tracker_zone_a_args+:: $.memberlist_zone_a_args,
  usage_tracker_zone_b_args+:: if isZoneBEnabled then $.memberlist_zone_b_args else $.memberlist_zone_a_args,

  usage_tracker_zone_a_statefulset: overrideSuperIfExists('usage_tracker_zone_a_statefulset', if isZoneAEnabled then statefulSet.spec.template.spec.withTolerationsMixin($.newMimirMultiZoneToleration()) else {}),
  usage_tracker_zone_b_statefulset: overrideSuperIfExists('usage_tracker_zone_b_statefulset', if isZoneBEnabled then statefulSet.spec.template.spec.withTolerationsMixin($.newMimirMultiZoneToleration()) else {}),

  // Ensure all configured addresses are zonal ones.
  local usageTrackerZoneAConfigError = if isZoneAEnabled then $.validateMimirMultiZoneConfig(['usage_tracker_zone_a_statefulset']),
  assert usageTrackerZoneAConfigError == null : usageTrackerZoneAConfigError,

  local usageTrackerZoneBConfigError = if isZoneBEnabled then $.validateMimirMultiZoneConfig(['usage_tracker_zone_b_statefulset']),
  assert usageTrackerZoneBConfigError == null : usageTrackerZoneBConfigError,

  // Utility used to override a field only if exists in super.
  local overrideSuperIfExists(name, override) = if !( name in super) || super[name] == null || super[name] == {} then null else
    super[name] + override,
}
