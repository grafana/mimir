{
  local container = $.core.v1.container,
  local deployment = $.apps.v1.deployment,
  local service = $.core.v1.service,

  _config+:: {
    // Allow to configure whether the query-tee should be deployed in single or multi-zone.
    // Multi-zone and single-zone can be enabled at the same time during migrations.
    single_zone_query_tee_enabled: !$._config.multi_zone_query_tee_enabled && $._config.query_tee_enabled,
    multi_zone_query_tee_enabled: $._config.multi_zone_read_path_enabled && $._config.query_tee_enabled,
  },

  local isSingleZoneEnabled = $._config.single_zone_query_tee_enabled,
  local isMultiZoneEnabled = $._config.multi_zone_query_tee_enabled,
  local isZoneAEnabled = isMultiZoneEnabled && std.length($._config.multi_zone_availability_zones) >= 1,
  local isZoneBEnabled = isMultiZoneEnabled && std.length($._config.multi_zone_availability_zones) >= 2,
  local isZoneCEnabled = isMultiZoneEnabled && std.length($._config.multi_zone_availability_zones) >= 3,

  query_tee_zone_a_node_affinity_matchers:: [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[0])],
  query_tee_zone_b_node_affinity_matchers:: [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[1])],
  query_tee_zone_c_node_affinity_matchers:: [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[2])],

  query_tee_zone_a_args:: $.query_tee_args,
  query_tee_zone_b_args:: $.query_tee_args,
  query_tee_zone_c_args:: $.query_tee_args,

  query_tee_zone_a_container:: if !isZoneAEnabled then null else
    $.newQueryTeeZoneContainer('a', $.query_tee_zone_a_args),

  query_tee_zone_b_container:: if !isZoneBEnabled then null else
    $.newQueryTeeZoneContainer('b', $.query_tee_zone_b_args),

  query_tee_zone_c_container:: if !isZoneCEnabled then null else
    $.newQueryTeeZoneContainer('c', $.query_tee_zone_c_args),

  query_tee_zone_a_deployment: if !isZoneAEnabled then null else
    $.newQueryTeeZoneDeployment('a', $.query_tee_zone_a_container, $.query_tee_zone_a_node_affinity_matchers),

  query_tee_zone_b_deployment: if !isZoneBEnabled then null else
    $.newQueryTeeZoneDeployment('b', $.query_tee_zone_b_container, $.query_tee_zone_b_node_affinity_matchers),

  query_tee_zone_c_deployment: if !isZoneCEnabled then null else
    $.newQueryTeeZoneDeployment('c', $.query_tee_zone_c_container, $.query_tee_zone_c_node_affinity_matchers),

  query_tee_zone_a_service: if !isZoneAEnabled then null else
    $.util.serviceFor($.query_tee_zone_a_deployment),

  query_tee_zone_b_service: if !isZoneBEnabled then null else
    $.util.serviceFor($.query_tee_zone_b_deployment),

  query_tee_zone_c_service: if !isZoneCEnabled then null else
    $.util.serviceFor($.query_tee_zone_c_deployment),

  newQueryTeeZoneContainer(zone, args)::
    $.query_tee_container +
    container.withArgs($.util.mapToFlags(args)),

  newQueryTeeZoneDeployment(zone, container, nodeAffinityMatchers=[])::
    $.newQueryTeeDeployment('query-tee-zone-%s' % zone, container, nodeAffinityMatchers) +
    deployment.mixin.spec.withReplicas(2) +
    deployment.spec.template.spec.withTolerationsMixin($.newMimirMultiZoneToleration()),

  query_tee_deployment: if !isSingleZoneEnabled then null else super.query_tee_deployment,
  query_tee_service: if !isSingleZoneEnabled then null else super.query_tee_service,
}
