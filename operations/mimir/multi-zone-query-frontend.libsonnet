{
  _config+:: {
    // Allow to configure whether the query-frontend should be deployed in single or multi-zone.
    // Multi-zone and single-zone can be enabled at the same time during migrations.
    single_zone_query_frontend_enabled: !$._config.multi_zone_query_frontend_enabled,
    multi_zone_query_frontend_enabled: $._config.multi_zone_read_path_enabled,

    // Controls whether the traffic should be routed to multi-zone query-frontend.
    // This setting can be used by downstream projects during migrations from single to multi-zone.
    multi_zone_query_frontend_routing_enabled: $._config.multi_zone_query_frontend_enabled,
  },

  local container = $.core.v1.container,
  local deployment = $.apps.v1.deployment,
  local service = $.core.v1.service,

  local isSingleZoneEnabled = $._config.single_zone_query_frontend_enabled,
  local isMultiZoneEnabled = $._config.multi_zone_query_frontend_enabled,
  local isZoneAEnabled = isMultiZoneEnabled && std.length($._config.multi_zone_availability_zones) >= 1,
  local isZoneBEnabled = isMultiZoneEnabled && std.length($._config.multi_zone_availability_zones) >= 2,
  local isZoneCEnabled = isMultiZoneEnabled && std.length($._config.multi_zone_availability_zones) >= 3,
  local isAutoscalingSingleZoneEnabled = isSingleZoneEnabled && $._config.autoscaling_query_frontend_enabled,
  local isAutoscalingZoneAEnabled = isZoneAEnabled && $._config.autoscaling_query_frontend_enabled,
  local isAutoscalingZoneBEnabled = isZoneBEnabled && $._config.autoscaling_query_frontend_enabled,
  local isAutoscalingZoneCEnabled = isZoneCEnabled && $._config.autoscaling_query_frontend_enabled,

  assert !isMultiZoneEnabled || $._config.multi_zone_memcached_enabled : 'query-frontend multi-zone deployment requires memcached multi-zone to be enabled',

  query_frontend_zone_a_args:: $.query_frontend_args + $.query_frontend_only_args + $.query_frontend_zone_a_caching_config + $.querySchedulerClientZoneArgs('a') + $.querierClientZoneArgs('a') + $.rangeVectorSplittingZoneCachingConfig('a'),
  query_frontend_zone_b_args:: $.query_frontend_args + $.query_frontend_only_args + $.query_frontend_zone_b_caching_config + $.querySchedulerClientZoneArgs('b') + $.querierClientZoneArgs('b') + $.rangeVectorSplittingZoneCachingConfig('b'),
  query_frontend_zone_c_args:: $.query_frontend_args + $.query_frontend_only_args + $.query_frontend_zone_c_caching_config + $.querySchedulerClientZoneArgs('c') + $.querierClientZoneArgs('c') + $.rangeVectorSplittingZoneCachingConfig('c'),

  query_frontend_zone_a_env_map:: {},
  query_frontend_zone_b_env_map:: {},
  query_frontend_zone_c_env_map:: {},

  query_frontend_zone_a_node_affinity_matchers:: $.query_frontend_node_affinity_matchers + [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[0])],
  query_frontend_zone_b_node_affinity_matchers:: $.query_frontend_node_affinity_matchers + [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[1])],
  query_frontend_zone_c_node_affinity_matchers:: $.query_frontend_node_affinity_matchers + [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[2])],

  query_frontend_zone_a_container:: if !isZoneAEnabled then null else
    $.newQueryFrontendZoneContainer('a', $.query_frontend_zone_a_args, $.query_frontend_zone_a_env_map),

  query_frontend_zone_b_container:: if !isZoneBEnabled then null else
    $.newQueryFrontendZoneContainer('b', $.query_frontend_zone_b_args, $.query_frontend_zone_b_env_map),

  query_frontend_zone_c_container:: if !isZoneCEnabled then null else
    $.newQueryFrontendZoneContainer('c', $.query_frontend_zone_c_args, $.query_frontend_zone_c_env_map),

  query_frontend_zone_a_deployment: if !isZoneAEnabled then null else
    $.newQueryFrontendZoneDeployment('a', $.query_frontend_zone_a_container, $.query_frontend_zone_a_node_affinity_matchers) +
    (if !isAutoscalingZoneAEnabled then {} else $.removeReplicasFromSpec),

  query_frontend_zone_b_deployment: if !isZoneBEnabled then null else
    $.newQueryFrontendZoneDeployment('b', $.query_frontend_zone_b_container, $.query_frontend_zone_b_node_affinity_matchers) +
    (if !isAutoscalingZoneBEnabled then {} else $.removeReplicasFromSpec),

  query_frontend_zone_c_deployment: if !isZoneCEnabled then null else
    $.newQueryFrontendZoneDeployment('c', $.query_frontend_zone_c_container, $.query_frontend_zone_c_node_affinity_matchers) +
    (if !isAutoscalingZoneCEnabled then {} else $.removeReplicasFromSpec),

  query_frontend_zone_a_service: if !isZoneAEnabled then null else
    $.util.serviceFor($.query_frontend_zone_a_deployment, $._config.service_ignored_labels),

  query_frontend_zone_b_service: if !isZoneBEnabled then null else
    $.util.serviceFor($.query_frontend_zone_b_deployment, $._config.service_ignored_labels),

  query_frontend_zone_c_service: if !isZoneCEnabled then null else
    $.util.serviceFor($.query_frontend_zone_c_deployment, $._config.service_ignored_labels),

  query_frontend_zone_a_pdb: if !isZoneAEnabled then null else
    $.newMimirPdb('query-frontend-zone-a'),

  query_frontend_zone_b_pdb: if !isZoneBEnabled then null else
    $.newMimirPdb('query-frontend-zone-b'),

  query_frontend_zone_c_pdb: if !isZoneCEnabled then null else
    $.newMimirPdb('query-frontend-zone-c'),

  newQueryFrontendZoneContainer(zone, args, extraEnvVarMap={})::
    $.query_frontend_container +
    container.withArgs($.util.mapToFlags(args)) +
    (if std.length(extraEnvVarMap) > 0 then container.withEnvMixin(std.prune(extraEnvVarMap)) else {}),

  newQueryFrontendZoneDeployment(zone, container, nodeAffinityMatchers=[])::
    local name = 'query-frontend-zone-%s' % zone;

    $.newQueryFrontendDeployment(name, container, nodeAffinityMatchers) +
    deployment.spec.template.spec.withTolerationsMixin($.newMimirMultiZoneToleration()),

  // Ensure all configured addresses are zonal ones.
  local queryFrontendMultiZoneConfigError = $.validateMimirMultiZoneConfig([
    'query_frontend_zone_a_deployment',
    'query_frontend_zone_b_deployment',
    'query_frontend_zone_c_deployment',
  ]),
  assert queryFrontendMultiZoneConfigError == null : queryFrontendMultiZoneConfigError,

  // Remove single-zone deployment when it's disabled.
  query_frontend_deployment: if !isSingleZoneEnabled then null else super.query_frontend_deployment,
  query_frontend_service: if !isSingleZoneEnabled then null else super.query_frontend_service,
  query_frontend_pdb: if !isSingleZoneEnabled then null else super.query_frontend_pdb,

  // Autoscaling.
  query_frontend_scaled_object:
    if !isAutoscalingSingleZoneEnabled then
      null
    else if isMultiZoneEnabled then
      // When both single-zone and multi-zone coexists, the single-zone scaling metrics shouldn't
      // match the multi-zone pods.
      $.newQueryFrontendScaledObject('query-frontend', extra_matchers='pod!~"query-frontend-zone.*"')
    else
      super.query_frontend_scaled_object,

  query_frontend_zone_a_scaled_object: if !isAutoscalingZoneAEnabled then null else $.newQueryFrontendScaledObject('query-frontend-zone-a', 'pod=~"query-frontend-zone-a.*"'),
  query_frontend_zone_b_scaled_object: if !isAutoscalingZoneBEnabled then null else $.newQueryFrontendScaledObject('query-frontend-zone-b', 'pod=~"query-frontend-zone-b.*"'),
  query_frontend_zone_c_scaled_object: if !isAutoscalingZoneCEnabled then null else $.newQueryFrontendScaledObject('query-frontend-zone-c', 'pod=~"query-frontend-zone-c.*"'),
}
