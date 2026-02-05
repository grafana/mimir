{
  _config+:: {
    // Allow to configure whether the ruler should be deployed in single or multi-zone.
    // Multi-zone and single-zone can be enabled at the same time during migrations.
    single_zone_ruler_enabled: !$._config.multi_zone_ruler_enabled,
    multi_zone_ruler_enabled: $._config.multi_zone_read_path_enabled,

    // Controls whether the traffic should be routed to multi-zone ruler.
    // This setting can be used by downstream projects during migrations from single to multi-zone.
    multi_zone_ruler_routing_enabled: $._config.multi_zone_ruler_enabled,

    // When enabled, all ruler zones use aggregate metrics across all zones for autoscaling,
    // ensuring balanced replica counts. When disabled, each zone scales independently.
    // Defaults to multi_zone_ruler_enabled but can be overridden for progressive rollout.
    multi_zone_ruler_balanced_autoscaling_enabled: $._config.multi_zone_ruler_enabled,
  },

  local container = $.core.v1.container,
  local deployment = $.apps.v1.deployment,
  local service = $.core.v1.service,

  local isSingleZoneEnabled = $._config.ruler_enabled && $._config.single_zone_ruler_enabled,
  local isMultiZoneEnabled = $._config.ruler_enabled && $._config.multi_zone_ruler_enabled,
  local isZoneAEnabled = isMultiZoneEnabled && std.length($._config.multi_zone_availability_zones) >= 1,
  local isZoneBEnabled = isMultiZoneEnabled && std.length($._config.multi_zone_availability_zones) >= 2,
  local isZoneCEnabled = isMultiZoneEnabled && std.length($._config.multi_zone_availability_zones) >= 3,
  local isAutoscalingSingleZoneEnabled = isSingleZoneEnabled && $._config.autoscaling_ruler_enabled,
  local isAutoscalingZoneAEnabled = isZoneAEnabled && $._config.autoscaling_ruler_enabled,
  local isAutoscalingZoneBEnabled = isZoneBEnabled && $._config.autoscaling_ruler_enabled,
  local isAutoscalingZoneCEnabled = isZoneCEnabled && $._config.autoscaling_ruler_enabled,

  assert !isMultiZoneEnabled || $._config.ruler_remote_evaluation_enabled : 'ruler multi-zone deployment requires remote rule evaluations to be enabled',
  assert !isMultiZoneEnabled || $._config.multi_zone_memcached_enabled : 'ruler multi-zone deployment requires memcached multi-zone to be enabled',

  local rulerQueryFrontendClientZoneArgs(zone) = {
    'ruler.query-frontend.address': 'dns:///ruler-query-frontend-zone-%(zone)s.%(namespace)s.svc.%(cluster_domain)s:9095' % ($._config { zone: zone }),
  },

  ruler_zone_a_args:: $.ruler_args + $.ruler_storage_zone_a_caching_config + $.blocks_metadata_zone_a_caching_config + rulerQueryFrontendClientZoneArgs('a'),
  ruler_zone_b_args:: $.ruler_args + $.ruler_storage_zone_b_caching_config + $.blocks_metadata_zone_b_caching_config + rulerQueryFrontendClientZoneArgs('b'),
  ruler_zone_c_args:: $.ruler_args + $.ruler_storage_zone_c_caching_config + $.blocks_metadata_zone_c_caching_config + rulerQueryFrontendClientZoneArgs('c'),

  ruler_zone_a_env_map:: {},
  ruler_zone_b_env_map:: {},
  ruler_zone_c_env_map:: {},

  ruler_zone_a_node_affinity_matchers:: $.ruler_node_affinity_matchers + [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[0])],
  ruler_zone_b_node_affinity_matchers:: $.ruler_node_affinity_matchers + [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[1])],
  ruler_zone_c_node_affinity_matchers:: $.ruler_node_affinity_matchers + [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[2])],

  ruler_zone_a_container:: if !isZoneAEnabled then null else
    $.newRulerZoneContainer('a', $.ruler_zone_a_args, $.ruler_zone_a_env_map),

  ruler_zone_b_container:: if !isZoneBEnabled then null else
    $.newRulerZoneContainer('b', $.ruler_zone_b_args, $.ruler_zone_b_env_map),

  ruler_zone_c_container:: if !isZoneCEnabled then null else
    $.newRulerZoneContainer('c', $.ruler_zone_c_args, $.ruler_zone_c_env_map),

  ruler_zone_a_deployment: if !isZoneAEnabled then null else
    $.newRulerZoneDeployment('a', $.ruler_zone_a_container, $.ruler_zone_a_node_affinity_matchers) +
    (if !isAutoscalingZoneAEnabled then {} else $.removeReplicasFromSpec),

  ruler_zone_b_deployment: if !isZoneBEnabled then null else
    $.newRulerZoneDeployment('b', $.ruler_zone_b_container, $.ruler_zone_b_node_affinity_matchers) +
    (if !isAutoscalingZoneBEnabled then {} else $.removeReplicasFromSpec),

  ruler_zone_c_deployment: if !isZoneCEnabled then null else
    $.newRulerZoneDeployment('c', $.ruler_zone_c_container, $.ruler_zone_c_node_affinity_matchers) +
    (if !isAutoscalingZoneCEnabled then {} else $.removeReplicasFromSpec),

  ruler_zone_a_service: if !isZoneAEnabled then null else
    $.util.serviceFor($.ruler_zone_a_deployment, $._config.service_ignored_labels),

  ruler_zone_b_service: if !isZoneBEnabled then null else
    $.util.serviceFor($.ruler_zone_b_deployment, $._config.service_ignored_labels),

  ruler_zone_c_service: if !isZoneCEnabled then null else
    $.util.serviceFor($.ruler_zone_c_deployment, $._config.service_ignored_labels),

  ruler_zone_a_pdb: if !isZoneAEnabled then null else
    $.newMimirPdb('ruler-zone-a'),

  ruler_zone_b_pdb: if !isZoneBEnabled then null else
    $.newMimirPdb('ruler-zone-b'),

  ruler_zone_c_pdb: if !isZoneCEnabled then null else
    $.newMimirPdb('ruler-zone-c'),

  newRulerZoneContainer(zone, args, extraEnvVarMap={})::
    $.ruler_container +
    container.withArgs($.util.mapToFlags(args)) +
    (if std.length(extraEnvVarMap) > 0 then container.withEnvMixin(std.prune(extraEnvVarMap)) else {}),

  newRulerZoneDeployment(zone, container, nodeAffinityMatchers=[])::
    local name = 'ruler-zone-%s' % zone;

    $.newRulerDeployment(name, container, nodeAffinityMatchers) +
    deployment.mixin.spec.withReplicas(2) +
    deployment.spec.template.spec.withTolerationsMixin($.newMimirMultiZoneToleration()),

  // Ensure all configured addresses are zonal ones.
  local rulerMultiZoneConfigError = $.validateMimirMultiZoneConfig([
    'ruler_zone_a_deployment',
    'ruler_zone_b_deployment',
    'ruler_zone_c_deployment',
  ]),
  assert rulerMultiZoneConfigError == null : rulerMultiZoneConfigError,

  // Remove single-zone deployment when it's disabled.
  ruler_deployment: if !isSingleZoneEnabled then null else super.ruler_deployment,
  ruler_service: if !isSingleZoneEnabled then null else super.ruler_service,
  ruler_pdb: if !isSingleZoneEnabled then null else super.ruler_pdb,

  // Autoscaling.
  ruler_scaled_object:
    if !isAutoscalingSingleZoneEnabled then
      null
    else if isMultiZoneEnabled then
      // When both single-zone and multi-zone coexists, the single-zone scaling metrics shouldn't
      // match the multi-zone pods.
      $.newRulerScaledObject('ruler', extra_matchers='pod!~"ruler-zone.*"')
    else
      super.ruler_scaled_object,

  // When balanced autoscaling is enabled, all zones use aggregate metrics to ensure balanced replica counts.
  // The query_weight divides the aggregate metric by the number of zones so each zone scales to its fair share.
  // When disabled, each zone scales independently based on its own metrics.
  local numZones = std.length($._config.multi_zone_availability_zones),
  local rulerZoneExtraMatchers(zone) =
    if $._config.multi_zone_ruler_balanced_autoscaling_enabled
    then 'pod=~"ruler-zone-.*"'
    else 'pod=~"ruler-zone-%s.*"' % zone,
  local rulerZoneQueryWeight =
    if $._config.multi_zone_ruler_balanced_autoscaling_enabled
    then 1.0 / numZones
    else 1,

  ruler_zone_a_scaled_object: if !isAutoscalingZoneAEnabled then null else $.newRulerScaledObject('ruler-zone-a', rulerZoneExtraMatchers('a'), rulerZoneQueryWeight),
  ruler_zone_b_scaled_object: if !isAutoscalingZoneBEnabled then null else $.newRulerScaledObject('ruler-zone-b', rulerZoneExtraMatchers('b'), rulerZoneQueryWeight),
  ruler_zone_c_scaled_object: if !isAutoscalingZoneCEnabled then null else $.newRulerScaledObject('ruler-zone-c', rulerZoneExtraMatchers('c'), rulerZoneQueryWeight),
}
