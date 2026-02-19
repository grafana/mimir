{
  _config+:: {
    // Allow to configure whether the distributor should be deployed in single or multi-zone.
    // Multi-zone and single-zone can be enabled at the same time during migrations.
    single_zone_distributor_enabled: !$._config.multi_zone_distributor_enabled,
    multi_zone_distributor_enabled: $._config.multi_zone_write_path_enabled,
    multi_zone_distributor_replicas: std.length($._config.multi_zone_availability_zones),
  },

  local container = $.core.v1.container,
  local deployment = $.apps.v1.deployment,
  local service = $.core.v1.service,

  local isSingleZoneEnabled = $._config.single_zone_distributor_enabled,
  local isMultiZoneEnabled = $._config.multi_zone_distributor_enabled,
  local isZoneAEnabled = isMultiZoneEnabled && std.length($._config.multi_zone_availability_zones) >= 1,
  local isZoneBEnabled = isMultiZoneEnabled && std.length($._config.multi_zone_availability_zones) >= 2,
  local isZoneCEnabled = isMultiZoneEnabled && std.length($._config.multi_zone_availability_zones) >= 3,
  local isAutoscalingEnabled = $._config.autoscaling_distributor_enabled,
  local isAutoscalingSingleZoneEnabled = isSingleZoneEnabled && isAutoscalingEnabled,
  local isAutoscalingZoneAEnabled = isZoneAEnabled && isAutoscalingEnabled,
  local isAutoscalingZoneBEnabled = isZoneBEnabled && isAutoscalingEnabled,
  local isAutoscalingZoneCEnabled = isZoneCEnabled && isAutoscalingEnabled,

  distributor_zone_a_args:: $.distributor_args,
  distributor_zone_b_args:: $.distributor_args,
  distributor_zone_c_args:: $.distributor_args,

  distributor_zone_a_env_map:: {},
  distributor_zone_b_env_map:: {},
  distributor_zone_c_env_map:: {},

  distributor_zone_a_node_affinity_matchers:: $.distributor_node_affinity_matchers + [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[0])],
  distributor_zone_b_node_affinity_matchers:: $.distributor_node_affinity_matchers + [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[1])],
  distributor_zone_c_node_affinity_matchers:: $.distributor_node_affinity_matchers + [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[2])],

  distributor_zone_a_container:: if !isZoneAEnabled then null else
    $.newDistributorZoneContainer('a', $.distributor_zone_a_args, $.distributor_zone_a_env_map),

  distributor_zone_b_container:: if !isZoneBEnabled then null else
    $.newDistributorZoneContainer('b', $.distributor_zone_b_args, $.distributor_zone_b_env_map),

  distributor_zone_c_container:: if !isZoneCEnabled then null else
    $.newDistributorZoneContainer('c', $.distributor_zone_c_args, $.distributor_zone_c_env_map),

  distributor_zone_a_deployment: if !isZoneAEnabled then null else
    $.newDistributorZoneDeployment('a', $.distributor_zone_a_container, $.distributor_zone_a_node_affinity_matchers) +
    (if !isAutoscalingZoneAEnabled then {} else $.removeReplicasFromSpec),

  distributor_zone_b_deployment: if !isZoneBEnabled then null else
    $.newDistributorZoneDeployment('b', $.distributor_zone_b_container, $.distributor_zone_b_node_affinity_matchers) +
    (if !isAutoscalingZoneBEnabled then {} else $.removeReplicasFromSpec),

  distributor_zone_c_deployment: if !isZoneCEnabled then null else
    $.newDistributorZoneDeployment('c', $.distributor_zone_c_container, $.distributor_zone_c_node_affinity_matchers) +
    (if !isAutoscalingZoneCEnabled then {} else $.removeReplicasFromSpec),

  distributor_zone_a_service: if !isZoneAEnabled then null else
    $.util.serviceFor($.distributor_zone_a_deployment, $._config.service_ignored_labels) +
    service.mixin.spec.withClusterIp('None'),

  distributor_zone_b_service: if !isZoneBEnabled then null else
    $.util.serviceFor($.distributor_zone_b_deployment, $._config.service_ignored_labels) +
    service.mixin.spec.withClusterIp('None'),

  distributor_zone_c_service: if !isZoneCEnabled then null else
    $.util.serviceFor($.distributor_zone_c_deployment, $._config.service_ignored_labels) +
    service.mixin.spec.withClusterIp('None'),

  distributor_zone_a_pdb: if !isZoneAEnabled then null else
    $.newMimirPdb('distributor-zone-a'),

  distributor_zone_b_pdb: if !isZoneBEnabled then null else
    $.newMimirPdb('distributor-zone-b'),

  distributor_zone_c_pdb: if !isZoneCEnabled then null else
    $.newMimirPdb('distributor-zone-c'),

  newDistributorZoneContainer(zone, args, extraEnvVarMap={})::
    $.distributor_container +
    container.withArgs($.util.mapToFlags(args)) +
    (if std.length(extraEnvVarMap) > 0 then container.withEnvMap(std.prune(extraEnvVarMap)) else {}),

  newDistributorZoneDeployment(zone, container, nodeAffinityMatchers=[])::
    local name = 'distributor-zone-%s' % zone;

    $.newDistributorDeployment(name, container, nodeAffinityMatchers) +
    deployment.mixin.spec.withReplicas(std.ceil($._config.multi_zone_distributor_replicas / std.length($._config.multi_zone_availability_zones))) +
    deployment.spec.template.spec.withTolerationsMixin($.newMimirMultiZoneToleration()),

  // Ensure all configured addresses are zonal ones.
  local distributorMultiZoneConfigError = $.validateMimirMultiZoneConfig([
    'distributor_zone_a_deployment',
    'distributor_zone_b_deployment',
    'distributor_zone_c_deployment',
  ]),
  assert distributorMultiZoneConfigError == null : distributorMultiZoneConfigError,

  // Remove single-zone deployment when multi-zone is enabled.
  distributor_deployment: if !isSingleZoneEnabled then null else
    super.distributor_deployment + (if !isAutoscalingSingleZoneEnabled then {} else $.removeReplicasFromSpec),
  distributor_service: if !isSingleZoneEnabled then null else super.distributor_service,
  distributor_pdb: if !isSingleZoneEnabled then null else super.distributor_pdb,

  // Autoscaling.
  distributor_scaled_object:
    if !isAutoscalingSingleZoneEnabled then
      null
    else if isMultiZoneEnabled then
      // When both single-zone and multi-zone coexists, the single-zone scaling metrics shouldn't
      // match the multi-zone pods.
      $.newDistributorScaledObject('distributor', extra_matchers='pod!~"distributor-zone.*"')
    else
      super.distributor_scaled_object,

  distributor_zone_a_scaled_object: if !isAutoscalingZoneAEnabled then null else $.newDistributorScaledObject('distributor-zone-a', 'pod=~"distributor-zone-a.*"'),
  distributor_zone_b_scaled_object: if !isAutoscalingZoneBEnabled then null else $.newDistributorScaledObject('distributor-zone-b', 'pod=~"distributor-zone-b.*"'),
  distributor_zone_c_scaled_object: if !isAutoscalingZoneCEnabled then null else $.newDistributorScaledObject('distributor-zone-c', 'pod=~"distributor-zone-c.*"'),
}
