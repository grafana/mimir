{
  _config+:: {
    // Allow to configure whether the cortex-gw should be deployed in single or multi-zone.
    // Multi-zone and single-zone can be enabled at the same time during migrations.
    single_zone_distributor_enabled: !$._config.multi_zone_distributor_enabled,
    multi_zone_distributor_enabled: false,
    multi_zone_availability_zones: [],
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
    $.newDistributorZoneDeployment('a', $.distributor_zone_a_container, $.distributor_zone_a_node_affinity_matchers),

  distributor_zone_b_deployment: if !isZoneBEnabled then null else
    $.newDistributorZoneDeployment('b', $.distributor_zone_b_container, $.distributor_zone_b_node_affinity_matchers),

  distributor_zone_c_deployment: if !isZoneCEnabled then null else
    $.newDistributorZoneDeployment('c', $.distributor_zone_c_container, $.distributor_zone_c_node_affinity_matchers),

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
    (if std.length(extraEnvVarMap) > 0 then container.withEnvMixin(std.prune(extraEnvVarMap)) else {}),

  newDistributorZoneDeployment(zone, container, nodeAffinityMatchers=[])::
    local name = 'distributor-zone-%s' % zone;

    $.newDistributorDeployment(name, container, nodeAffinityMatchers) +
    deployment.mixin.spec.withReplicas(std.ceil($._config.multi_zone_distributor_replicas / std.length($._config.multi_zone_availability_zones))) +
    deployment.spec.template.spec.withTolerationsMixin([
      $.core.v1.toleration.withKey('topology') +
      $.core.v1.toleration.withOperator('Equal') +
      $.core.v1.toleration.withValue('multi-az') +
      $.core.v1.toleration.withEffect('NoSchedule'),
    ]),

  // Remove single-zone deployment when multi-zone is enabled.
  distributor_deployment: if !isSingleZoneEnabled then null else super.distributor_deployment,
  distributor_service: if !isSingleZoneEnabled then null else super.distributor_service,
  distributor_pdb: if !isSingleZoneEnabled then null else super.distributor_pdb,
  distributor_scaled_object: if !isSingleZoneEnabled then null else super.distributor_scaled_object,
}
