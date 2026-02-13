{
  _config+:: {
    // Allow to configure whether the querier should be deployed in single or multi-zone.
    // Multi-zone and single-zone can be enabled at the same time during migrations.
    single_zone_querier_enabled: !$._config.multi_zone_querier_enabled,
    multi_zone_querier_enabled: $._config.multi_zone_read_path_enabled,
  },

  local container = $.core.v1.container,
  local deployment = $.apps.v1.deployment,
  local service = $.core.v1.service,

  local isSingleZoneEnabled = $._config.single_zone_querier_enabled,
  local isMultiZoneEnabled = $._config.multi_zone_querier_enabled,
  local isZoneAEnabled = isMultiZoneEnabled && std.length($._config.multi_zone_availability_zones) >= 1,
  local isZoneBEnabled = isMultiZoneEnabled && std.length($._config.multi_zone_availability_zones) >= 2,
  local isZoneCEnabled = isMultiZoneEnabled && std.length($._config.multi_zone_availability_zones) >= 3,
  local isAutoscalingSingleZoneEnabled = isSingleZoneEnabled && $._config.autoscaling_querier_enabled,
  local isAutoscalingZoneAEnabled = isZoneAEnabled && $._config.autoscaling_querier_enabled,
  local isAutoscalingZoneBEnabled = isZoneBEnabled && $._config.autoscaling_querier_enabled,
  local isAutoscalingZoneCEnabled = isZoneCEnabled && $._config.autoscaling_querier_enabled,

  assert !isMultiZoneEnabled || $._config.multi_zone_memcached_enabled : 'querier multi-zone deployment requires memcached multi-zone to be enabled',

  local querierZoneArgs(zone) = {
    // Prefer querying ingesters and store-gateways in the same zone, to reduce cross-AZ data transfer.
    'querier.prefer-availability-zones': 'zone-%s,zone-%s-backup' % [zone, zone],
  },

  querierClientZoneArgs(zone):: {
    // The querier runs on a dedicated ring per zone.
    'querier.ring.prefix': 'querier-zone-%s/' % zone,
  },

  querier_zone_a_args:: $.querier_args + $.querier_only_args + $.blocks_metadata_zone_a_caching_config + $.querySchedulerClientZoneArgs('a') + $.querierClientZoneArgs('a') + querierZoneArgs('a'),
  querier_zone_b_args:: $.querier_args + $.querier_only_args + $.blocks_metadata_zone_b_caching_config + $.querySchedulerClientZoneArgs('b') + $.querierClientZoneArgs('b') + querierZoneArgs('b'),
  querier_zone_c_args:: $.querier_args + $.querier_only_args + $.blocks_metadata_zone_c_caching_config + $.querySchedulerClientZoneArgs('c') + $.querierClientZoneArgs('c') + querierZoneArgs('c'),

  querier_zone_a_env_map:: {},
  querier_zone_b_env_map:: {},
  querier_zone_c_env_map:: {},

  querier_zone_a_node_affinity_matchers:: $.querier_node_affinity_matchers + [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[0])],
  querier_zone_b_node_affinity_matchers:: $.querier_node_affinity_matchers + [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[1])],
  querier_zone_c_node_affinity_matchers:: $.querier_node_affinity_matchers + [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[2])],

  querier_zone_a_container:: if !isZoneAEnabled then null else
    $.newQuerierZoneContainer('a', $.querier_zone_a_args, $.querier_zone_a_env_map),

  querier_zone_b_container:: if !isZoneBEnabled then null else
    $.newQuerierZoneContainer('b', $.querier_zone_b_args, $.querier_zone_b_env_map),

  querier_zone_c_container:: if !isZoneCEnabled then null else
    $.newQuerierZoneContainer('c', $.querier_zone_c_args, $.querier_zone_c_env_map),

  querier_zone_a_deployment: if !isZoneAEnabled then null else
    $.newQuerierZoneDeployment('a', $.querier_zone_a_container, $.querier_zone_a_node_affinity_matchers) +
    (if !isAutoscalingZoneAEnabled then {} else $.removeReplicasFromSpec),

  querier_zone_b_deployment: if !isZoneBEnabled then null else
    $.newQuerierZoneDeployment('b', $.querier_zone_b_container, $.querier_zone_b_node_affinity_matchers) +
    (if !isAutoscalingZoneBEnabled then {} else $.removeReplicasFromSpec),

  querier_zone_c_deployment: if !isZoneCEnabled then null else
    $.newQuerierZoneDeployment('c', $.querier_zone_c_container, $.querier_zone_c_node_affinity_matchers) +
    (if !isAutoscalingZoneCEnabled then {} else $.removeReplicasFromSpec),

  querier_zone_a_service: if !isZoneAEnabled then null else
    $.util.serviceFor($.querier_zone_a_deployment, $._config.service_ignored_labels),

  querier_zone_b_service: if !isZoneBEnabled then null else
    $.util.serviceFor($.querier_zone_b_deployment, $._config.service_ignored_labels),

  querier_zone_c_service: if !isZoneCEnabled then null else
    $.util.serviceFor($.querier_zone_c_deployment, $._config.service_ignored_labels),

  querier_zone_a_pdb: if !isZoneAEnabled then null else
    $.newMimirPdb('querier-zone-a'),

  querier_zone_b_pdb: if !isZoneBEnabled then null else
    $.newMimirPdb('querier-zone-b'),

  querier_zone_c_pdb: if !isZoneCEnabled then null else
    $.newMimirPdb('querier-zone-c'),

  newQuerierZoneContainer(zone, args, extraEnvVarMap={})::
    $.querier_container +
    container.withArgs($.util.mapToFlags(args)) +
    (if std.length(extraEnvVarMap) > 0 then container.withEnvMixin(std.prune(extraEnvVarMap)) else {}),

  newQuerierZoneDeployment(zone, container, nodeAffinityMatchers=[])::
    local name = 'querier-zone-%s' % zone;

    $.newQuerierDeployment(name, container, nodeAffinityMatchers) +
    deployment.mixin.spec.withReplicas(2) +
    deployment.spec.template.spec.withTolerationsMixin($.newMimirMultiZoneToleration()),

  // Ensure all configured addresses are zonal ones.
  local querierMultiZoneConfigError = $.validateMimirMultiZoneConfig([
    'querier_zone_a_deployment',
    'querier_zone_b_deployment',
    'querier_zone_c_deployment',
  ]),
  assert querierMultiZoneConfigError == null : querierMultiZoneConfigError,

  // Remove single-zone deployment when it's disabled.
  querier_deployment: if !isSingleZoneEnabled then null else super.querier_deployment,
  querier_service: if !isSingleZoneEnabled then null else super.querier_service,
  querier_pdb: if !isSingleZoneEnabled then null else super.querier_pdb,

  // Autoscaling.
  local querierScaledObject(name, querier_args, extra_matchers='') =
    $.newQuerierScaledObject(
      name=name,
      query_scheduler_container_name='query-scheduler',
      querier_container_name='querier',
      querier_max_concurrent=querier_args['querier.max-concurrent'],
      min_replicas=$._config.autoscaling_querier_min_replicas_per_zone,
      max_replicas=$._config.autoscaling_querier_max_replicas_per_zone,
      target_utilization=$._config.autoscaling_querier_target_utilization,
      ignore_null_values=$._config.autoscaling_querier_ignore_null_values,
      extra_matchers=extra_matchers,
    ),

  querier_scaled_object:
    if !isAutoscalingSingleZoneEnabled then
      null
    else if isMultiZoneEnabled then
      // When both single-zone and multi-zone coexists, the single-zone scaling metrics shouldn't
      // match the multi-zone pods.
      querierScaledObject('querier', $.querier_args, extra_matchers='pod!~"(querier|query-scheduler)-zone.*"')
    else
      super.querier_scaled_object,

  querier_zone_a_scaled_object: if !isAutoscalingZoneAEnabled then null else querierScaledObject('querier-zone-a', $.querier_zone_a_args, 'pod=~"(querier|query-scheduler)-zone-a.*"'),
  querier_zone_b_scaled_object: if !isAutoscalingZoneBEnabled then null else querierScaledObject('querier-zone-b', $.querier_zone_b_args, 'pod=~"(querier|query-scheduler)-zone-b.*"'),
  querier_zone_c_scaled_object: if !isAutoscalingZoneCEnabled then null else querierScaledObject('querier-zone-c', $.querier_zone_c_args, 'pod=~"(querier|query-scheduler)-zone-c.*"'),
}
