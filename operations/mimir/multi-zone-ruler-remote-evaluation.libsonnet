{
  _config+:: {
    // Allow to configure whether the ruler's remote evaluation stack should be deployed in single or multi-zone.
    // Multi-zone and single-zone can be enabled at the same time during migrations.
    single_zone_ruler_remote_evaluation_enabled: !$._config.multi_zone_ruler_remote_evaluation_enabled,
    multi_zone_ruler_remote_evaluation_enabled: $._config.multi_zone_read_path_enabled,
  },

  local container = $.core.v1.container,
  local deployment = $.apps.v1.deployment,
  local service = $.core.v1.service,

  local isSingleZoneEnabled = $._config.single_zone_ruler_remote_evaluation_enabled && $._config.ruler_remote_evaluation_enabled,
  local isMultiZoneEnabled = $._config.multi_zone_ruler_remote_evaluation_enabled && $._config.ruler_remote_evaluation_enabled,
  local isZoneAEnabled = isMultiZoneEnabled && std.length($._config.multi_zone_availability_zones) >= 1,
  local isZoneBEnabled = isMultiZoneEnabled && std.length($._config.multi_zone_availability_zones) >= 2,
  local isZoneCEnabled = isMultiZoneEnabled && std.length($._config.multi_zone_availability_zones) >= 3,

  local isBackupAMultiAZEnabled = $._config.multi_zone_store_gateway_zone_a_backup_multi_az_enabled && std.length($._config.multi_zone_availability_zones) >= 1,
  local isBackupBMultiAZEnabled = $._config.multi_zone_store_gateway_zone_b_backup_multi_az_enabled && std.length($._config.multi_zone_availability_zones) >= 2,

  assert !isMultiZoneEnabled || $._config.query_scheduler_service_discovery_mode == 'ring' : 'ruler-query-scheduler multi-zone deployment requires service discovery mode to be set to "ring"',
  assert !isMultiZoneEnabled || $._config.multi_zone_memcached_enabled : 'ruler remote evaluation multi-zone deployment requires memcached multi-zone to be enabled',

  //
  // Ruler-querier
  //

  local isRulerQuerierAutoscalingSingleZoneEnabled = isSingleZoneEnabled && $._config.autoscaling_ruler_querier_enabled,
  local isRulerQuerierAutoscalingZoneAEnabled = isZoneAEnabled && $._config.autoscaling_ruler_querier_enabled,
  local isRulerQuerierAutoscalingZoneBEnabled = isZoneBEnabled && $._config.autoscaling_ruler_querier_enabled,
  local isRulerQuerierAutoscalingZoneCEnabled = isZoneCEnabled && $._config.autoscaling_ruler_querier_enabled,

  local rulerQuerierZoneArgs(zone) = {
    // Prefer querying ingesters and store-gateways in the same zone, to reduce cross-AZ data transfer.
    'querier.prefer-availability-zones': 'zone-%s' % zone +
                                         if zone == 'a' && isBackupAMultiAZEnabled then ',zone-a-backup'
                                         else if zone == 'b' && isBackupBMultiAZEnabled then ',zone-b-backup'
                                         else '',
  },

  rulerQuerierClientZoneArgs(zone):: {
    // The ruler-querier runs on a dedicated ring per zone.
    'querier.ring.prefix': 'ruler-querier-zone-%s/' % zone,
  },

  ruler_querier_zone_a_args:: $.ruler_querier_args + $.blocks_metadata_zone_a_caching_config + $.rulerQuerySchedulerClientZoneArgs('a') + $.rulerQuerierClientZoneArgs('a') + rulerQuerierZoneArgs('a'),
  ruler_querier_zone_b_args:: $.ruler_querier_args + $.blocks_metadata_zone_b_caching_config + $.rulerQuerySchedulerClientZoneArgs('b') + $.rulerQuerierClientZoneArgs('b') + rulerQuerierZoneArgs('b'),
  ruler_querier_zone_c_args:: $.ruler_querier_args + $.blocks_metadata_zone_c_caching_config + $.rulerQuerySchedulerClientZoneArgs('c') + $.rulerQuerierClientZoneArgs('c') + rulerQuerierZoneArgs('c'),

  ruler_querier_zone_a_env_map:: {},
  ruler_querier_zone_b_env_map:: {},
  ruler_querier_zone_c_env_map:: {},

  ruler_querier_zone_a_node_affinity_matchers:: $.ruler_querier_node_affinity_matchers + [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[0])],
  ruler_querier_zone_b_node_affinity_matchers:: $.ruler_querier_node_affinity_matchers + [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[1])],
  ruler_querier_zone_c_node_affinity_matchers:: $.ruler_querier_node_affinity_matchers + [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[2])],

  ruler_querier_zone_a_container:: if !isZoneAEnabled then null else
    $.newRulerQuerierZoneContainer('a', $.ruler_querier_zone_a_args, $.ruler_querier_zone_a_env_map),

  ruler_querier_zone_b_container:: if !isZoneBEnabled then null else
    $.newRulerQuerierZoneContainer('b', $.ruler_querier_zone_b_args, $.ruler_querier_zone_b_env_map),

  ruler_querier_zone_c_container:: if !isZoneCEnabled then null else
    $.newRulerQuerierZoneContainer('c', $.ruler_querier_zone_c_args, $.ruler_querier_zone_c_env_map),

  ruler_querier_zone_a_deployment: if !isZoneAEnabled then null else
    $.newRulerQuerierZoneDeployment('a', $.ruler_querier_zone_a_container, $.ruler_querier_zone_a_node_affinity_matchers) +
    (if !isRulerQuerierAutoscalingZoneAEnabled then {} else $.removeReplicasFromSpec),

  ruler_querier_zone_b_deployment: if !isZoneBEnabled then null else
    $.newRulerQuerierZoneDeployment('b', $.ruler_querier_zone_b_container, $.ruler_querier_zone_b_node_affinity_matchers) +
    (if !isRulerQuerierAutoscalingZoneBEnabled then {} else $.removeReplicasFromSpec),

  ruler_querier_zone_c_deployment: if !isZoneCEnabled then null else
    $.newRulerQuerierZoneDeployment('c', $.ruler_querier_zone_c_container, $.ruler_querier_zone_c_node_affinity_matchers) +
    (if !isRulerQuerierAutoscalingZoneCEnabled then {} else $.removeReplicasFromSpec),

  ruler_querier_zone_a_service: if !isZoneAEnabled then null else
    $.util.serviceFor($.ruler_querier_zone_a_deployment, $._config.service_ignored_labels),

  ruler_querier_zone_b_service: if !isZoneBEnabled then null else
    $.util.serviceFor($.ruler_querier_zone_b_deployment, $._config.service_ignored_labels),

  ruler_querier_zone_c_service: if !isZoneCEnabled then null else
    $.util.serviceFor($.ruler_querier_zone_c_deployment, $._config.service_ignored_labels),

  ruler_querier_zone_a_pdb: if !isZoneAEnabled then null else
    $.newMimirPdb('ruler-querier-zone-a'),

  ruler_querier_zone_b_pdb: if !isZoneBEnabled then null else
    $.newMimirPdb('ruler-querier-zone-b'),

  ruler_querier_zone_c_pdb: if !isZoneCEnabled then null else
    $.newMimirPdb('ruler-querier-zone-c'),

  newRulerQuerierZoneContainer(zone, args, extraEnvVarMap={})::
    $.ruler_querier_container +
    container.withArgs($.util.mapToFlags(args)) +
    (if std.length(extraEnvVarMap) > 0 then container.withEnvMixin(std.prune(extraEnvVarMap)) else {}),

  newRulerQuerierZoneDeployment(zone, container, nodeAffinityMatchers=[])::
    local name = 'ruler-querier-zone-%s' % zone;

    $.newQuerierDeployment(name, container, nodeAffinityMatchers) +
    deployment.mixin.spec.withReplicas(2) +
    deployment.spec.template.spec.withTolerationsMixin($.newMimirMultiZoneToleration()),

  // Ensure all configured addresses are zonal ones.
  local rulerQuerierMultiZoneConfigError = $.validateMimirMultiZoneConfig([
    'ruler_querier_zone_a_deployment',
    'ruler_querier_zone_b_deployment',
    'ruler_querier_zone_c_deployment',
  ]),
  assert rulerQuerierMultiZoneConfigError == null : rulerQuerierMultiZoneConfigError,

  // Remove single-zone deployment when it's disabled.
  ruler_querier_deployment: if !isSingleZoneEnabled then null else super.ruler_querier_deployment,
  ruler_querier_service: if !isSingleZoneEnabled then null else super.ruler_querier_service,
  ruler_querier_pdb: if !isSingleZoneEnabled then null else super.ruler_querier_pdb,

  // Autoscaling.
  ruler_querier_scaled_object:
    if !isRulerQuerierAutoscalingSingleZoneEnabled then
      null
    else if isMultiZoneEnabled then
      // When both single-zone and multi-zone coexists, the single-zone scaling metrics shouldn't
      // match the multi-zone pods.
      $.newRulerQuerierScaledObject('ruler-querier', $.ruler_querier_args, extra_matchers='pod!~"(ruler-querier|ruler-query-scheduler)-zone.*"')
    else
      super.ruler_querier_scaled_object,

  ruler_querier_zone_a_scaled_object: if !isRulerQuerierAutoscalingZoneAEnabled then null else $.newRulerQuerierScaledObject('ruler-querier-zone-a', $.ruler_querier_zone_a_args, 'pod=~"(ruler-querier|ruler-query-scheduler)-zone-a.*"'),
  ruler_querier_zone_b_scaled_object: if !isRulerQuerierAutoscalingZoneBEnabled then null else $.newRulerQuerierScaledObject('ruler-querier-zone-b', $.ruler_querier_zone_b_args, 'pod=~"(ruler-querier|ruler-query-scheduler)-zone-b.*"'),
  ruler_querier_zone_c_scaled_object: if !isRulerQuerierAutoscalingZoneCEnabled then null else $.newRulerQuerierScaledObject('ruler-querier-zone-c', $.ruler_querier_zone_c_args, 'pod=~"(ruler-querier|ruler-query-scheduler)-zone-c.*"'),

  //
  // Ruler-query-frontend
  //

  local isRulerQueryFrontendAutoscalingSingleZoneEnabled = isSingleZoneEnabled && $._config.autoscaling_ruler_query_frontend_enabled,
  local isRulerQueryFrontendAutoscalingZoneAEnabled = isZoneAEnabled && $._config.autoscaling_ruler_query_frontend_enabled,
  local isRulerQueryFrontendAutoscalingZoneBEnabled = isZoneBEnabled && $._config.autoscaling_ruler_query_frontend_enabled,
  local isRulerQueryFrontendAutoscalingZoneCEnabled = isZoneCEnabled && $._config.autoscaling_ruler_query_frontend_enabled,

  ruler_query_frontend_zone_a_args:: $.ruler_query_frontend_args + $.query_frontend_zone_a_caching_config + $.rulerQuerySchedulerClientZoneArgs('a') + $.rulerQuerierClientZoneArgs('a'),
  ruler_query_frontend_zone_b_args:: $.ruler_query_frontend_args + $.query_frontend_zone_b_caching_config + $.rulerQuerySchedulerClientZoneArgs('b') + $.rulerQuerierClientZoneArgs('b'),
  ruler_query_frontend_zone_c_args:: $.ruler_query_frontend_args + $.query_frontend_zone_c_caching_config + $.rulerQuerySchedulerClientZoneArgs('c') + $.rulerQuerierClientZoneArgs('c'),

  ruler_query_frontend_zone_a_env_map:: {},
  ruler_query_frontend_zone_b_env_map:: {},
  ruler_query_frontend_zone_c_env_map:: {},

  ruler_query_frontend_zone_a_node_affinity_matchers:: $.ruler_query_frontend_node_affinity_matchers + [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[0])],
  ruler_query_frontend_zone_b_node_affinity_matchers:: $.ruler_query_frontend_node_affinity_matchers + [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[1])],
  ruler_query_frontend_zone_c_node_affinity_matchers:: $.ruler_query_frontend_node_affinity_matchers + [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[2])],

  ruler_query_frontend_zone_a_container:: if !isZoneAEnabled then null else
    $.newRulerQueryFrontendZoneContainer('a', $.ruler_query_frontend_zone_a_args, $.ruler_query_frontend_zone_a_env_map),

  ruler_query_frontend_zone_b_container:: if !isZoneBEnabled then null else
    $.newRulerQueryFrontendZoneContainer('b', $.ruler_query_frontend_zone_b_args, $.ruler_query_frontend_zone_b_env_map),

  ruler_query_frontend_zone_c_container:: if !isZoneCEnabled then null else
    $.newRulerQueryFrontendZoneContainer('c', $.ruler_query_frontend_zone_c_args, $.ruler_query_frontend_zone_c_env_map),

  ruler_query_frontend_zone_a_deployment: if !isZoneAEnabled then null else
    $.newRulerQueryFrontendZoneDeployment('a', $.ruler_query_frontend_zone_a_container, $.ruler_query_frontend_zone_a_node_affinity_matchers) +
    (if !isRulerQueryFrontendAutoscalingZoneAEnabled then {} else $.removeReplicasFromSpec),

  ruler_query_frontend_zone_b_deployment: if !isZoneBEnabled then null else
    $.newRulerQueryFrontendZoneDeployment('b', $.ruler_query_frontend_zone_b_container, $.ruler_query_frontend_zone_b_node_affinity_matchers) +
    (if !isRulerQueryFrontendAutoscalingZoneBEnabled then {} else $.removeReplicasFromSpec),

  ruler_query_frontend_zone_c_deployment: if !isZoneCEnabled then null else
    $.newRulerQueryFrontendZoneDeployment('c', $.ruler_query_frontend_zone_c_container, $.ruler_query_frontend_zone_c_node_affinity_matchers) +
    (if !isRulerQueryFrontendAutoscalingZoneCEnabled then {} else $.removeReplicasFromSpec),

  ruler_query_frontend_zone_a_service: if !isZoneAEnabled then null else
    $.newRulerQueryFrontendZoneService($.ruler_query_frontend_zone_a_deployment),

  ruler_query_frontend_zone_b_service: if !isZoneBEnabled then null else
    $.newRulerQueryFrontendZoneService($.ruler_query_frontend_zone_b_deployment),

  ruler_query_frontend_zone_c_service: if !isZoneCEnabled then null else
    $.newRulerQueryFrontendZoneService($.ruler_query_frontend_zone_c_deployment),

  ruler_query_frontend_zone_a_pdb: if !isZoneAEnabled then null else
    $.newMimirPdb('ruler-query-frontend-zone-a'),

  ruler_query_frontend_zone_b_pdb: if !isZoneBEnabled then null else
    $.newMimirPdb('ruler-query-frontend-zone-b'),

  ruler_query_frontend_zone_c_pdb: if !isZoneCEnabled then null else
    $.newMimirPdb('ruler-query-frontend-zone-c'),

  newRulerQueryFrontendZoneContainer(zone, args, extraEnvVarMap={})::
    $.ruler_query_frontend_container +
    container.withArgs($.util.mapToFlags(args)) +
    (if std.length(extraEnvVarMap) > 0 then container.withEnvMixin(std.prune(extraEnvVarMap)) else {}),

  newRulerQueryFrontendZoneDeployment(zone, container, nodeAffinityMatchers=[])::
    local name = 'ruler-query-frontend-zone-%s' % zone;

    $.newQueryFrontendDeployment(name, container, nodeAffinityMatchers) +
    deployment.spec.template.spec.withTolerationsMixin($.newMimirMultiZoneToleration()),

  newRulerQueryFrontendZoneService(deployment)::
    $.util.serviceFor(deployment, $._config.service_ignored_labels) +
    // Note: We use a headless service because the ruler uses gRPC load balancing.
    service.mixin.spec.withClusterIp('None'),

  // Ensure all configured addresses are zonal ones.
  local rulerQueryFrontendMultiZoneConfigError = $.validateMimirMultiZoneConfig([
    'ruler_query_frontend_zone_a_deployment',
    'ruler_query_frontend_zone_b_deployment',
    'ruler_query_frontend_zone_c_deployment',
  ]),
  assert rulerQueryFrontendMultiZoneConfigError == null : rulerQueryFrontendMultiZoneConfigError,

  // Remove single-zone deployment when it's disabled.
  ruler_query_frontend_deployment: if !isSingleZoneEnabled then null else super.ruler_query_frontend_deployment,
  ruler_query_frontend_service: if !isSingleZoneEnabled then null else super.ruler_query_frontend_service,
  ruler_query_frontend_pdb: if !isSingleZoneEnabled then null else super.ruler_query_frontend_pdb,

  // Autoscaling.
  ruler_query_frontend_scaled_object:
    if !isRulerQueryFrontendAutoscalingSingleZoneEnabled then
      null
    else if isMultiZoneEnabled then
      // When both single-zone and multi-zone coexists, the single-zone scaling metrics shouldn't
      // match the multi-zone pods.
      $.newRulerQueryFrontendScaledObject('ruler-query-frontend', extra_matchers='pod!~"ruler-query-frontend-zone.*"')
    else
      super.ruler_query_frontend_scaled_object,

  ruler_query_frontend_zone_a_scaled_object: if !isRulerQueryFrontendAutoscalingZoneAEnabled then null else $.newRulerQueryFrontendScaledObject('ruler-query-frontend-zone-a', 'pod=~"ruler-query-frontend-zone-a.*"'),
  ruler_query_frontend_zone_b_scaled_object: if !isRulerQueryFrontendAutoscalingZoneBEnabled then null else $.newRulerQueryFrontendScaledObject('ruler-query-frontend-zone-b', 'pod=~"ruler-query-frontend-zone-b.*"'),
  ruler_query_frontend_zone_c_scaled_object: if !isRulerQueryFrontendAutoscalingZoneCEnabled then null else $.newRulerQueryFrontendScaledObject('ruler-query-frontend-zone-c', 'pod=~"ruler-query-frontend-zone-c.*"'),

  //
  // Ruler-query-scheduler
  //

  ruler_query_scheduler_zone_a_args:: $.ruler_query_scheduler_args + $.rulerQuerySchedulerClientZoneArgs('a'),
  ruler_query_scheduler_zone_b_args:: $.ruler_query_scheduler_args + $.rulerQuerySchedulerClientZoneArgs('b'),
  ruler_query_scheduler_zone_c_args:: $.ruler_query_scheduler_args + $.rulerQuerySchedulerClientZoneArgs('c'),

  ruler_query_scheduler_zone_a_env_map:: {},
  ruler_query_scheduler_zone_b_env_map:: {},
  ruler_query_scheduler_zone_c_env_map:: {},

  ruler_query_scheduler_zone_a_node_affinity_matchers:: $.ruler_query_scheduler_node_affinity_matchers + [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[0])],
  ruler_query_scheduler_zone_b_node_affinity_matchers:: $.ruler_query_scheduler_node_affinity_matchers + [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[1])],
  ruler_query_scheduler_zone_c_node_affinity_matchers:: $.ruler_query_scheduler_node_affinity_matchers + [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[2])],

  ruler_query_scheduler_zone_a_container:: if !isZoneAEnabled then null else
    $.newRulerQuerySchedulerZoneContainer('a', $.ruler_query_scheduler_zone_a_args, $.ruler_query_scheduler_zone_a_env_map),

  ruler_query_scheduler_zone_b_container:: if !isZoneBEnabled then null else
    $.newRulerQuerySchedulerZoneContainer('b', $.ruler_query_scheduler_zone_b_args, $.ruler_query_scheduler_zone_b_env_map),

  ruler_query_scheduler_zone_c_container:: if !isZoneCEnabled then null else
    $.newRulerQuerySchedulerZoneContainer('c', $.ruler_query_scheduler_zone_c_args, $.ruler_query_scheduler_zone_c_env_map),

  ruler_query_scheduler_zone_a_deployment: if !isZoneAEnabled then null else
    $.newRulerQuerySchedulerZoneDeployment('a', $.ruler_query_scheduler_zone_a_container, $.ruler_query_scheduler_zone_a_node_affinity_matchers),

  ruler_query_scheduler_zone_b_deployment: if !isZoneBEnabled then null else
    $.newRulerQuerySchedulerZoneDeployment('b', $.ruler_query_scheduler_zone_b_container, $.ruler_query_scheduler_zone_b_node_affinity_matchers),

  ruler_query_scheduler_zone_c_deployment: if !isZoneCEnabled then null else
    $.newRulerQuerySchedulerZoneDeployment('c', $.ruler_query_scheduler_zone_c_container, $.ruler_query_scheduler_zone_c_node_affinity_matchers),

  ruler_query_scheduler_zone_a_service: if !isZoneAEnabled then null else
    $.util.serviceFor($.ruler_query_scheduler_zone_a_deployment, $._config.service_ignored_labels),

  ruler_query_scheduler_zone_b_service: if !isZoneBEnabled then null else
    $.util.serviceFor($.ruler_query_scheduler_zone_b_deployment, $._config.service_ignored_labels),

  ruler_query_scheduler_zone_c_service: if !isZoneCEnabled then null else
    $.util.serviceFor($.ruler_query_scheduler_zone_c_deployment, $._config.service_ignored_labels),

  ruler_query_scheduler_zone_a_pdb: if !isZoneAEnabled then null else
    $.newMimirPdb('ruler-query-scheduler-zone-a'),

  ruler_query_scheduler_zone_b_pdb: if !isZoneBEnabled then null else
    $.newMimirPdb('ruler-query-scheduler-zone-b'),

  ruler_query_scheduler_zone_c_pdb: if !isZoneCEnabled then null else
    $.newMimirPdb('ruler-query-scheduler-zone-c'),

  newRulerQuerySchedulerZoneContainer(zone, args, extraEnvVarMap={})::
    $.ruler_query_scheduler_container +
    container.withArgs($.util.mapToFlags(args)) +
    (if std.length(extraEnvVarMap) > 0 then container.withEnvMixin(std.prune(extraEnvVarMap)) else {}),

  newRulerQuerySchedulerZoneDeployment(zone, container, nodeAffinityMatchers=[])::
    local name = 'ruler-query-scheduler-zone-%s' % zone;

    $.newQuerySchedulerDeployment(name, container, nodeAffinityMatchers) +
    deployment.spec.template.spec.withTolerationsMixin($.newMimirMultiZoneToleration()),

  rulerQuerySchedulerClientZoneArgs(zone):: {
    // The query-scheduler runs on a dedicated ring per zone.
    'query-scheduler.ring.prefix': 'ruler-query-scheduler-zone-%s/' % zone,
  },

  // Ensure all configured addresses are zonal ones.
  local rulerQuerySchedulerMultiZoneConfigError = $.validateMimirMultiZoneConfig([
    'ruler_query_scheduler_zone_a_deployment',
    'ruler_query_scheduler_zone_b_deployment',
    'ruler_query_scheduler_zone_c_deployment',
  ]),
  assert rulerQuerySchedulerMultiZoneConfigError == null : rulerQuerySchedulerMultiZoneConfigError,

  // Remove single-zone deployment when it's disabled.
  ruler_query_scheduler_deployment: if !isSingleZoneEnabled then null else super.ruler_query_scheduler_deployment,
  ruler_query_scheduler_service: if !isSingleZoneEnabled then null else super.ruler_query_scheduler_service,
  ruler_query_scheduler_discovery_service: if !isSingleZoneEnabled then null else super.ruler_query_scheduler_discovery_service,
  ruler_query_scheduler_pdb: if !isSingleZoneEnabled then null else super.ruler_query_scheduler_pdb,
}
