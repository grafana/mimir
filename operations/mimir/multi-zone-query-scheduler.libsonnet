{
  _config+:: {
    // Allow to configure whether the query-scheduler should be deployed in single or multi-zone.
    // Multi-zone and single-zone can be enabled at the same time during migrations.
    single_zone_query_scheduler_enabled: !$._config.multi_zone_query_scheduler_enabled,
    multi_zone_query_scheduler_enabled: false,
  },

  local container = $.core.v1.container,
  local deployment = $.apps.v1.deployment,
  local service = $.core.v1.service,

  local isSingleZoneEnabled = $._config.single_zone_query_scheduler_enabled,
  local isMultiZoneEnabled = $._config.multi_zone_query_scheduler_enabled,
  local isZoneAEnabled = isMultiZoneEnabled && std.length($._config.multi_zone_availability_zones) >= 1,
  local isZoneBEnabled = isMultiZoneEnabled && std.length($._config.multi_zone_availability_zones) >= 2,
  local isZoneCEnabled = isMultiZoneEnabled && std.length($._config.multi_zone_availability_zones) >= 3,

  assert !isMultiZoneEnabled || $._config.query_scheduler_service_discovery_mode == 'ring' : 'query-scheduler multi-zone deployment requires service discovery mode to be set to "ring"',

  query_scheduler_zone_a_args:: $.query_scheduler_args + $.querySchedulerClientZoneArgs('a'),
  query_scheduler_zone_b_args:: $.query_scheduler_args + $.querySchedulerClientZoneArgs('b'),
  query_scheduler_zone_c_args:: $.query_scheduler_args + $.querySchedulerClientZoneArgs('c'),

  query_scheduler_zone_a_env_map:: {},
  query_scheduler_zone_b_env_map:: {},
  query_scheduler_zone_c_env_map:: {},

  query_scheduler_zone_a_node_affinity_matchers:: $.query_scheduler_node_affinity_matchers + [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[0])],
  query_scheduler_zone_b_node_affinity_matchers:: $.query_scheduler_node_affinity_matchers + [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[1])],
  query_scheduler_zone_c_node_affinity_matchers:: $.query_scheduler_node_affinity_matchers + [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[2])],

  query_scheduler_zone_a_container:: if !isZoneAEnabled then null else
    $.newQuerySchedulerZoneContainer('a', $.query_scheduler_zone_a_args, $.query_scheduler_zone_a_env_map),

  query_scheduler_zone_b_container:: if !isZoneBEnabled then null else
    $.newQuerySchedulerZoneContainer('b', $.query_scheduler_zone_b_args, $.query_scheduler_zone_b_env_map),

  query_scheduler_zone_c_container:: if !isZoneCEnabled then null else
    $.newQuerySchedulerZoneContainer('c', $.query_scheduler_zone_c_args, $.query_scheduler_zone_c_env_map),

  query_scheduler_zone_a_deployment: if !isZoneAEnabled then null else
    $.newQuerySchedulerZoneDeployment('a', $.query_scheduler_zone_a_container, $.query_scheduler_zone_a_node_affinity_matchers),

  query_scheduler_zone_b_deployment: if !isZoneBEnabled then null else
    $.newQuerySchedulerZoneDeployment('b', $.query_scheduler_zone_b_container, $.query_scheduler_zone_b_node_affinity_matchers),

  query_scheduler_zone_c_deployment: if !isZoneCEnabled then null else
    $.newQuerySchedulerZoneDeployment('c', $.query_scheduler_zone_c_container, $.query_scheduler_zone_c_node_affinity_matchers),

  query_scheduler_zone_a_service: if !isZoneAEnabled then null else
    $.util.serviceFor($.query_scheduler_zone_a_deployment, $._config.service_ignored_labels),

  query_scheduler_zone_b_service: if !isZoneBEnabled then null else
    $.util.serviceFor($.query_scheduler_zone_b_deployment, $._config.service_ignored_labels),

  query_scheduler_zone_c_service: if !isZoneCEnabled then null else
    $.util.serviceFor($.query_scheduler_zone_c_deployment, $._config.service_ignored_labels),

  query_scheduler_zone_a_pdb: if !isZoneAEnabled then null else
    $.newMimirPdb('query-scheduler-zone-a'),

  query_scheduler_zone_b_pdb: if !isZoneBEnabled then null else
    $.newMimirPdb('query-scheduler-zone-b'),

  query_scheduler_zone_c_pdb: if !isZoneCEnabled then null else
    $.newMimirPdb('query-scheduler-zone-c'),

  newQuerySchedulerZoneContainer(zone, args, extraEnvVarMap={})::
    $.query_scheduler_container +
    container.withArgs($.util.mapToFlags(args)) +
    (if std.length(extraEnvVarMap) > 0 then container.withEnvMixin(std.prune(extraEnvVarMap)) else {}),

  newQuerySchedulerZoneDeployment(zone, container, nodeAffinityMatchers=[])::
    local name = 'query-scheduler-zone-%s' % zone;

    $.newQuerySchedulerDeployment(name, container, nodeAffinityMatchers) +
    deployment.spec.template.spec.withTolerationsMixin($.newMimirMultiZoneToleration()),

  querySchedulerClientZoneArgs(zone):: {
    // The query-scheduler runs on a dedicated ring per zone.
    'query-scheduler.ring.prefix': 'query-scheduler-zone-%s/' % zone,
  },

  // Ensure all configured addresses are zonal ones.
  local querySchedulerMultiZoneConfigError = $.validateMimirMultiZoneConfig([
    'query_scheduler_zone_a_deployment',
    'query_scheduler_zone_b_deployment',
    'query_scheduler_zone_c_deployment',
  ]),
  assert querySchedulerMultiZoneConfigError == null : querySchedulerMultiZoneConfigError,

  // Remove single-zone deployment when it's disabled.
  query_scheduler_deployment: if !isSingleZoneEnabled then null else super.query_scheduler_deployment,
  query_scheduler_service: if !isSingleZoneEnabled then null else super.query_scheduler_service,
  query_scheduler_discovery_service: if !isSingleZoneEnabled then null else super.query_scheduler_discovery_service,
  query_scheduler_pdb: if !isSingleZoneEnabled then null else super.query_scheduler_pdb,
}
