{
  _config+:: {
    multi_zone_memberlist_bridge_enabled: false,

    // Whether the zone-aware routing should be enabled.
    memberlist_zone_aware_routing_enabled: false,

    // Priority class for memberlist-bridge pods. Memberlist-bridge pods act as bridges between AZs
    // so they're critical to avoid network partitioning.
    memberlist_bridge_priority_class: 'high-nonpreempting',

    // Number of memberlist-bridge replicas per zone. Memberlist zone-aware routing has an
    // auto-failover mechanism to temporarily disable zone-aware routing if it detects that
    // a zone has no healthy bridges; for this reason, 2 replicas are enough for high-availability
    // without a significant risk of network partitioning if both bridges in a zone are unhealthy.
    memberlist_bridge_replicas_per_zone: 2,
  },

  _images+:: if $._config.multi_zone_memberlist_bridge_enabled then {
    memberlist_bridge: $._images.mimir,
  } else {},

  local container = $.core.v1.container,
  local deployment = $.apps.v1.deployment,
  local service = $.core.v1.service,

  local isMultiZoneEnabled = $._config.multi_zone_memberlist_bridge_enabled,
  local isZoneAEnabled = isMultiZoneEnabled && std.length($._config.multi_zone_availability_zones) >= 1,
  local isZoneBEnabled = isMultiZoneEnabled && std.length($._config.multi_zone_availability_zones) >= 2,
  local isZoneCEnabled = isMultiZoneEnabled && std.length($._config.multi_zone_availability_zones) >= 3,

  assert !$._config.memberlist_zone_aware_routing_enabled || $._config.multi_zone_memberlist_bridge_enabled : 'memberlist zone-aware routing requires memberlist-bridge multi-zone deployment',

  memberlist_bridge_ports:: $.util.defaultPorts,
  memberlist_bridge_node_affinity_matchers:: [],

  memberlist_bridge_args::
    $._config.commonConfig
    + $._config.usageStatsConfig
    + $._config.querySchedulerRingClientConfig
    + $._config.memberlistConfig
    + {
      target: 'memberlist-kv',
      'server.http-listen-port': $._config.server_http_port,
    },

  // Zonal memberlist config that must be applied by all zonal deployments.
  local memberlistZoneArgs(zone) = if $._config.memberlist_zone_aware_routing_enabled then {
    'memberlist.zone-aware-routing.enabled': true,
    'memberlist.zone-aware-routing.instance-availability-zone': 'zone-%s' % zone,
    'memberlist.zone-aware-routing.role': 'member',
  } else {},

  memberlist_zone_a_args:: memberlistZoneArgs('a'),
  memberlist_zone_b_args:: memberlistZoneArgs('b'),
  memberlist_zone_c_args:: memberlistZoneArgs('c'),

  // We always enable zone-aware routing for the zonal memberlist-bridge, so that "bridges" are zone-aware
  // when zone-aware routing is enabled for "members" too (members are the other Mimir components).
  local memberlistBridgeZoneArgs(zone) = {
    // When memberlist cross-zone routing is enabled, memberlist-bridge pods act as bridges between AZs
    // so they're critical to avoid network partitioning. We configure them to reduce likelihood of dropping
    // messages in the brodcast queue.
    'memberlist.zone-aware-routing.enabled': true,
    'memberlist.zone-aware-routing.instance-availability-zone': 'zone-%s' % zone,
    'memberlist.zone-aware-routing.role': 'bridge',
    'memberlist.max-concurrent-writes': 15,
  },

  memberlist_bridge_zone_a_args:: $.memberlist_bridge_args + $.memberlist_zone_a_args + memberlistBridgeZoneArgs('a'),
  memberlist_bridge_zone_b_args:: $.memberlist_bridge_args + $.memberlist_zone_b_args + memberlistBridgeZoneArgs('b'),
  memberlist_bridge_zone_c_args:: $.memberlist_bridge_args + $.memberlist_zone_c_args + memberlistBridgeZoneArgs('c'),

  memberlist_bridge_zone_a_env_map:: {},
  memberlist_bridge_zone_b_env_map:: {},
  memberlist_bridge_zone_c_env_map:: {},

  memberlist_bridge_zone_a_node_affinity_matchers:: $.memberlist_bridge_node_affinity_matchers + [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[0])],
  memberlist_bridge_zone_b_node_affinity_matchers:: $.memberlist_bridge_node_affinity_matchers + [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[1])],
  memberlist_bridge_zone_c_node_affinity_matchers:: $.memberlist_bridge_node_affinity_matchers + [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[2])],

  memberlist_bridge_zone_a_container:: if !isZoneAEnabled then null else
    $.newMemberlistBridgeZoneContainer('a', $.memberlist_bridge_zone_a_args, $.memberlist_bridge_zone_a_env_map),

  memberlist_bridge_zone_b_container:: if !isZoneBEnabled then null else
    $.newMemberlistBridgeZoneContainer('b', $.memberlist_bridge_zone_b_args, $.memberlist_bridge_zone_b_env_map),

  memberlist_bridge_zone_c_container:: if !isZoneCEnabled then null else
    $.newMemberlistBridgeZoneContainer('c', $.memberlist_bridge_zone_c_args, $.memberlist_bridge_zone_c_env_map),

  memberlist_bridge_zone_a_deployment: if !isZoneAEnabled then null else
    $.newMemberlistBridgeZoneDeployment('a', $.memberlist_bridge_zone_a_container, $.memberlist_bridge_zone_a_node_affinity_matchers),

  memberlist_bridge_zone_b_deployment: if !isZoneBEnabled then null else
    $.newMemberlistBridgeZoneDeployment('b', $.memberlist_bridge_zone_b_container, $.memberlist_bridge_zone_b_node_affinity_matchers),

  memberlist_bridge_zone_c_deployment: if !isZoneCEnabled then null else
    $.newMemberlistBridgeZoneDeployment('c', $.memberlist_bridge_zone_c_container, $.memberlist_bridge_zone_c_node_affinity_matchers),

  memberlist_bridge_zone_a_pdb: if !isZoneAEnabled then null else
    $.newMimirPdb('memberlist-bridge-zone-a'),

  memberlist_bridge_zone_b_pdb: if !isZoneBEnabled then null else
    $.newMimirPdb('memberlist-bridge-zone-b'),

  memberlist_bridge_zone_c_pdb: if !isZoneCEnabled then null else
    $.newMimirPdb('memberlist-bridge-zone-c'),

  newMemberlistBridgeZoneContainer(zone, args, extraEnvVarMap={})::
    container.new('memberlist-bridge', $._images.memberlist_bridge)
    + container.withPorts($.memberlist_bridge_ports)
    + container.withArgs($.util.mapToFlags(args))
    + $.tracing_env_mixin
    + $.util.readinessProbe
    + $.util.resourcesRequests('0.25', '1Gi')
    + $.util.resourcesLimits(null, '2Gi')
    + (if std.length(extraEnvVarMap) > 0 then container.withEnvMixin(std.prune(extraEnvVarMap)) else {}),

  newMemberlistBridgeZoneDeployment(zone, container, nodeAffinityMatchers=[])::
    local name = 'memberlist-bridge-zone-%s' % zone;

    deployment.new(name, $._config.memberlist_bridge_replicas_per_zone, [container])
    + $.newMimirSpreadTopology(name, 1)
    + $.newMimirNodeAffinityMatchers(nodeAffinityMatchers)
    + (if !std.isObject($._config.node_selector) then {} else deployment.mixin.spec.template.spec.withNodeSelectorMixin($._config.node_selector))
    + deployment.mixin.spec.strategy.rollingUpdate.withMaxUnavailable(0)
    + deployment.spec.template.spec.withTolerationsMixin($.newMimirMultiZoneToleration())
    // When memberlist cross-zone routing is enabled, memberlist-bridge pods act as bridges between AZs
    // so they're critical to avoid network partitioning. We want to guarantee that 2 bridges don't run
    // on the same node, and we also want to run them with higher-than-default priority.
    + deployment.spec.template.spec.withPriorityClassName($._config.memberlist_bridge_priority_class)
    + $.util.antiAffinity,

  // Ensure all configured addresses are zonal ones.
  local memberlistBridgeMultiZoneConfigError = $.validateMimirMultiZoneConfig([
    'memberlist_bridge_zone_a_deployment',
    'memberlist_bridge_zone_b_deployment',
    'memberlist_bridge_zone_c_deployment',
  ]),
  assert memberlistBridgeMultiZoneConfigError == null : memberlistBridgeMultiZoneConfigError,

  //
  // Apply memberlist config to all Mimir components. Single-zone workloads should always connect
  // to a single zone, which is zone A, as that is the default zone by design.
  //

  local isZoneBAvailable = std.length($._config.multi_zone_availability_zones) >= 2,
  local isZoneCAvailable = std.length($._config.multi_zone_availability_zones) >= 3,

  distributor_args+:: $.memberlist_zone_a_args,
  distributor_zone_a_args+:: $.memberlist_zone_a_args,
  distributor_zone_b_args+:: $.memberlist_zone_b_args,
  distributor_zone_c_args+:: $.memberlist_zone_c_args,

  query_frontend_args+:: $.memberlist_zone_a_args,
  query_frontend_zone_a_args+:: $.memberlist_zone_a_args,
  query_frontend_zone_b_args+:: $.memberlist_zone_b_args,
  query_frontend_zone_c_args+:: $.memberlist_zone_c_args,

  query_scheduler_args+:: $.memberlist_zone_a_args,
  query_scheduler_zone_a_args+:: $.memberlist_zone_a_args,
  query_scheduler_zone_b_args+:: $.memberlist_zone_b_args,
  query_scheduler_zone_c_args+:: $.memberlist_zone_c_args,

  querier_args+:: $.memberlist_zone_a_args,
  querier_zone_a_args+:: $.memberlist_zone_a_args,
  querier_zone_b_args+:: $.memberlist_zone_b_args,
  querier_zone_c_args+:: $.memberlist_zone_c_args,

  ruler_args+:: $.memberlist_zone_a_args,
  ruler_zone_a_args+:: $.memberlist_zone_a_args,
  ruler_zone_b_args+:: $.memberlist_zone_b_args,
  ruler_zone_c_args+:: $.memberlist_zone_c_args,

  ruler_query_frontend_args+:: $.memberlist_zone_a_args,
  ruler_query_frontend_zone_a_args+:: $.memberlist_zone_a_args,
  ruler_query_frontend_zone_b_args+:: $.memberlist_zone_b_args,
  ruler_query_frontend_zone_c_args+:: $.memberlist_zone_c_args,

  ruler_query_scheduler_args+:: $.memberlist_zone_a_args,
  ruler_query_scheduler_zone_a_args+:: $.memberlist_zone_a_args,
  ruler_query_scheduler_zone_b_args+:: $.memberlist_zone_b_args,
  ruler_query_scheduler_zone_c_args+:: $.memberlist_zone_c_args,

  ruler_querier_args+:: $.memberlist_zone_a_args,
  ruler_querier_zone_a_args+:: $.memberlist_zone_a_args,
  ruler_querier_zone_b_args+:: $.memberlist_zone_b_args,
  ruler_querier_zone_c_args+:: $.memberlist_zone_c_args,

  // Ingesters get split in "virtual" zones, that could eventually be mapped to different AZs.
  // If multi-AZ deployment is enabled, then we apply the zone-specific memberlist config, otherwise the zone-a one.
  ingester_zone_a_args+:: $.memberlist_zone_a_args,
  ingester_zone_b_args+:: if $._config.multi_zone_ingester_multi_az_enabled && isZoneBAvailable then $.memberlist_zone_b_args else $.memberlist_zone_a_args,
  ingester_zone_c_args+:: if $._config.multi_zone_ingester_multi_az_enabled && isZoneCAvailable then $.memberlist_zone_c_args else $.memberlist_zone_a_args,

  // Store-gateways get split in "virtual" zones, that could eventually be mapped to different AZs.
  // If multi-AZ deployment is enabled, then we apply the zone-specific memberlist config, otherwise the zone-a one.
  store_gateway_zone_a_args+:: $.memberlist_zone_a_args,
  store_gateway_zone_b_args+:: if $._config.multi_zone_store_gateway_zone_b_multi_az_enabled && isZoneBAvailable then $.memberlist_zone_b_args else $.memberlist_zone_a_args,
  store_gateway_zone_c_args+:: if $._config.multi_zone_store_gateway_zone_c_multi_az_enabled && isZoneCAvailable then $.memberlist_zone_c_args else $.memberlist_zone_a_args,

  // Other components only deployed to zone-a.
  compactor_args+:: $.memberlist_zone_a_args,
}
