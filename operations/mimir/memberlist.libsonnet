{
  local setupGossipRing(storeOption, consulHostnameOption, multiStoreOptionsPrefix) = if $._config.multikv_migration_enabled then {
    [storeOption]: 'multi',
    [multiStoreOptionsPrefix + '.primary']: $._config.multikv_primary,
    [multiStoreOptionsPrefix + '.secondary']: $._config.multikv_secondary,
    // don't remove consul.hostname, it may still be needed.
  } else {
    [storeOption]: 'memberlist',
    [consulHostnameOption]: null,
  },

  // Memberlist is enabled for the query-frontend only if query-scheduler should be discovered via the ring.
  local queryFrontendMemberlistEnabled =
    $._config.memberlist_ring_enabled &&
    $._config.query_scheduler_service_discovery_mode == 'ring' &&
    $._config.query_scheduler_service_discovery_ring_read_path_enabled,

  // Memberlist is enabled for the query-scheduler only if ring-based service discovery is enabled.
  local querySchedulerMemberlistEnabled =
    $._config.memberlist_ring_enabled &&
    $._config.query_scheduler_service_discovery_mode == 'ring',

  _config+:: {
    // Enables use of memberlist for all rings, instead of consul. If multikv_migration_enabled is true, consul hostname is still configured,
    // but "primary" KV depends on value of multikv_primary.
    memberlist_ring_enabled: true,

    // Configures the memberlist cluster label. When verification is enabled, a memberlist member rejects any packet or stream
    // with a mismatching cluster label.
    memberlist_cluster_label: '',
    memberlist_cluster_label_verification_disabled: false,

    // To migrate from Consul to Memberlist check "Migrating from Consul to Memberlist KV store for hash rings" article in Mimir documentation.
    multikv_migration_enabled: false,  // Enable multi KV.
    multikv_migration_teardown: false,  // If multikv_migration_enabled=false and multikv_migration_teardown=true, runtime configuration for multi KV is preserved.
    multikv_switch_primary_secondary: false,  // Switch primary and secondary KV stores in runtime configuration for multi KV.
    multikv_mirror_enabled: false,  // Enable mirroring of writes from primary to secondary KV store.

    // Don't change these values during migration. Use multikv_switch_primary_secondary instead.
    multikv_primary: 'consul',
    multikv_secondary: 'memberlist',

    // Use memberlist only. This works fine on already-migrated clusters.
    // To do a migration from Consul to memberlist, multi kv storage needs to be used (See below).
    ingesterRingClientConfig+: if !$._config.memberlist_ring_enabled then {} else (setupGossipRing('ingester.ring.store', 'ingester.ring.consul.hostname', 'ingester.ring.multi') + $._config.memberlistConfig),

    queryBlocksStorageConfig+:: if !$._config.memberlist_ring_enabled then {} else (setupGossipRing('store-gateway.sharding-ring.store', 'store-gateway.sharding-ring.consul.hostname', 'store-gateway.sharding-ring.multi') + $._config.memberlistConfig),

    overridesExporterRingConfig+: if !$._config.memberlist_ring_enabled || !$._config.overrides_exporter_ring_enabled then {}
    else (setupGossipRing('overrides-exporter.ring.store', 'overrides-exporter.ring.consul.hostname', 'overrides-exporter.ring.multi') + $._config.memberlistConfig),

    querySchedulerRingClientConfig+: if !queryFrontendMemberlistEnabled then {} else (setupGossipRing('query-scheduler.ring.store', 'query-scheduler.ring.consul.hostname', 'query-scheduler.ring.multi') + $._config.memberlistConfig),
    querySchedulerRingLifecyclerConfig+: if !querySchedulerMemberlistEnabled then {} else (setupGossipRing('query-scheduler.ring.store', 'query-scheduler.ring.consul.hostname', 'query-scheduler.ring.multi') + $._config.memberlistConfig),

    // When doing migration via multi KV store, this section can be used
    // to configure runtime parameters of multi KV store
    multi_kv_config: if !$._config.multikv_migration_enabled && !$._config.multikv_migration_teardown then {} else {
      primary: if $._config.multikv_switch_primary_secondary then $._config.multikv_secondary else $._config.multikv_primary,
      mirror_enabled: $._config.multikv_mirror_enabled,
    },

    memberlistConfig:: {
      'memberlist.bind-port': gossipRingPort,
      'memberlist.join': 'dns+gossip-ring.%s.svc.cluster.local:%d' % [$._config.namespace, gossipRingPort],
    } + (
      if $._config.memberlist_cluster_label == '' then {} else {
        'memberlist.cluster-label': $._config.memberlist_cluster_label,
      }
    ) + (
      if !$._config.memberlist_cluster_label_verification_disabled then {} else {
        'memberlist.cluster-label-verification-disabled': true,
      }
    ),
  },

  alertmanager_args+: if !$._config.memberlist_ring_enabled then {} else (setupGossipRing('alertmanager.sharding-ring.store', 'alertmanager.sharding-ring.consul.hostname', 'alertmanager.sharding-ring.multi') + $._config.memberlistConfig),
  distributor_args+: if !$._config.memberlist_ring_enabled then {} else (setupGossipRing('distributor.ring.store', 'distributor.ring.consul.hostname', 'distributor.ring.multi') + $._config.memberlistConfig),
  ruler_args+: if !$._config.memberlist_ring_enabled then {} else (setupGossipRing('ruler.ring.store', 'ruler.ring.consul.hostname', 'ruler.ring.multi') + $._config.memberlistConfig),
  compactor_args+: if !$._config.memberlist_ring_enabled then {} else (setupGossipRing('compactor.ring.store', 'compactor.ring.consul.hostname', 'compactor.ring.multi') + $._config.memberlistConfig),

  local gossipRingPort = 7946,

  local containerPort = $.core.v1.containerPort,
  local gossipPort = containerPort.newNamed(name='gossip-ring', containerPort=gossipRingPort),

  alertmanager_ports+:: if !$._config.memberlist_ring_enabled then [] else [gossipPort],
  compactor_ports+:: if !$._config.memberlist_ring_enabled then [] else [gossipPort],
  distributor_ports+:: if !$._config.memberlist_ring_enabled then [] else [gossipPort],
  ingester_ports+:: if !$._config.memberlist_ring_enabled then [] else [gossipPort],
  querier_ports+:: if !$._config.memberlist_ring_enabled then [] else [gossipPort],
  ruler_ports+:: if !$._config.memberlist_ring_enabled then [] else [gossipPort],
  store_gateway_ports+:: if !$._config.memberlist_ring_enabled then [] else [gossipPort],
  query_scheduler_ports+:: if !querySchedulerMemberlistEnabled then [] else [gossipPort],
  query_frontend_ports+:: if !queryFrontendMemberlistEnabled then [] else [gossipPort],

  // Don't add label to matcher, only to pod labels.
  local gossipLabel = $.apps.v1.statefulSet.spec.template.metadata.withLabelsMixin({ [$._config.gossip_member_label]: 'true' }),

  alertmanager_statefulset: overrideSuperIfExists(
    'alertmanager_statefulset',
    if !$._config.memberlist_ring_enabled then {} else gossipLabel
  ),

  compactor_statefulset: overrideSuperIfExists(
    'compactor_statefulset',
    if !$._config.memberlist_ring_enabled then {} else gossipLabel
  ),

  distributor_deployment: overrideSuperIfExists(
    'distributor_deployment',
    if !$._config.memberlist_ring_enabled then {} else gossipLabel
  ),

  ingester_statefulset: overrideSuperIfExists(
    'ingester_statefulset',
    if !$._config.memberlist_ring_enabled then {} else gossipLabel
  ),

  ingester_zone_a_statefulset: overrideSuperIfExists(
    'ingester_zone_a_statefulset',
    if !$._config.memberlist_ring_enabled then {} else gossipLabel
  ),

  ingester_zone_b_statefulset: overrideSuperIfExists(
    'ingester_zone_b_statefulset',
    if !$._config.memberlist_ring_enabled then {} else gossipLabel
  ),

  ingester_zone_c_statefulset: overrideSuperIfExists(
    'ingester_zone_c_statefulset',
    if !$._config.memberlist_ring_enabled then {} else gossipLabel
  ),

  querier_deployment: overrideSuperIfExists(
    'querier_deployment',
    if !$._config.memberlist_ring_enabled then {} else gossipLabel
  ),

  ruler_querier_deployment: overrideSuperIfExists(
    'ruler_querier_deployment',
    if !$._config.memberlist_ring_enabled then {} else gossipLabel
  ),

  ruler_deployment: overrideSuperIfExists(
    'ruler_deployment',
    if !$._config.memberlist_ring_enabled then {} else gossipLabel
  ),

  query_scheduler_deployment: overrideSuperIfExists(
    'query_scheduler_deployment',
    if !querySchedulerMemberlistEnabled then {} else gossipLabel
  ),

  ruler_query_scheduler_deployment: overrideSuperIfExists(
    'ruler_query_scheduler_deployment',
    if !querySchedulerMemberlistEnabled then {} else gossipLabel
  ),

  query_frontend_deployment: overrideSuperIfExists(
    'query_frontend_deployment',
    if !queryFrontendMemberlistEnabled then {} else gossipLabel
  ),

  ruler_query_frontend_deployment: overrideSuperIfExists(
    'ruler_query_frontend_deployment',
    if !queryFrontendMemberlistEnabled then {} else gossipLabel
  ),

  store_gateway_statefulset: overrideSuperIfExists(
    'store_gateway_statefulset',
    if !$._config.memberlist_ring_enabled then {} else gossipLabel
  ),

  store_gateway_zone_a_statefulset: overrideSuperIfExists(
    'store_gateway_zone_a_statefulset',
    if !$._config.memberlist_ring_enabled then {} else gossipLabel
  ),

  store_gateway_zone_b_statefulset: overrideSuperIfExists(
    'store_gateway_zone_b_statefulset',
    if !$._config.memberlist_ring_enabled then {} else gossipLabel
  ),

  store_gateway_zone_c_statefulset: overrideSuperIfExists(
    'store_gateway_zone_c_statefulset',
    if !$._config.memberlist_ring_enabled then {} else gossipLabel
  ),

  // Headless service (= no assigned IP, DNS returns all targets instead) pointing to gossip network members.
  gossip_ring_service:
    if !$._config.memberlist_ring_enabled then null
    else
      local service = $.core.v1.service;
      local servicePort = $.core.v1.servicePort;

      local ports = [
        servicePort.newNamed('gossip-ring', gossipRingPort, gossipRingPort) +
        servicePort.withProtocol('TCP'),
      ];
      service.new(
        'gossip-ring',  // name
        { [$._config.gossip_member_label]: 'true' },  // point to all gossip members
        ports,
      ) + service.mixin.spec.withClusterIp('None'),  // headless service

  // Utility used to override a field only if exists in super.
  local overrideSuperIfExists(name, override) = if !( name in super) || super[name] == null || super[name] == {} then null else
    super[name] + override,
}
