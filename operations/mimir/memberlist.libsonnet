{
  local memberlistConfig = {
    'memberlist.abort-if-join-fails': false,
    'memberlist.bind-port': gossipRingPort,
    'memberlist.join': 'gossip-ring.%s.svc.cluster.local:%d' % [$._config.namespace, gossipRingPort],
  },

  local setupGossipRing(storeOption, consulHostnameOption, multiStoreOptionsPrefix) = if $._config.multikv_migration_enabled then {
    [storeOption]: 'multi',
    [multiStoreOptionsPrefix + '.primary']: $._config.multikv_primary,
    [multiStoreOptionsPrefix + '.secondary']: $._config.multikv_secondary,
    // don't remove consul.hostname, it may still be needed.
  } else {
    [storeOption]: 'memberlist',
    [consulHostnameOption]: null,
  },

  _config+:: {
    // Enables use of memberlist for all rings, instead of consul. If multikv_migration_enabled is true, consul hostname is still configured,
    // but "primary" KV depends on value of multikv_primary.
    memberlist_ring_enabled: false,

    // Migrating from consul to memberlist is a multi-step process:
    // 1) Enable multikv_migration_enabled, with primary=consul, secondary=memberlist, and multikv_mirror_enabled=false, restart components.
    // 2) Set multikv_mirror_enabled=true. This doesn't require restart.
    // 3) Swap multikv_primary and multikv_secondary, ie. multikv_primary=memberlist, multikv_secondary=consul. This doesn't require restart.
    // 4) Set multikv_migration_enabled=false and multikv_migration_teardown=true. This requires a restart, but components will now use only memberlist.
    // 5) Set multikv_migration_teardown=false. This doesn't require a restart.
    multikv_migration_enabled: false,
    multikv_migration_teardown: false,
    multikv_primary: 'consul',
    multikv_secondary: 'memberlist',
    multikv_switch_primary_secondary: false,
    multikv_mirror_enabled: false,

    // Use memberlist only. This works fine on already-migrated clusters.
    // To do a migration from Consul to memberlist, multi kv storage needs to be used (See below).
    ingesterRingClientConfig+: if !$._config.memberlist_ring_enabled then {} else (setupGossipRing('ingester.ring.store', 'ingester.ring.consul.hostname', 'ingester.ring.multi') + memberlistConfig),

    queryBlocksStorageConfig+:: if !$._config.memberlist_ring_enabled then {} else (setupGossipRing('store-gateway.sharding-ring.store', 'store-gateway.sharding-ring.consul.hostname', 'store-gateway.sharding-ring.multi') + memberlistConfig),

    // When doing migration via multi KV store, this section can be used
    // to configure runtime parameters of multi KV store
    multi_kv_config: if !$._config.multikv_migration_enabled && !$._config.multikv_migration_teardown then {} else {
      primary: if $._config.multikv_switch_primary_secondary then $._config.multikv_secondary else $._config.multikv_primary,
      mirror_enabled: $._config.multikv_mirror_enabled,
    },
  },

  distributor_args+: if !$._config.memberlist_ring_enabled then {} else (setupGossipRing('distributor.ring.store', 'distributor.ring.consul.hostname', 'distributor.ring.multi') + memberlistConfig),

  ruler_args+: if !$._config.memberlist_ring_enabled then {} else (setupGossipRing('ruler.ring.store', 'ruler.ring.consul.hostname', 'ruler.ring.multi') + memberlistConfig),

  compactor_args+: if !$._config.memberlist_ring_enabled then {} else (setupGossipRing('compactor.ring.store', 'compactor.ring.consul.hostname', 'compactor.ring.multi') + memberlistConfig),

  local gossipRingPort = 7946,

  local containerPort = $.core.v1.containerPort,
  local gossipPort = containerPort.newNamed(name='gossip-ring', containerPort=gossipRingPort),

  compactor_ports+:: if !$._config.memberlist_ring_enabled then [] else [gossipPort],
  distributor_ports+:: if !$._config.memberlist_ring_enabled then [] else [gossipPort],
  ingester_ports+:: if !$._config.memberlist_ring_enabled then [] else [gossipPort],
  querier_ports+:: if !$._config.memberlist_ring_enabled then [] else [gossipPort],
  ruler_ports+:: if !$._config.memberlist_ring_enabled then [] else [gossipPort],
  store_gateway_ports+:: if !$._config.memberlist_ring_enabled then [] else [gossipPort],

  // Don't add label to matcher, only to pod labels.
  local gossipLabel = $.apps.v1.statefulSet.spec.template.metadata.withLabelsMixin({ [$._config.gossip_member_label]: 'true' }),

  compactor_statefulset+: if !$._config.memberlist_ring_enabled then {} else
    gossipLabel,

  distributor_deployment+: if !$._config.memberlist_ring_enabled then {} else
    gossipLabel,

  ingester_statefulset: if $._config.multi_zone_ingester_enabled && !$._config.multi_zone_ingester_migration_enabled then null else
    (super.ingester_statefulset + if !$._config.memberlist_ring_enabled then {} else gossipLabel),

  ingester_zone_a_statefulset: if !$._config.multi_zone_ingester_enabled then null else
    (super.ingester_zone_a_statefulset + if !$._config.memberlist_ring_enabled then {} else gossipLabel),

  ingester_zone_b_statefulset: if !$._config.multi_zone_ingester_enabled then null else
    (super.ingester_zone_b_statefulset + if !$._config.memberlist_ring_enabled then {} else gossipLabel),

  ingester_zone_c_statefulset: if !$._config.multi_zone_ingester_enabled then null else
    (super.ingester_zone_c_statefulset + if !$._config.memberlist_ring_enabled then {} else gossipLabel),

  querier_deployment+: if !$._config.memberlist_ring_enabled then {} else gossipLabel,

  ruler_deployment+: if !$._config.memberlist_ring_enabled || !$._config.ruler_enabled then {} else gossipLabel,

  store_gateway_statefulset: if $._config.multi_zone_store_gateway_enabled && !$._config.multi_zone_store_gateway_migration_enabled then null else
    (super.store_gateway_statefulset + if !$._config.memberlist_ring_enabled then {} else gossipLabel),

  store_gateway_zone_a_statefulset: if !$._config.multi_zone_store_gateway_enabled then null else
    (super.store_gateway_zone_a_statefulset + if !$._config.memberlist_ring_enabled then {} else gossipLabel),

  store_gateway_zone_b_statefulset: if !$._config.multi_zone_store_gateway_enabled then null else
    (super.store_gateway_zone_b_statefulset + if !$._config.memberlist_ring_enabled then {} else gossipLabel),

  store_gateway_zone_c_statefulset: if !$._config.multi_zone_store_gateway_enabled then null else
    (super.store_gateway_zone_c_statefulset + if !$._config.memberlist_ring_enabled then {} else gossipLabel),

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
}
