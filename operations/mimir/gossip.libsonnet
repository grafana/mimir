{
  local memberlistConfig = {
    'memberlist.abort-if-join-fails': false,
    'memberlist.bind-port': gossipRingPort,
    'memberlist.join': 'gossip-ring.%s.svc.cluster.local:%d' % [$._config.namespace, gossipRingPort],
  },

  local setupGossipRing(storeOption, consulHostnameOption) = if $._config.multikv_migration_enabled then {
    [storeOption]: 'multi',
    // don't remove consul.hostname, it may still be needed.
  } else {
    [storeOption]: 'memberlist',
    [consulHostnameOption]: null,
  },

  _config+:: {
    // Migrating from consul to memberlist is a multi-step process:
    // 1) Enable multikv_migration_enabled, with primary=consul, secondary=memberlist, and multikv_mirror_enabled=false, restart components.
    // 2) Set multikv_mirror_enabled=true. This doesn't require restart.
    // 3) Swap multikv_primary and multikv_secondary, ie. multikv_primary=memberlist, multikv_secondary=consul. This doesn't require restart.
    // 4) Set multikv_migration_enabled=false. This requires restart, but components will now use only memberlist.
    multikv_migration_enabled: false,
    multikv_primary: 'consul',
    multikv_secondary: 'memberlist',
    multikv_mirror_enabled: false,

    // Use memberlist only. This works fine on already-migrated clusters.
    // To do a migration from Consul to memberlist, multi kv storage needs to be used (See below).
    ingesterRingClientConfig+: setupGossipRing('ingester.ring.store', 'ingester.ring.consul.hostname') + memberlistConfig,

    queryBlocksStorageConfig+:: setupGossipRing('store-gateway.sharding-ring.store', 'store-gateway.sharding-ring.consul.hostname') + memberlistConfig,

    // When doing migration via multi KV store, this section can be used
    // to configure runtime parameters of multi KV store
    multi_kv_config: if !$._config.multikv_migration_enabled then {} else {
      primary: $._config.multikv_primary,
      secondary: $._config.multikv_secondary,
      mirror_enabled: $._config.multikv_mirror_enabled,
    },
  },

  distributor_args+: setupGossipRing('distributor.ring.store', 'distributor.ring.consul.hostname') + memberlistConfig,

  ruler_args+: setupGossipRing('ruler.ring.store', 'ruler.ring.consul.hostname') + memberlistConfig,

  compactor_args+: setupGossipRing('compactor.ring.store', 'compactor.ring.consul.hostname') + memberlistConfig,

  local gossipRingPort = 7946,

  local containerPort = $.core.v1.containerPort,
  local gossipPort = containerPort.newNamed(name='gossip-ring', containerPort=gossipRingPort),

  compactor_ports+:: [gossipPort],
  distributor_ports+:: [gossipPort],
  ingester_ports+:: [gossipPort],
  querier_ports+:: [gossipPort],
  ruler_ports+:: [gossipPort],
  store_gateway_ports+:: [gossipPort],

  // Don't add label to matcher, only to pod labels.
  local gossipLabel = $.apps.v1.statefulSet.spec.template.metadata.withLabelsMixin({ [$._config.gossip_member_label]: 'true' }),

  compactor_statefulset+:
    gossipLabel,

  distributor_deployment+:
    gossipLabel,

  ingester_statefulset+:
    gossipLabel,

  querier_deployment+:
    gossipLabel,

  ruler_deployment+:
    if $._config.ruler_enabled then gossipLabel else {},

  store_gateway_statefulset+:
    gossipLabel,

  // Headless service (= no assigned IP, DNS returns all targets instead) pointing to gossip network members.
  gossip_ring_service:
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
