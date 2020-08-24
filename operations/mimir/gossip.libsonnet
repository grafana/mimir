{
  _config+:: {
    // Use memberlist only. This works fine on already-migrated clusters.
    // To do a migration from Consul to memberlist, multi kv storage needs to be used (See below).
    ringConfig+: {
      'ring.store': 'memberlist',
      'memberlist.abort-if-join-fails': false,
      'memberlist.bind-port': gossipRingPort,
      'memberlist.join': 'gossip-ring.%s.svc.cluster.local:%d' % [$._config.namespace, gossipRingPort],
    },

    // This can be used to enable multi KV store, with consul and memberlist.
    ringConfigMulti: {
      'ring.store': 'multi',
      'multi.primary': 'consul',
      'multi.secondary': 'memberlist',
    },

    // When doing migration via multi KV store, this section can be used
    // to configure runtime parameters of multi KV store
    /*
        multi_kv_config: {
          primary: 'memberlist',
          // 'mirror-enabled': false, // renamed to 'mirror_enabled' on after r67
        },
     */
  },

  ingester_args+: {
    // wait longer to see LEAVING ingester in the gossiped ring, to avoid
    // auto-join without transfer from LEAVING ingester.
    'ingester.join-after': '60s',

    // Updating heartbeat is low-cost operation when using gossiped ring, we can
    // do it more often (gossiping will happen no matter what, we may as well send
    // recent timestamps).
    // It also helps other components to see more recent update in the ring.
    'ingester.heartbeat-period': '5s',
  },

  local gossipRingPort = 7946,

  local containerPort = $.core.v1.containerPort,
  local gossipPort = containerPort.newNamed(name='gossip-ring', containerPort=gossipRingPort),

  distributor_ports+:: [gossipPort],
  querier_ports+:: [gossipPort],
  ingester_ports+:: [gossipPort],

  local gossip_member_label = 'gossip_ring_member',

  distributor_deployment_labels+:: { [gossip_member_label]: 'true' },
  ingester_deployment_labels+:: { [gossip_member_label]: 'true' },
  querier_deployment_labels+:: { [gossip_member_label]: 'true' },

  // Don't use gossip ring member label in service definition.
  distributor_service_ignored_labels+:: [gossip_member_label],
  ingester_service_ignored_labels+:: [gossip_member_label],
  querier_service_ignored_labels+:: [gossip_member_label],

  // Headless service (= no assigned IP, DNS returns all targets instead) pointing to some
  // users of gossiped-ring. We use ingesters as seed nodes for joining gossip cluster.
  // During migration to gossip, it may be useful to use distributors instead, since they are restarted faster.
  gossip_ring_service:
    local service = $.core.v1.service;

    // backwards compatibility with ksonnet
    local servicePort =
      if std.objectHasAll($.core.v1, 'servicePort')
      then $.core.v1.servicePort
      else service.mixin.spec.portsType;

    local ports = [
      servicePort.newNamed('gossip-ring', gossipRingPort, gossipRingPort) +
      servicePort.withProtocol('TCP'),
    ];
    service.new(
      'gossip-ring',  // name
      { [gossip_member_label]: 'true' },  // point to all gossip members
      ports,
    ) + service.mixin.spec.withClusterIp('None'),  // headless service
}
