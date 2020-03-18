{
  local container = $.core.v1.container,
  local containerPort = $.core.v1.containerPort,

  distributor_args::
    $._config.ringConfig +
    $._config.distributorConfig +
    {
      target: 'distributor',

      'validation.reject-old-samples': true,
      'validation.reject-old-samples.max-age': '12h',
      'limits.per-user-override-config': '/etc/cortex/overrides.yaml',
      'distributor.remote-timeout': '20s',

      'distributor.ha-tracker.enable': true,
      'distributor.ha-tracker.enable-for-all-users': true,
      'distributor.ha-tracker.store': 'etcd',
      'distributor.ha-tracker.etcd.endpoints': 'etcd-client.%s.svc.cluster.local.:2379' % $._config.namespace,
      'distributor.ha-tracker.prefix': 'prom_ha/',

      // The memory requests are 2G, and we barely use 100M.
      // By adding a ballast of 1G, we can drastically reduce GC, but also keep the usage at
      // around 1.25G, reducing the 99%ile.
      'mem-ballast-size-bytes': 1 << 30,  // 1GB

      // The cortex-gateway should frequently reopen the connections towards the
      // distributors in order to guarantee that new distributors receive traffic
      // as soon as they're ready.
      'server.grpc.keepalive.max-connection-age': '2m',
      'server.grpc.keepalive.max-connection-age-grace': '5m',
      'server.grpc.keepalive.max-connection-idle': '1m',

      'distributor.ingestion-rate-limit-strategy': 'global',
      'distributor.ingestion-rate-limit': 100000,  // 100K
      'distributor.ingestion-burst-size': 1000000,  // 1M

      // The ingestion rate global limit requires the distributors to form a ring.
      'distributor.ring.consul.hostname': 'consul.%s.svc.cluster.local:8500' % $._config.namespace,
      'distributor.ring.consul.consistent-reads': false,
      'distributor.ring.consul.watch-rate-limit': 1,
      'distributor.ring.consul.watch-burst-size': 1,
      'distributor.ring.prefix': '',
    },

  distributor_ports:: $.util.defaultPorts,

  distributor_container::
    container.new('distributor', $._images.distributor) +
    container.withPorts($.distributor_ports) +
    container.withArgsMixin($.util.mapToFlags($.distributor_args)) +
    $.util.resourcesRequests('2', '2Gi') +
    $.util.resourcesLimits('6', '4Gi') +
    $.jaeger_mixin,

  local deployment = $.apps.v1beta1.deployment,

  distributor_deployment_labels:: {},

  distributor_deployment:
    deployment.new('distributor', 3, [$.distributor_container], $.distributor_deployment_labels) +
    $.util.antiAffinity +
    $.util.configVolumeMount('overrides', '/etc/cortex'),

  local service = $.core.v1.service,

  distributor_service_ignored_labels:: [],

  distributor_service:
    $.util.serviceFor($.distributor_deployment, $.distributor_service_ignored_labels) +
    service.mixin.spec.withClusterIp('None'),
}
