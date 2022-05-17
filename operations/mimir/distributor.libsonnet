{
  local container = $.core.v1.container,
  local containerPort = $.core.v1.containerPort,

  distributor_args::
    $._config.grpcConfig +
    $._config.ingesterRingClientConfig +
    $._config.distributorLimitsConfig +
    {
      target: 'distributor',

      'runtime-config.file': '%s/overrides.yaml' % $._config.overrides_configmap_mountpoint,

      'distributor.ha-tracker.enable': true,
      'distributor.ha-tracker.enable-for-all-users': true,
      'distributor.ha-tracker.store': 'etcd',
      'distributor.ha-tracker.etcd.endpoints': 'etcd-client.%s.svc.cluster.local.:2379' % $._config.namespace,
      'distributor.ha-tracker.prefix': 'prom_ha/',

      // The memory requests are 2G, and we barely use 100M.
      // By adding a ballast of 1G, we can drastically reduce GC, but also keep the usage at
      // around 1.25G, reducing the 99%ile.
      'mem-ballast-size-bytes': 1 << 30,  // 1GB

      'server.http-listen-port': $._config.server_http_port,
      'server.grpc.keepalive.max-connection-age': '2m',
      'server.grpc.keepalive.max-connection-age-grace': '5m',
      'server.grpc.keepalive.max-connection-idle': '1m',

      // The ingestion rate global limit requires the distributors to form a ring.
      'distributor.ring.store': 'consul',
      'distributor.ring.consul.hostname': 'consul.%s.svc.cluster.local:8500' % $._config.namespace,
      'distributor.ring.prefix': '',
    },

  distributor_ports:: $.util.defaultPorts,

  distributor_container::
    container.new('distributor', $._images.distributor) +
    container.withPorts($.distributor_ports) +
    container.withArgsMixin($.util.mapToFlags($.distributor_args)) +
    $.util.resourcesRequests('2', '2Gi') +
    $.util.resourcesLimits(null, '4Gi') +
    $.util.readinessProbe +
    $.jaeger_mixin,

  local deployment = $.apps.v1.deployment,

  distributor_deployment:
    deployment.new('distributor', 3, [$.distributor_container]) +
    (if $._config.distributor_allow_multiple_replicas_on_same_node then {} else $.util.antiAffinity) +
    $.util.configVolumeMount($._config.overrides_configmap, $._config.overrides_configmap_mountpoint) +
    (if !std.isObject($._config.node_selector) then {} else deployment.mixin.spec.template.spec.withNodeSelectorMixin($._config.node_selector)) +
    deployment.mixin.spec.strategy.rollingUpdate.withMaxSurge(5) +
    deployment.mixin.spec.strategy.rollingUpdate.withMaxUnavailable(1),

  local service = $.core.v1.service,

  distributor_service:
    $.util.serviceFor($.distributor_deployment, $._config.service_ignored_labels) +
    service.mixin.spec.withClusterIp('None'),
}
