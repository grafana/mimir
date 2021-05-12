{
  local container = $.core.v1.container,
  local containerPort = $.core.v1.containerPort,

  distributor_args::
    $._config.grpcConfig +
    $._config.ringConfig +
    $._config.distributorConfig +
    {
      target: 'distributor',

      'validation.reject-old-samples': true,
      'validation.reject-old-samples.max-age': '12h',
      'runtime-config.file': '/etc/cortex/overrides.yaml',
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

      'server.grpc.keepalive.max-connection-age': '2m',
      'server.grpc.keepalive.max-connection-age-grace': '5m',
      'server.grpc.keepalive.max-connection-idle': '1m',

      'distributor.ingestion-rate-limit-strategy': 'global',
      'distributor.ingestion-rate-limit': $._config.limits.ingestion_rate,
      'distributor.ingestion-burst-size': $._config.limits.ingestion_burst_size,

      // The ingestion rate global limit requires the distributors to form a ring.
      'distributor.ring.consul.hostname': 'consul.%s.svc.cluster.local:8500' % $._config.namespace,
      'distributor.ring.prefix': '',

      // Do not extend the replication set on unhealthy (or LEAVING) ingester when "unregister on shutdown"
      // is set to false.
      'distributor.extend-writes': $._config.unregister_ingesters_on_shutdown,
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

  distributor_deployment_labels:: {},

  distributor_deployment:
    deployment.new('distributor', 3, [$.distributor_container], $.distributor_deployment_labels) +
    $.util.antiAffinity +
    $.util.configVolumeMount($._config.overrides_configmap, '/etc/cortex'),

  local service = $.core.v1.service,

  distributor_service_ignored_labels:: [],

  distributor_service:
    $.util.serviceFor($.distributor_deployment, $.distributor_service_ignored_labels) +
    service.mixin.spec.withClusterIp('None'),
}
