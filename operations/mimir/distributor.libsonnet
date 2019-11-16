{
  local container = $.core.v1.container,
  local containerPort = $.core.v1.containerPort,

  distributor_args::
    $._config.ringConfig +
    $._config.distributorConfig +
    {
      target: 'distributor',

      'distributor.ingestion-rate-limit': 10000,
      'distributor.ingestion-burst-size': 20000,
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
    },

  distributor_container::
    container.new('distributor', $._images.distributor) +
    container.withPorts($.util.defaultPorts) +
    container.withArgsMixin($.util.mapToFlags($.distributor_args)) +
    $.util.resourcesRequests('2', '2Gi') +
    $.util.resourcesLimits('6', '4Gi') +
    $.jaeger_mixin,

  local deployment = $.apps.v1beta1.deployment,

  distributor_deployment:
    deployment.new('distributor', 3, [
      $.distributor_container,
    ]) +
    $.util.antiAffinity +
    $.util.configVolumeMount('overrides', '/etc/cortex'),

  local service = $.core.v1.service,

  distributor_service:
    $.util.serviceFor($.distributor_deployment) +
    service.mixin.spec.withClusterIp('None'),
}
