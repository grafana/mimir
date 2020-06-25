{
  local container = $.core.v1.container,

  ruler_args::
    $._config.ringConfig +
    $._config.storeConfig +
    $._config.storageConfig +
    $._config.queryConfig +
    $._config.distributorConfig +
    $._config.rulerClientConfig +
    {
      target: 'ruler',
      // Alertmanager configs
      'ruler.alertmanager-url': 'http://alertmanager.%s.svc.cluster.local/alertmanager' % $._config.namespace,
      'experimental.ruler.enable-api': true,

      // Ring Configs
      'ruler.enable-sharding': true,
      'ruler.ring.consul.hostname': 'consul.%s.svc.cluster.local:8500' % $._config.namespace,
    },

  ruler_container::
    container.new('ruler', $._images.ruler) +
    container.withPorts($.util.defaultPorts) +
    container.withArgsMixin($.util.mapToFlags($.ruler_args)) +
    $.util.resourcesRequests('1', '6Gi') +
    $.util.resourcesLimits('16', '16Gi') +
    $.util.readinessProbe +
    $.jaeger_mixin,

  local deployment = $.apps.v1.deployment,

  ruler_deployment:
    deployment.new('ruler', 2, [$.ruler_container]) +
    deployment.mixin.spec.template.spec.withTerminationGracePeriodSeconds(600) +
    $.util.antiAffinity +
    $.util.configVolumeMount('overrides', '/etc/cortex') +
    $.storage_config_mixin,

  local service = $.core.v1.service,

  ruler_service:
    $.util.serviceFor($.ruler_deployment),
}
