{
  local container = $.core.v1.container,

  ruler_args::
    $._config.grpcConfig +
    $._config.ringConfig +
    $._config.storeConfig +
    $._config.storageConfig +
    $._config.blocksStorageConfig +
    $._config.queryConfig +
    $._config.distributorConfig +
    $._config.rulerClientConfig +
    {
      target: 'ruler',
      // Alertmanager configs
      'ruler.alertmanager-url': 'http://alertmanager.%s.svc.cluster.local/alertmanager' % $._config.namespace,
      'experimental.ruler.enable-api': true,
      'api.response-compression-enabled': true,

      // Ring Configs
      'ruler.enable-sharding': true,
      'ruler.ring.consul.hostname': 'consul.%s.svc.cluster.local:8500' % $._config.namespace,

      // Limits
      'ruler.max-rules-per-rule-group': $._config.limits.ruler_max_rules_per_rule_group,
      'ruler.max-rule-groups-per-tenant': $._config.limits.ruler_max_rule_groups_per_tenant,

      // Storage
      'querier.second-store-engine': $._config.querier_second_storage_engine,
    },

  ruler_container::
    if $._config.ruler_enabled then
      container.new('ruler', $._images.ruler) +
      container.withPorts($.util.defaultPorts) +
      container.withArgsMixin($.util.mapToFlags($.ruler_args)) +
      $.util.resourcesRequests('1', '6Gi') +
      $.util.resourcesLimits('16', '16Gi') +
      $.util.readinessProbe +
      $.jaeger_mixin
    else {},

  local deployment = $.apps.v1.deployment,

  ruler_deployment:
    if $._config.ruler_enabled then
      deployment.new('ruler', 2, [$.ruler_container]) +
      deployment.mixin.spec.strategy.rollingUpdate.withMaxSurge(0) +
      deployment.mixin.spec.strategy.rollingUpdate.withMaxUnavailable(1) +
      deployment.mixin.spec.template.spec.withTerminationGracePeriodSeconds(600) +
      $.util.antiAffinity +
      $.util.configVolumeMount($._config.overrides_configmap, '/etc/cortex') +
      $.storage_config_mixin
    else {},

  local service = $.core.v1.service,

  ruler_service:
    if $._config.ruler_enabled then
      $.util.serviceFor($.ruler_deployment)
    else {},
}
