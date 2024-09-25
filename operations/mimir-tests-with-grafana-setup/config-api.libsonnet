{
  _config+:: {
    config_api_enabled: false,
  },

  config_api_args:: {
    'server.http-listen-port': '80',
    target: 'config-api',
    'instrumentation.enabled': 'false',
    'auth.type': 'trust',
  } + $._config.storageConfig + $._config.rulerStorageConfig + $.mimirRuntimeConfigFile,

  config_api_ports:: $.util.defaultPorts,

  config_api_node_affinity_matchers:: [],

  local container = $.core.v1.container,

  config_api_container::
    if $._config.config_api_enabled
    then
      container.new('config-api', $._images.config_api) +
      container.withPorts($.config_api_ports) +
      container.withArgsMixin($.util.mapToFlags($.config_api_args)) +
      $.util.resourcesRequests('100m', '500Mi') +
      $.util.resourcesLimits(null, '1Gi') +
      $.util.readinessProbe +
      $.jaeger_mixin
    else {},

  local deployment = $.apps.v1.deployment,
  config_api_deployment:
    if $._config.config_api_enabled
    then
      deployment.new('config-api', 2, [$.config_api_container]) +
      $.mimirVolumeMounts +
      $.newMimirNodeAffinityMatchers($.config_api_node_affinity_matchers) +
      deployment.mixin.spec.strategy.rollingUpdate.withMaxSurge(1) +
      deployment.mixin.spec.strategy.rollingUpdate.withMaxUnavailable(1) +
      deployment.mixin.spec.template.spec.withImagePullSecretsMixin({ name: $._config.pull_secret }) +
      if $.mimir_weekly_release >= 309 then deployment.mixin.spec.template.spec.withImagePullSecretsMixin({ name: $._config.gar_pull_secret })
      else {}
    else {},

  local service = $.core.v1.service,
  config_api_service:
    if $._config.config_api_enabled
    then
      $.util.serviceFor($.config_api_deployment, $._config.service_ignored_labels) +
      service.mixin.spec.withClusterIp('None')
    else {},

  cortex_gateway_args+:: if $._config.config_api_enabled then {
    'mimir.config.endpoint': 'http://config-api:80',
  } else {},
}
