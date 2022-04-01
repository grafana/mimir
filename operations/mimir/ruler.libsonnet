{
  local container = $.core.v1.container,

  ruler_args::
    $._config.grpcConfig +
    $._config.storageConfig +
    $._config.blocksStorageConfig +
    $._config.queryConfig +
    $._config.queryEngineConfig +
    $._config.ingesterRingClientConfig +
    $._config.rulerClientConfig +
    $._config.rulerLimitsConfig +
    $._config.queryBlocksStorageConfig +
    $.blocks_metadata_caching_config +
    $.bucket_index_config
    {
      target: 'ruler',

      // File path used to store temporary rule files loaded by the Prometheus rule managers.
      'ruler.rule-path': '/rules',

      // Alertmanager configs
      'ruler.alertmanager-url': 'http://alertmanager.%s.svc.cluster.local/alertmanager' % $._config.namespace,

      // Ring Configs
      'ruler.ring.store': 'consul',
      'ruler.ring.consul.hostname': 'consul.%s.svc.cluster.local:8500' % $._config.namespace,

      // Limits
      'server.grpc-max-send-msg-size-bytes': 10 * 1024 * 1024,
      'server.grpc-max-recv-msg-size-bytes': 10 * 1024 * 1024,

      'server.http-listen-port': $._config.server_http_port,

      // Do not extend the replication set on unhealthy (or LEAVING) ingester when "unregister on shutdown"
      // is set to false.
      'distributor.extend-writes': $._config.unregister_ingesters_on_shutdown,
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
      (if !std.isObject($._config.node_selector) then {} else deployment.mixin.spec.template.spec.withNodeSelectorMixin($._config.node_selector)) +
      deployment.mixin.spec.strategy.rollingUpdate.withMaxSurge(0) +
      deployment.mixin.spec.strategy.rollingUpdate.withMaxUnavailable(1) +
      deployment.mixin.spec.template.spec.withTerminationGracePeriodSeconds(600) +
      (if $._config.ruler_allow_multiple_replicas_on_same_node then {} else $.util.antiAffinity) +
      $.util.configVolumeMount($._config.overrides_configmap, $._config.overrides_configmap_mountpoint)
    else {},

  local service = $.core.v1.service,

  ruler_service:
    if $._config.ruler_enabled then
      $.util.serviceFor($.ruler_deployment, $._config.service_ignored_labels)
    else {},
}
