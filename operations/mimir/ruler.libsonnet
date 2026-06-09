{
  local container = $.core.v1.container,
  local configMap = $.core.v1.configMap,
  local deployment = $.apps.v1.deployment,
  local volume = $.core.v1.volume,
  local volumeMount = $.core.v1.volumeMount,

  // Generate a list of rule file objects (key, tenant, filename).
  // Each key is generated as "rule-N" where N is object` index to avoid restrictions on tenant ID characters in Kubernetes volume mounts.
  local rulesPairs = std.mapWithIndex(
    function(i, pair) { key: 'rule-' + i, tenant: pair[0], filename: pair[1] },
    [
      [tenant, filename]
      for tenant in std.sort(std.objectFields($._config.ruler_local_rules))
      for filename in std.sort(std.objectFields($._config.ruler_local_rules[tenant]))
    ]
  ),

  local hasLocalRules =
    $._config.ruler_storage_backend == 'local' &&
    std.length(rulesPairs) > 0,

  local localRulesConfigMapData = {
    [p.key]: $._config.ruler_local_rules[p.tenant][p.filename]
    for p in rulesPairs
  },

  local localRulesVolumeItems = [
    { key: p.key, path: p.tenant + '/' + p.filename }
    for p in rulesPairs
  ],

  ruler_args::
    $._config.commonConfig +
    $._config.usageStatsConfig +
    $._config.grpcConfig +
    $._config.storageConfig +
    $._config.blocksStorageConfig +
    $._config.rulerStorageConfig +
    $._config.queryConfig +
    $._config.queryEngineConfig +
    $._config.ingesterRingClientConfig +
    $._config.rulerLimitsConfig +
    $._config.queryBlocksStorageConfig +
    $.blocks_metadata_caching_config +
    $.ruler_storage_caching_config +
    $.bucket_index_config
    {
      target: 'ruler',

      // Set the memory ballast to help with extensive CPU usage due to GC at ruler's start. This prevents the autoscaler
      // from adding more replicas around rollouts, and reduces the effect of GC on a restart overall.
      'mem-ballast-size-bytes': 1 << 30,  // 1GiB

      // File path used to store temporary rule files loaded by the Prometheus rule managers.
      'ruler.rule-path': '/rules',

      // Alertmanager configs
      'ruler.alertmanager-url': 'http://alertmanager.%(namespace)s.svc.%(cluster_domain)s/alertmanager' % $._config,

      // Ring Configs
      'ruler.ring.store': 'consul',
      'ruler.ring.consul.hostname': 'consul.%(namespace)s.svc.%(cluster_domain)s:8500' % $._config,

      'server.http-listen-port': $._config.server_http_port,

      // Increased from 2s to 10s in order to accommodate writing large rule results ot he ingester.
      'distributor.remote-timeout': '10s',
    } + if $._config.ingest_storage_enabled then {
      // Set the max buffered bytes in the Kafka client used by the ruler based on the expected max rule evaluation response size,
      // clamping it between 1 GB (default) and 4 GB.
      'ingest-storage.kafka.producer-max-buffered-bytes': std.clamp(10 * $._config.ruler_remote_evaluation_max_query_response_size_bytes, 1024 * 1024 * 1024, 4 * 1024 * 1024 * 1024),
    } else {},

  ruler_env_map:: {},

  ruler_node_affinity_matchers:: [],

  ruler_local_rules_config_map:
    if hasLocalRules then
      configMap.new('ruler-local-rules') +
      configMap.withData(localRulesConfigMapData)
    else {},

  newRulerContainer(name, args, envVarMap={})::
    container.new(name, $._images.ruler) +
    container.withPorts($.util.defaultPorts) +
    container.withArgsMixin($.util.mapToFlags(args)) +
    (if std.length(envVarMap) > 0 then container.withEnvMap(std.prune(envVarMap)) else {}) +
    $.util.resourcesRequests('1', '6Gi') +
    $.util.resourcesLimits('16', '16Gi') +
    $.util.readinessProbe +
    $.tracing_env_mixin +
    (if hasLocalRules then
       container.withVolumeMountsMixin([
         volumeMount.new('ruler-local-rules', $._config.ruler_local_directory),
       ])
     else {}),

  ruler_container::
    if $._config.ruler_enabled then
      $.newRulerContainer('ruler', $.ruler_args, $.ruler_env_map)
    else {},

  ruler_termination_grace_period_seconds:: 600,

  newRulerDeployment(name, container, nodeAffinityMatchers=[])::
    deployment.new(name, 2, [container]) +
    $.newMimirNodeAffinityMatchers(nodeAffinityMatchers) +
    (if !std.isObject($._config.node_selector) then {} else deployment.mixin.spec.template.spec.withNodeSelectorMixin($._config.node_selector)) +
    deployment.mixin.spec.strategy.rollingUpdate.withMaxSurge('50%') +
    deployment.mixin.spec.strategy.rollingUpdate.withMaxUnavailable(0) +
    deployment.mixin.spec.template.spec.withTerminationGracePeriodSeconds($.ruler_termination_grace_period_seconds) +
    $.newMimirSpreadTopology(name, $._config.ruler_querier_topology_spread_max_skew) +
    $.mimirVolumeMounts +
    (if hasLocalRules then
       deployment.mixin.spec.template.spec.withVolumesMixin([
         volume.fromConfigMap('ruler-local-rules', 'ruler-local-rules') +
         { configMap+: { items: localRulesVolumeItems } },
       ])
     else {}),

  ruler_deployment: if !$._config.ruler_enabled then null else
    $.newRulerDeployment('ruler', $.ruler_container, $.ruler_node_affinity_matchers),

  ruler_service: if !$._config.ruler_enabled then null else
    $.util.serviceFor($.ruler_deployment, $._config.service_ignored_labels),

  ruler_pdb: if !$._config.ruler_enabled then null else
    $.newMimirPdb('ruler'),
}
