{
  local container = $.core.v1.container,

  // CLI flags for queriers. Also applied to ruler-queriers.
  querier_args::
    $._config.commonConfig +
    $._config.usageStatsConfig +
    $._config.grpcConfig +
    $._config.storageConfig +
    $._config.blocksStorageConfig +
    $._config.queryConfig +
    $._config.queryEngineConfig +
    $._config.ingesterRingClientConfig +
    $._config.queryBlocksStorageConfig +
    $._config.querySchedulerRingClientConfig +
    $.blocks_metadata_caching_config +
    $.bucket_index_config
    {
      target: 'querier',

      'server.http-listen-port': $._config.server_http_port,
      'querier.max-concurrent': $._config.querier_max_concurrency,

      'querier.frontend-address': if !$._config.is_microservices_deployment_mode || $._config.query_scheduler_enabled then null else
        'query-frontend-discovery.%(namespace)s.svc.%(cluster_domain)s:9095' % $._config,
      'querier.frontend-client.grpc-max-send-msg-size': 100 << 20,

      // We request high memory but the Go heap is typically very low (< 100MB) and this causes
      // the GC to trigger continuously. Setting a ballast of 256MB reduces GC.
      'mem-ballast-size-bytes': 1 << 28,  // 256M
    },

  // CLI flags that are applied only to queriers, and not ruler-queriers.
  // Values take precedence over querier_args.
  querier_only_args:: {},

  querier_ports:: $.util.defaultPorts,

  newQuerierContainer(name, args, envmap={})::
    container.new(name, $._images.querier) +
    container.withPorts($.querier_ports) +
    container.withArgsMixin($.util.mapToFlags(args)) +
    $.jaeger_mixin +
    $.util.readinessProbe +
    (if std.length(envmap) > 0 then container.withEnvMap(std.prune(envmap)) else {}) +
    $.util.resourcesRequests('1', '12Gi') +
    $.util.resourcesLimits(null, '24Gi'),

  querier_env_map:: {
    JAEGER_REPORTER_MAX_QUEUE_SIZE: '5000',

    // Dynamically set GOMAXPROCS based on CPU request.
    GOMAXPROCS: std.toString(
      std.ceil(
        std.max(
          $.util.parseCPU($.querier_container.resources.requests.cpu) * 2,
          $.util.parseCPU($.querier_container.resources.requests.cpu) + 4
        ),
      )
    ),
  },

  querier_node_affinity_matchers:: [],

  querier_container::
    self.newQuerierContainer('querier', $.querier_args + $.querier_only_args, $.querier_env_map),

  newQuerierDeployment(name, container, nodeAffinityMatchers=[])::
    local deployment = $.apps.v1.deployment;

    deployment.new(name, 6, [container]) +
    $.newMimirSpreadTopology(name, $._config.querier_topology_spread_max_skew) +
    $.newMimirNodeAffinityMatchers(nodeAffinityMatchers) +
    $.mimirVolumeMounts +
    (if !std.isObject($._config.node_selector) then {} else deployment.mixin.spec.template.spec.withNodeSelectorMixin($._config.node_selector)) +
    deployment.mixin.spec.strategy.rollingUpdate.withMaxSurge('15%') +
    deployment.mixin.spec.strategy.rollingUpdate.withMaxUnavailable(0) +
    // Set a termination grace period greater than query timeout.
    deployment.mixin.spec.template.spec.withTerminationGracePeriodSeconds(180),

  querier_deployment: if !$._config.is_microservices_deployment_mode then null else
    self.newQuerierDeployment('querier', $.querier_container, $.querier_node_affinity_matchers),

  querier_service: if !$._config.is_microservices_deployment_mode then null else
    $.util.serviceFor($.querier_deployment, $._config.service_ignored_labels),

  querier_pdb: if !$._config.is_microservices_deployment_mode then null else
    $.newMimirPdb('querier'),
}
