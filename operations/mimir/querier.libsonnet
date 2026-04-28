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
    $.range_vector_splitting_caching_config +
    $.bucket_index_config +
    $.querierUseQuerySchedulerArgs('query-scheduler') +
    {
      target: 'querier',

      'server.http-listen-port': $._config.server_http_port,
      'querier.max-concurrent': $._config.querier_max_concurrency,

      'querier.frontend-client.grpc-max-send-msg-size': 100 << 20,

      // We request high memory but the Go heap is typically very low (< 100MB) and this causes
      // the GC to trigger continuously. Setting a ballast of 256MB reduces GC.
      'mem-ballast-size-bytes': 1 << 28,  // 256M

      'querier.store-gateway-client.grpc-max-recv-msg-size': $._config.store_gateway_grpc_max_query_response_size_bytes,
    },

  // CLI flags that are applied only to queriers, and not ruler-queriers.
  // Values take precedence over querier_args.
  querier_only_args:: {},

  // Timeout validation for querier
  local validateQuerierTimeouts() =
    local q_timeout = if 'querier.timeout' in $.querier_args then
      $.util.parseDuration($.querier_args['querier.timeout'])
    else
      $.util.getFlagDefaultSeconds('querier.timeout');

    local q_write_timeout = if 'server.http-write-timeout' in $.querier_args then
      $.util.parseDuration($.querier_args['server.http-write-timeout'])
    else
      $.util.getFlagDefaultSeconds('server.http-write-timeout');

    assert q_timeout == null || q_write_timeout == null || q_timeout <= q_write_timeout :
           'querier: querier.timeout (%s) must be less than or equal to server.http-write-timeout (%s)' %
           [
      if 'querier.timeout' in $.querier_args then $.querier_args['querier.timeout'] else ('default: %ss' % $.util.getFlagDefaultSeconds('querier.timeout')),
      if 'server.http-write-timeout' in $.querier_args then $.querier_args['server.http-write-timeout'] else ('default: %ss' % $.util.getFlagDefaultSeconds('server.http-write-timeout')),
    ];

    true,

  // Execute validation
  querier_timeout_validation:: validateQuerierTimeouts(),

  querier_ports:: $.util.defaultPorts,

  newQuerierContainer(name, args, envmap={})::
    container.new(name, $._images.querier) +
    container.withPorts($.querier_ports) +
    container.withArgsMixin($.util.mapToFlags(args)) +
    $.tracing_env_mixin +
    $.util.readinessProbe +
    (if std.length(envmap) > 0 then container.withEnvMap(std.prune(envmap)) else {}) +
    $.util.resourcesRequests('1', '12Gi') +
    $.util.resourcesLimits(null, '24Gi'),

  querier_env_map:: {
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

  querier_deployment:
    self.newQuerierDeployment('querier', $.querier_container, $.querier_node_affinity_matchers),

  querier_service:
    $.util.serviceFor($.querier_deployment, $._config.service_ignored_labels),

  querier_pdb:
    $.newMimirPdb('querier'),
}
