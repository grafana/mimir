{
  local container = $.core.v1.container,
  local containerPort = $.core.v1.containerPort,
  local deployment = $.apps.v1.deployment,
  local service = $.core.v1.service,

  // When write requests go through distributors via gRPC, we want gRPC clients to re-resolve the distributors DNS
  // endpoint before the distributor process is terminated, in order to avoid any failures during graceful shutdown.
  // To achieve it, we set a shutdown delay greater than the gRPC max connection age, and we set an even higher
  // termination grace period (to give some extra buffer to the process to gracefully shutdown).
  local grpc_max_connection_age_seconds = 60,
  local shutdown_delay_seconds = 90,
  local termination_grace_period_seconds = shutdown_delay_seconds + 10,

  distributor_args::
    $._config.commonConfig +
    $._config.usageStatsConfig +
    $._config.grpcConfig +
    $._config.grpcIngressConfig +
    $._config.ingesterRingClientConfig +
    $._config.distributorLimitsConfig +
    {
      target: 'distributor',

      'distributor.ha-tracker.enable': true,
      'distributor.ha-tracker.enable-for-all-users': true,
      'distributor.ha-tracker.store': 'etcd',
      'distributor.ha-tracker.etcd.endpoints': 'etcd-client.%(namespace)s.svc.%(cluster_domain)s:2379' % $._config,
      'distributor.ha-tracker.prefix': 'prom_ha/',

      // The memory requests are 2G, and we barely use 100M.
      // By adding a ballast of 1G, we can drastically reduce GC, but also keep the usage at
      // around 1.25G, reducing the 99%ile.
      'mem-ballast-size-bytes': 1 << 30,  // 1GB

      'server.http-listen-port': $._config.server_http_port,

      // The ingestion rate global limit requires the distributors to form a ring.
      'distributor.ring.store': 'consul',
      'distributor.ring.consul.hostname': 'consul.%(namespace)s.svc.%(cluster_domain)s:8500' % $._config,
      'distributor.ring.prefix': '',

      // Relax pressure on KV store when running at scale.
      'distributor.ring.heartbeat-period': '1m',
      'distributor.ring.heartbeat-timeout': '4m',

      // Allow DNS changes to propagate before killing off distributor
      // to avoid connection failures in cortex-gw and therefore 5xx writes.
      // Issue: https://github.com/grafana/mimir-squad/issues/454
      'shutdown-delay': shutdown_delay_seconds + 's',
      'server.grpc.keepalive.max-connection-age': grpc_max_connection_age_seconds + 's',
    } + $.mimirRuntimeConfigFile,

  distributor_ports:: $.util.defaultPorts,

  distributor_env_map:: {
    // Dynamically set GOMAXPROCS based on CPU request.
    GOMAXPROCS: std.toString(
      std.ceil(
        std.max(
          8,  // Always run on at least 8 gothreads, so that at least 2 of them (25%) are dedicated to GC.
          $.util.parseCPU($.distributor_container.resources.requests.cpu) * 2
        ),
      )
    ),
    JAEGER_REPORTER_MAX_QUEUE_SIZE: std.toString(1000),
  },

  distributor_node_affinity_matchers:: [],

  newDistributorContainer(name, args, envVarMap={})::
    container.new(name, $._images.distributor) +
    container.withPorts($.distributor_ports) +
    container.withArgsMixin($.util.mapToFlags(args)) +
    (if std.length(envVarMap) > 0 then container.withEnvMap(std.prune(envVarMap)) else {}) +
    $.util.resourcesRequests('2', '2Gi') +
    $.util.resourcesLimits(null, '4Gi') +
    $.util.readinessProbe +
    $.jaeger_mixin,

  distributor_container::
    $.newDistributorContainer('distributor', $.distributor_args, $.distributor_env_map),

  newDistributorDeployment(name, container, nodeAffinityMatchers=[])::
    deployment.new(name, 3, [container]) +
    $.newMimirSpreadTopology(name, $._config.distributor_topology_spread_max_skew) +
    $.mimirVolumeMounts +
    (if !std.isObject($._config.node_selector) then {} else deployment.mixin.spec.template.spec.withNodeSelectorMixin($._config.node_selector)) +
    $.newMimirNodeAffinityMatchers(nodeAffinityMatchers) +
    deployment.mixin.spec.strategy.rollingUpdate.withMaxSurge('15%') +
    deployment.mixin.spec.strategy.rollingUpdate.withMaxUnavailable(0) +
    deployment.mixin.spec.template.spec.withTerminationGracePeriodSeconds(termination_grace_period_seconds),

  distributor_deployment: if !$._config.is_microservices_deployment_mode then null else
    $.newDistributorDeployment('distributor', $.distributor_container, $.distributor_node_affinity_matchers),

  distributor_service: if !$._config.is_microservices_deployment_mode then null else
    $.util.serviceFor($.distributor_deployment, $._config.service_ignored_labels) +
    service.mixin.spec.withClusterIp('None'),

  distributor_pdb: if !$._config.is_microservices_deployment_mode then null else
    $.newMimirPdb('distributor'),
}
