{
  local container = $.core.v1.container,
  local pvc = $.core.v1.persistentVolumeClaim,
  local statefulSet = $.apps.v1.statefulSet,
  local volumeMount = $.core.v1.volumeMount,
  local envVar = $.core.v1.envVar,

  // The store-gateway runs a statefulset.
  local store_gateway_data_pvc =
    pvc.new() +
    pvc.mixin.spec.resources.withRequests({ storage: $._config.store_gateway_data_disk_size }) +
    pvc.mixin.spec.withAccessModes(['ReadWriteOnce']) +
    pvc.mixin.spec.withStorageClassName($._config.store_gateway_data_disk_class) +
    pvc.mixin.metadata.withName('store-gateway-data'),

  store_gateway_args::
    $._config.commonConfig +
    $._config.usageStatsConfig +
    $._config.grpcConfig +
    $._config.storageConfig +
    $._config.blocksStorageConfig +
    $._config.queryBlocksStorageConfig +
    {
      target: 'store-gateway',

      'server.http-listen-port': $._config.server_http_port,

      // Persist ring tokens so that when the store-gateway will be restarted
      // it will pick the same tokens
      'store-gateway.sharding-ring.tokens-file-path': '/data/tokens',
      'store-gateway.sharding-ring.wait-stability-min-duration': '1m',

      // Do not unregister from ring at shutdown, so that no blocks re-shuffling occurs during rollouts.
      'store-gateway.sharding-ring.unregister-on-shutdown': false,

      // Relax pressure on KV store when running at scale.
      'store-gateway.sharding-ring.heartbeat-period': '1m',
    } +
    (if !$._config.store_gateway_lazy_loading_enabled then {
       'blocks-storage.bucket-store.index-header.lazy-loading-enabled': false,
     } else {}) +
    $.blocks_chunks_concurrency_connection_config +
    $.blocks_chunks_caching_config +
    $.blocks_metadata_caching_config +
    $.bucket_index_config +
    $.mimirRuntimeConfigFile,

  store_gateway_ports:: $.util.defaultPorts,

  store_gateway_env_map:: {
    // Dynamically set GOMAXPROCS based on CPU request.
    GOMAXPROCS: std.toString(
      std.ceil(
        std.max(
          $.util.parseCPU($.store_gateway_container.resources.requests.cpu) * 2,
          $.util.parseCPU($.store_gateway_container.resources.requests.cpu) + 4
        ),
      )
    ),
    // Dynamically set GOMEMLIMIT based on memory request.
    GOMEMLIMIT: std.toString(std.floor($.util.siToBytes($.store_gateway_container.resources.requests.memory))),

    JAEGER_REPORTER_MAX_QUEUE_SIZE: '1000',
  },

  store_gateway_node_affinity_matchers:: [],

  store_gateway_container::
    container.new('store-gateway', $._images.store_gateway) +
    container.withPorts($.store_gateway_ports) +
    container.withArgsMixin($.util.mapToFlags($.store_gateway_args)) +
    container.withVolumeMountsMixin([volumeMount.new('store-gateway-data', '/data')]) +
    $.util.resourcesRequests('1', '12Gi') +
    $.util.resourcesLimits(null, '18Gi') +
    $.util.readinessProbe +
    $.jaeger_mixin,

  newStoreGatewayStatefulSet(name, container, withAntiAffinity=false, nodeAffinityMatchers=[])::
    $.newMimirStatefulSet(name, 3, container, store_gateway_data_pvc) +
    $.newMimirNodeAffinityMatchers(nodeAffinityMatchers) +
    statefulSet.mixin.spec.template.spec.withTerminationGracePeriodSeconds(120) +
    $.mimirVolumeMounts +
    (if withAntiAffinity then $.util.antiAffinity else {}),

  store_gateway_statefulset: if !$._config.is_microservices_deployment_mode then null else
    self.newStoreGatewayStatefulSet(
      'store-gateway',
      $.store_gateway_container + (if std.length($.store_gateway_env_map) > 0 then container.withEnvMap(std.prune($.store_gateway_env_map)) else {}),
      !$._config.store_gateway_allow_multiple_replicas_on_same_node,
      $.store_gateway_node_affinity_matchers,
    ),

  store_gateway_service: if !$._config.is_microservices_deployment_mode then null else
    $.util.serviceFor($.store_gateway_statefulset, $._config.service_ignored_labels),

  store_gateway_pdb: if !$._config.is_microservices_deployment_mode then null else
    // To avoid any disruption in the read path we need at least 1 replica of each
    // block available, so the disruption budget depends on the blocks replication factor.
    local maxUnavailable = if $._config.store_gateway_replication_factor > 1 then $._config.store_gateway_replication_factor - 1 else 1;
    $.newMimirPdb('store-gateway', maxUnavailable),
}
