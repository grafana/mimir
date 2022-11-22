{
  local container = $.core.v1.container,
  local pvc = $.core.v1.persistentVolumeClaim,
  local statefulSet = $.apps.v1.statefulSet,
  local volumeMount = $.core.v1.volumeMount,

  // The store-gateway runs a statefulset.
  local store_gateway_data_pvc =
    pvc.new() +
    pvc.mixin.spec.resources.withRequests({ storage: $._config.store_gateway_data_disk_size }) +
    pvc.mixin.spec.withAccessModes(['ReadWriteOnce']) +
    pvc.mixin.spec.withStorageClassName($._config.store_gateway_data_disk_class) +
    pvc.mixin.metadata.withName('store-gateway-data'),

  store_gateway_args::
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

      // Block index-headers are pre-downloaded but lazy mmaped and loaded at query time.
      'blocks-storage.bucket-store.index-header-lazy-loading-enabled': 'true',
      'blocks-storage.bucket-store.index-header-lazy-loading-idle-timeout': '60m',

      'blocks-storage.bucket-store.max-chunk-pool-bytes': 12 * 1024 * 1024 * 1024,

      // We should keep a number of idle connections equal to the max "get" concurrency,
      // in order to avoid re-opening connections continuously (this would be slower
      // and fill up the conntrack table too).
      //
      // The downside of this approach is that we'll end up with an higher number of
      // active connections to memcached, so we have to make sure connections limit
      // set in memcached is high enough.
      'blocks-storage.bucket-store.index-cache.memcached.max-get-multi-concurrency': 100,
      'blocks-storage.bucket-store.chunks-cache.memcached.max-get-multi-concurrency': 100,
      'blocks-storage.bucket-store.metadata-cache.memcached.max-get-multi-concurrency': 100,
      'blocks-storage.bucket-store.index-cache.memcached.max-idle-connections': $.store_gateway_args['blocks-storage.bucket-store.index-cache.memcached.max-get-multi-concurrency'],
      'blocks-storage.bucket-store.chunks-cache.memcached.max-idle-connections': $.store_gateway_args['blocks-storage.bucket-store.chunks-cache.memcached.max-get-multi-concurrency'],
      'blocks-storage.bucket-store.metadata-cache.memcached.max-idle-connections': $.store_gateway_args['blocks-storage.bucket-store.metadata-cache.memcached.max-get-multi-concurrency'],
    } +
    $.blocks_chunks_caching_config +
    $.blocks_metadata_caching_config +
    $.bucket_index_config +
    $.mimirRuntimeConfigFile,

  store_gateway_ports:: $.util.defaultPorts,

  store_gateway_container::
    container.new('store-gateway', $._images.store_gateway) +
    container.withPorts($.store_gateway_ports) +
    container.withArgsMixin($.util.mapToFlags($.store_gateway_args)) +
    container.withVolumeMountsMixin([volumeMount.new('store-gateway-data', '/data')]) +
    $.util.resourcesRequests('1', '12Gi') +
    $.util.resourcesLimits(null, '18Gi') +
    $.util.readinessProbe +
    $.jaeger_mixin,

  newStoreGatewayStatefulSet(name, container, with_anti_affinity=false)::
    $.newMimirStatefulSet(name, 3, container, store_gateway_data_pvc) +
    statefulSet.mixin.spec.template.spec.withTerminationGracePeriodSeconds(120) +
    $.mimirVolumeMounts +
    (if with_anti_affinity then $.util.antiAffinity else {}),

  store_gateway_statefulset: if !$._config.is_microservices_deployment_mode then null else
    self.newStoreGatewayStatefulSet('store-gateway', $.store_gateway_container, !$._config.store_gateway_allow_multiple_replicas_on_same_node),

  store_gateway_service: if !$._config.is_microservices_deployment_mode then null else
    $.util.serviceFor($.store_gateway_statefulset, $._config.service_ignored_labels),

  store_gateway_pdb: if !$._config.is_microservices_deployment_mode then null else
    // To avoid any disruption in the read path we need at least 1 replica of each
    // block available, so the disruption budget depends on the blocks replication factor.
    local maxUnavailable = if $._config.store_gateway_replication_factor > 1 then $._config.store_gateway_replication_factor - 1 else 1;
    $.newMimirPdb('store-gateway', maxUnavailable),
}
