{
  local container = $.core.v1.container,
  local podDisruptionBudget = $.policy.v1beta1.podDisruptionBudget,
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
    $._config.grpcConfig +
    $._config.storageConfig +
    $._config.blocksStorageConfig +
    $._config.queryBlocksStorageConfig +
    {
      target: 'store-gateway',

      'server.http-listen-port': $._config.server_http_port,

      'runtime-config.file': '%s/overrides.yaml' % $._config.overrides_configmap_mountpoint,

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

      // Queriers will not query store for data younger than 12h (see -querier.query-store-after).
      // Store-gateways don't need to load blocks with very most recent data. We use 2h buffer to
      // make sure that blocks are ready for querying when needed.
      'blocks-storage.bucket-store.ignore-blocks-within': '10h',
    } +
    $.blocks_chunks_caching_config +
    $.blocks_metadata_caching_config +
    $.bucket_index_config,

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
    statefulSet.new(name, 3, [container], store_gateway_data_pvc) +
    statefulSet.mixin.spec.withServiceName(name) +
    statefulSet.mixin.metadata.withNamespace($._config.namespace) +
    statefulSet.mixin.metadata.withLabels({ name: name }) +
    statefulSet.mixin.spec.template.metadata.withLabels({ name: name }) +
    statefulSet.mixin.spec.selector.withMatchLabels({ name: name }) +
    statefulSet.mixin.spec.template.spec.securityContext.withRunAsUser(0) +
    (if !std.isObject($._config.node_selector) then {} else statefulSet.mixin.spec.template.spec.withNodeSelectorMixin($._config.node_selector)) +
    statefulSet.mixin.spec.updateStrategy.withType('RollingUpdate') +
    statefulSet.mixin.spec.template.spec.withTerminationGracePeriodSeconds(120) +
    // Parallelly scale up/down store-gateway instances instead of starting them
    // one by one. This does NOT affect rolling updates: they will continue to be
    // rolled out one by one (the next pod will be rolled out once the previous is
    // ready).
    statefulSet.mixin.spec.withPodManagementPolicy('Parallel') +
    $.util.configVolumeMount($._config.overrides_configmap, $._config.overrides_configmap_mountpoint) +
    (if with_anti_affinity then $.util.antiAffinity else {}),

  store_gateway_statefulset: self.newStoreGatewayStatefulSet('store-gateway', $.store_gateway_container, !$._config.store_gateway_allow_multiple_replicas_on_same_node),

  store_gateway_service:
    $.util.serviceFor($.store_gateway_statefulset, $._config.service_ignored_labels),

  store_gateway_pdb:
    podDisruptionBudget.new() +
    podDisruptionBudget.mixin.metadata.withName('store-gateway-pdb') +
    podDisruptionBudget.mixin.metadata.withLabels({ name: 'store-gateway-pdb' }) +
    podDisruptionBudget.mixin.spec.selector.withMatchLabels({ name: 'store-gateway' }) +
    // To avoid any disruption in the read path we need at least 1 replica of each
    // block available, so the disruption budget depends on the blocks replication factor.
    podDisruptionBudget.mixin.spec.withMaxUnavailable(if $._config.store_gateway_replication_factor > 1 then $._config.store_gateway_replication_factor - 1 else 1),
}
