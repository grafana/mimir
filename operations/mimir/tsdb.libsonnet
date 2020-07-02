{
  local pvc = $.core.v1.persistentVolumeClaim,
  local volumeMount = $.core.v1.volumeMount,
  local container = $.core.v1.container,
  local statefulSet = $.apps.v1.statefulSet,
  local service = $.core.v1.service,

  _config+:: {
    // Enforce TSDB storage
    storage_backend: 'none',
    storage_engine: 'tsdb',

    // Allow to configure the store-gateway disk.
    cortex_store_gateway_data_disk_size: '50Gi',
    cortex_store_gateway_data_disk_class: 'standard',

    // Allow to configure the compactor disk.
    cortex_compactor_data_disk_size: '250Gi',
    cortex_compactor_data_disk_class: 'standard',

    // Allow to fine tune compactor.
    cortex_compactor_max_concurrency: 1,
  },

  blocks_chunks_caching_config::
    (
      if $._config.memcached_index_queries_enabled then {
        'experimental.tsdb.bucket-store.index-cache.backend': 'memcached',
        'experimental.tsdb.bucket-store.index-cache.memcached.addresses': 'dnssrvnoa+memcached-index-queries.%(namespace)s.svc.cluster.local:11211' % $._config,
        'experimental.tsdb.bucket-store.index-cache.memcached.timeout': '200ms',
        'experimental.tsdb.bucket-store.index-cache.memcached.max-item-size': $._config.memcached_index_queries_max_item_size_mb * 1024 * 1024,
        'experimental.tsdb.bucket-store.index-cache.memcached.max-async-buffer-size': '25000',
        'experimental.tsdb.bucket-store.index-cache.memcached.max-async-concurrency': '50',
        'experimental.tsdb.bucket-store.index-cache.memcached.max-get-multi-batch-size': '100',
        'experimental.tsdb.bucket-store.index-cache.postings-compression-enabled': 'true',
      } else {}
    ) + (
      if $._config.memcached_chunks_enabled then {
        'experimental.tsdb.bucket-store.chunks-cache.backend': 'memcached',
        'experimental.tsdb.bucket-store.chunks-cache.memcached.addresses': 'dnssrvnoa+memcached.%(namespace)s.svc.cluster.local:11211' % $._config,
        'experimental.tsdb.bucket-store.chunks-cache.memcached.timeout': '200ms',
        'experimental.tsdb.bucket-store.chunks-cache.memcached.max-item-size': $._config.memcached_chunks_max_item_size_mb * 1024 * 1024,
        'experimental.tsdb.bucket-store.chunks-cache.memcached.max-async-buffer-size': '25000',
        'experimental.tsdb.bucket-store.chunks-cache.memcached.max-async-concurrency': '50',
        'experimental.tsdb.bucket-store.chunks-cache.memcached.max-get-multi-batch-size': '100',
      } else {}
    ),

  blocks_metadata_caching_config:: if $._config.memcached_metadata_enabled then {
    'experimental.tsdb.bucket-store.metadata-cache.backend': 'memcached',
    'experimental.tsdb.bucket-store.metadata-cache.memcached.addresses': 'dnssrvnoa+memcached-metadata.%(namespace)s.svc.cluster.local:11211' % $._config,
    'experimental.tsdb.bucket-store.metadata-cache.memcached.timeout': '200ms',
    'experimental.tsdb.bucket-store.metadata-cache.memcached.max-item-size': $._config.memcached_metadata_max_item_size_mb * 1024 * 1024,
    'experimental.tsdb.bucket-store.metadata-cache.memcached.max-async-buffer-size': '25000',
    'experimental.tsdb.bucket-store.metadata-cache.memcached.max-async-concurrency': '50',
    'experimental.tsdb.bucket-store.metadata-cache.memcached.max-get-multi-batch-size': '100',
  } else {},

  querier_args+:: $.blocks_metadata_caching_config,

  // The ingesters should persist TSDB blocks and WAL on a persistent
  // volume in order to be crash resilient.
  local ingester_data_pvc =
    pvc.new() +
    pvc.mixin.spec.resources.withRequests({ storage: '100Gi' }) +
    pvc.mixin.spec.withAccessModes(['ReadWriteOnce']) +
    pvc.mixin.spec.withStorageClassName('fast') +
    pvc.mixin.metadata.withName('ingester-data'),

  ingester_deployment: {},

  ingester_args+:: {
    // Disable TSDB blocks transfer because of persistent volumes
    'ingester.max-transfer-retries': 0,
    'ingester.join-after': '0s',

    // Persist ring tokens so that when the ingester will be restarted
    // it will pick the same tokens
    'ingester.tokens-file-path': '/data/tokens',
  },

  ingester_container+::
    container.withVolumeMountsMixin([
      volumeMount.new('ingester-data', '/data'),
    ]),

  ingester_statefulset:
    statefulSet.new('ingester', 3, [$.ingester_container], ingester_data_pvc) +
    statefulSet.mixin.spec.withServiceName('ingester') +
    statefulSet.mixin.metadata.withNamespace($._config.namespace) +
    statefulSet.mixin.metadata.withLabels({ name: 'ingester' }) +
    statefulSet.mixin.spec.template.metadata.withLabels({ name: 'ingester' } + $.ingester_deployment_labels) +
    statefulSet.mixin.spec.selector.withMatchLabels({ name: 'ingester' }) +
    statefulSet.mixin.spec.template.spec.securityContext.withRunAsUser(0) +
    statefulSet.mixin.spec.template.spec.withTerminationGracePeriodSeconds(600) +
    statefulSet.mixin.spec.updateStrategy.withType('RollingUpdate') +
    $.util.configVolumeMount('overrides', '/etc/cortex') +
    $.util.podPriority('high') +
    $.util.antiAffinity,

  ingester_service:
    $.util.serviceFor($.ingester_statefulset, $.ingester_service_ignored_labels),

  // The compactor runs a statefulset with a single replica, because
  // it does not support horizontal scalability yet.
  local compactor_data_pvc =
    pvc.new() +
    pvc.mixin.spec.resources.withRequests({ storage: $._config.cortex_compactor_data_disk_size }) +
    pvc.mixin.spec.withAccessModes(['ReadWriteOnce']) +
    pvc.mixin.spec.withStorageClassName($._config.cortex_compactor_data_disk_class) +
    pvc.mixin.metadata.withName('compactor-data'),

  compactor_args::
    $._config.storageConfig
    {
      target: 'compactor',

      // Compactor config.
      'compactor.block-ranges': '2h,12h,24h',
      'compactor.data-dir': '/data',
      'compactor.compaction-interval': '30m',
      'compactor.compaction-concurrency': $._config.cortex_compactor_max_concurrency,
    },

  compactor_ports:: $.util.defaultPorts,

  compactor_container::
    container.new('compactor', $._images.compactor) +
    container.withPorts($.compactor_ports) +
    container.withArgsMixin($.util.mapToFlags($.compactor_args)) +
    container.withVolumeMountsMixin([volumeMount.new('compactor-data', '/data')]) +
    $.util.resourcesRequests('1', '6Gi') +
    $.util.resourcesLimits($._config.cortex_compactor_max_concurrency, '6Gi') +
    $.util.readinessProbe +
    $.jaeger_mixin,

  compactor_statefulset:
    statefulSet.new('compactor', 1, [$.compactor_container], compactor_data_pvc) +
    statefulSet.mixin.spec.withServiceName('compactor') +
    statefulSet.mixin.metadata.withNamespace($._config.namespace) +
    statefulSet.mixin.metadata.withLabels({ name: 'compactor' }) +
    statefulSet.mixin.spec.template.metadata.withLabels({ name: 'compactor' }) +
    statefulSet.mixin.spec.selector.withMatchLabels({ name: 'compactor' }) +
    statefulSet.mixin.spec.template.spec.securityContext.withRunAsUser(0) +
    statefulSet.mixin.spec.updateStrategy.withType('RollingUpdate') +
    statefulSet.mixin.spec.template.spec.withTerminationGracePeriodSeconds(900),

  // The store-gateway runs a statefulset.
  local store_gateway_data_pvc =
    pvc.new() +
    pvc.mixin.spec.resources.withRequests({ storage: $._config.cortex_store_gateway_data_disk_size }) +
    pvc.mixin.spec.withAccessModes(['ReadWriteOnce']) +
    pvc.mixin.spec.withStorageClassName($._config.cortex_store_gateway_data_disk_class) +
    pvc.mixin.metadata.withName('store-gateway-data'),

  store_gateway_args::
    $._config.storageConfig
    {
      target: 'store-gateway',

      // Persist ring tokens so that when the store-gateway will be restarted
      // it will pick the same tokens
      'experimental.store-gateway.tokens-file-path': '/data/tokens',
    } + $.blocks_chunks_caching_config + $.blocks_metadata_caching_config,

  store_gateway_ports:: $.util.defaultPorts,

  store_gateway_container::
    container.new('store-gateway', $._images.store_gateway) +
    container.withPorts($.store_gateway_ports) +
    container.withArgsMixin($.util.mapToFlags($.store_gateway_args)) +
    container.withVolumeMountsMixin([volumeMount.new('store-gateway-data', '/data')]) +
    $.util.resourcesRequests('1', '6Gi') +
    $.util.resourcesLimits(null, '6Gi') +
    $.util.readinessProbe +
    $.jaeger_mixin,

  store_gateway_statefulset:
    statefulSet.new('store-gateway', 3, [$.store_gateway_container], store_gateway_data_pvc) +
    statefulSet.mixin.spec.withServiceName('store-gateway') +
    statefulSet.mixin.metadata.withNamespace($._config.namespace) +
    statefulSet.mixin.metadata.withLabels({ name: 'store-gateway' }) +
    statefulSet.mixin.spec.template.metadata.withLabels({ name: 'store-gateway' }) +
    statefulSet.mixin.spec.selector.withMatchLabels({ name: 'store-gateway' }) +
    statefulSet.mixin.spec.template.spec.securityContext.withRunAsUser(0) +
    statefulSet.mixin.spec.updateStrategy.withType('RollingUpdate') +
    statefulSet.mixin.spec.template.spec.withTerminationGracePeriodSeconds(120) +
    // Parallelly scale up/down store-gateway instances instead of starting them
    // one by one. This does NOT affect rolling updates: they will continue to be
    // rolled out one by one (the next pod will be rolled out once the previous is
    // ready).
    statefulSet.mixin.spec.withPodManagementPolicy('Parallel'),

  store_gateway_service:
    $.util.serviceFor($.store_gateway_statefulset),
}
