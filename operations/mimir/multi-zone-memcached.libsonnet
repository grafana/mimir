{
  _config+:: {
    // Allow to configure whether memcached should be deployed in single or multi-zone.
    // Multi-zone and single-zone can be enabled at the same time during migrations.
    single_zone_memcached_enabled: !$._config.multi_zone_memcached_enabled,
    multi_zone_memcached_enabled: $._config.multi_zone_read_path_enabled,

    // Controls whether the traffic should be routed to multi-zone memcached.
    // This setting can be used by downstream projects during migrations from single to multi-zone.
    multi_zone_memcached_routing_enabled: $._config.multi_zone_memcached_enabled,

    // Per-zone replica counts for memcached deployments. Each defaults to the single-zone value.
    memcached_frontend_zone_a_replicas: $._config.memcached_frontend_replicas,
    memcached_frontend_zone_b_replicas: $._config.memcached_frontend_replicas,
    memcached_frontend_zone_c_replicas: $._config.memcached_frontend_replicas,

    memcached_index_queries_zone_a_replicas: $._config.memcached_index_queries_replicas,
    memcached_index_queries_zone_b_replicas: $._config.memcached_index_queries_replicas,
    memcached_index_queries_zone_c_replicas: $._config.memcached_index_queries_replicas,

    memcached_chunks_zone_a_replicas: $._config.memcached_chunks_replicas,
    memcached_chunks_zone_b_replicas: $._config.memcached_chunks_replicas,
    memcached_chunks_zone_c_replicas: $._config.memcached_chunks_replicas,

    memcached_metadata_zone_a_replicas: $._config.memcached_metadata_replicas,
    memcached_metadata_zone_b_replicas: $._config.memcached_metadata_replicas,
    memcached_metadata_zone_c_replicas: $._config.memcached_metadata_replicas,

    memcached_range_vector_splitting_zone_a_replicas: $._config.memcached_range_vector_splitting_replicas,
    memcached_range_vector_splitting_zone_b_replicas: $._config.memcached_range_vector_splitting_replicas,
    memcached_range_vector_splitting_zone_c_replicas: $._config.memcached_range_vector_splitting_replicas,
  },

  local container = $.core.v1.container,
  local statefulSet = $.apps.v1.statefulSet,
  local service = $.core.v1.service,
  local servicePort = $.core.v1.servicePort,

  local isSingleZoneEnabled = $._config.single_zone_memcached_enabled,
  local isMultiZoneEnabled = $._config.multi_zone_memcached_enabled,
  local isZoneAEnabled = isMultiZoneEnabled && std.length($._config.multi_zone_availability_zones) >= 1,
  local isZoneBEnabled = isMultiZoneEnabled && std.length($._config.multi_zone_availability_zones) >= 2,
  local isZoneCEnabled = isMultiZoneEnabled && std.length($._config.multi_zone_availability_zones) >= 3,

  // Frontend cache.
  memcached_frontend_zone_a_node_affinity_matchers:: $.memcached_frontend_node_affinity_matchers + [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[0])],
  memcached_frontend_zone_b_node_affinity_matchers:: $.memcached_frontend_node_affinity_matchers + [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[1])],
  memcached_frontend_zone_c_node_affinity_matchers:: $.memcached_frontend_node_affinity_matchers + [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[2])],

  newMemcachedFrontendZone(zone, nodeAffinityMatchers=[], replicas=2)::
    $.newMemcachedFrontend('memcached-frontend-zone-%s' % zone, nodeAffinityMatchers) + {
      statefulSet+:
        statefulSet.mixin.spec.withReplicas(replicas) +
        statefulSet.mixin.spec.template.spec.withTolerationsMixin($.newMimirMultiZoneToleration()),
    },

  memcached_frontend_zone_a: if !isZoneAEnabled then null else
    $.newMemcachedFrontendZone('a', $.memcached_frontend_zone_a_node_affinity_matchers, $._config.memcached_frontend_zone_a_replicas),

  memcached_frontend_zone_b: if !isZoneBEnabled then null else
    $.newMemcachedFrontendZone('b', $.memcached_frontend_zone_b_node_affinity_matchers, $._config.memcached_frontend_zone_b_replicas),

  memcached_frontend_zone_c: if !isZoneCEnabled then null else
    $.newMemcachedFrontendZone('c', $.memcached_frontend_zone_c_node_affinity_matchers, $._config.memcached_frontend_zone_c_replicas),

  // Index queries cache.
  memcached_index_queries_zone_a_node_affinity_matchers:: $.memcached_index_queries_node_affinity_matchers + [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[0])],
  memcached_index_queries_zone_b_node_affinity_matchers:: $.memcached_index_queries_node_affinity_matchers + [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[1])],
  memcached_index_queries_zone_c_node_affinity_matchers:: $.memcached_index_queries_node_affinity_matchers + [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[2])],

  newMemcachedIndexQueriesZone(zone, nodeAffinityMatchers=[], replicas=2)::
    $.newMemcachedIndexQueries('memcached-index-queries-zone-%s' % zone, nodeAffinityMatchers) + {
      statefulSet+:
        statefulSet.mixin.spec.withReplicas(replicas) +
        statefulSet.mixin.spec.template.spec.withTolerationsMixin($.newMimirMultiZoneToleration()),
    },

  memcached_index_queries_zone_a: if !isZoneAEnabled then null else
    $.newMemcachedIndexQueriesZone('a', $.memcached_index_queries_zone_a_node_affinity_matchers, $._config.memcached_index_queries_zone_a_replicas),

  memcached_index_queries_zone_b: if !isZoneBEnabled then null else
    $.newMemcachedIndexQueriesZone('b', $.memcached_index_queries_zone_b_node_affinity_matchers, $._config.memcached_index_queries_zone_b_replicas),

  memcached_index_queries_zone_c: if !isZoneCEnabled then null else
    $.newMemcachedIndexQueriesZone('c', $.memcached_index_queries_zone_c_node_affinity_matchers, $._config.memcached_index_queries_zone_c_replicas),

  // Chunks cache.
  memcached_chunks_zone_a_node_affinity_matchers:: $.memcached_chunks_node_affinity_matchers + [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[0])],
  memcached_chunks_zone_b_node_affinity_matchers:: $.memcached_chunks_node_affinity_matchers + [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[1])],
  memcached_chunks_zone_c_node_affinity_matchers:: $.memcached_chunks_node_affinity_matchers + [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[2])],

  newMemcachedChunksZone(zone, nodeAffinityMatchers=[], replicas=2)::
    $.newMemcachedChunks('memcached-zone-%s' % zone, nodeAffinityMatchers) + {
      statefulSet+:
        statefulSet.mixin.spec.withReplicas(replicas) +
        statefulSet.mixin.spec.template.spec.withTolerationsMixin($.newMimirMultiZoneToleration()),
    },

  memcached_chunks_zone_a: if !isZoneAEnabled then null else
    $.newMemcachedChunksZone('a', $.memcached_chunks_zone_a_node_affinity_matchers, $._config.memcached_chunks_zone_a_replicas),

  memcached_chunks_zone_b: if !isZoneBEnabled then null else
    $.newMemcachedChunksZone('b', $.memcached_chunks_zone_b_node_affinity_matchers, $._config.memcached_chunks_zone_b_replicas),

  memcached_chunks_zone_c: if !isZoneCEnabled then null else
    $.newMemcachedChunksZone('c', $.memcached_chunks_zone_c_node_affinity_matchers, $._config.memcached_chunks_zone_c_replicas),

  // Metadata cache.
  memcached_metadata_zone_a_node_affinity_matchers:: $.memcached_metadata_node_affinity_matchers + [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[0])],
  memcached_metadata_zone_b_node_affinity_matchers:: $.memcached_metadata_node_affinity_matchers + [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[1])],
  memcached_metadata_zone_c_node_affinity_matchers:: $.memcached_metadata_node_affinity_matchers + [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[2])],

  newMemcachedMetadataZone(zone, nodeAffinityMatchers=[], replicas=2)::
    $.newMemcachedMetadata('memcached-metadata-zone-%s' % zone, nodeAffinityMatchers) + {
      statefulSet+:
        statefulSet.mixin.spec.withReplicas(replicas) +
        statefulSet.mixin.spec.template.spec.withTolerationsMixin($.newMimirMultiZoneToleration()) +

        // Add a label shared across all zones, used by the cross-zone service.
        statefulSet.mixin.spec.template.metadata.withLabelsMixin({ 'cache-group': 'metadata' }),
    },

  memcached_metadata_zone_a: if !isZoneAEnabled then null else
    $.newMemcachedMetadataZone('a', $.memcached_metadata_zone_a_node_affinity_matchers, $._config.memcached_metadata_zone_a_replicas),

  memcached_metadata_zone_b: if !isZoneBEnabled then null else
    $.newMemcachedMetadataZone('b', $.memcached_metadata_zone_b_node_affinity_matchers, $._config.memcached_metadata_zone_b_replicas),

  memcached_metadata_zone_c: if !isZoneCEnabled then null else
    $.newMemcachedMetadataZone('c', $.memcached_metadata_zone_c_node_affinity_matchers, $._config.memcached_metadata_zone_c_replicas),

  memcached_metadata_multi_zone_service: if !isMultiZoneEnabled then null else
    local name = 'memcached-metadata-multi-zone';
    local labels = { 'cache-group': 'metadata' };
    local ports = [
      servicePort.newNamed(name='memcached-client', port=11211, targetPort=11211) +
      servicePort.withProtocol('TCP'),
    ];

    service.new(name, labels, ports) +
    service.mixin.metadata.withLabels({ name: name }) +
    // We use a headless service because this K8S service is used for service discovery.
    service.mixin.spec.withClusterIp('None'),

  // Range vector splitting cache.
  memcached_range_vector_splitting_zone_a_node_affinity_matchers:: $.memcached_range_vector_splitting_node_affinity_matchers + [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[0])],
  memcached_range_vector_splitting_zone_b_node_affinity_matchers:: $.memcached_range_vector_splitting_node_affinity_matchers + [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[1])],
  memcached_range_vector_splitting_zone_c_node_affinity_matchers:: $.memcached_range_vector_splitting_node_affinity_matchers + [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[2])],

  newMemcachedRangeVectorSplittingZone(zone, nodeAffinityMatchers=[], replicas=2)::
    $.newMemcachedRangeVectorSplitting('memcached-range-vector-splitting-zone-%s' % zone, nodeAffinityMatchers) + {
      statefulSet+:
        statefulSet.mixin.spec.withReplicas(replicas) +
        statefulSet.mixin.spec.template.spec.withTolerationsMixin($.newMimirMultiZoneToleration()),
    },

  memcached_range_vector_splitting_zone_a: if !isZoneAEnabled || !$._config.query_engine_range_vector_splitting_enabled then null else
    $.newMemcachedRangeVectorSplittingZone('a', $.memcached_range_vector_splitting_zone_a_node_affinity_matchers, $._config.memcached_range_vector_splitting_zone_a_replicas),

  memcached_range_vector_splitting_zone_b: if !isZoneBEnabled || !$._config.query_engine_range_vector_splitting_enabled then null else
    $.newMemcachedRangeVectorSplittingZone('b', $.memcached_range_vector_splitting_zone_b_node_affinity_matchers, $._config.memcached_range_vector_splitting_zone_b_replicas),

  memcached_range_vector_splitting_zone_c: if !isZoneCEnabled || !$._config.query_engine_range_vector_splitting_enabled then null else
    $.newMemcachedRangeVectorSplittingZone('c', $.memcached_range_vector_splitting_zone_c_node_affinity_matchers, $._config.memcached_range_vector_splitting_zone_c_replicas),

  // Remove single-zone deployment when it's disabled.
  memcached_frontend: if !isSingleZoneEnabled then null else super.memcached_frontend,
  memcached_index_queries: if !isSingleZoneEnabled then null else super.memcached_index_queries,
  memcached_chunks: if !isSingleZoneEnabled then null else super.memcached_chunks,
  memcached_metadata: if !isSingleZoneEnabled then null else super.memcached_metadata,
  memcached_range_vector_splitting: if !isSingleZoneEnabled then null else super.memcached_range_vector_splitting,

  // Shared config between Mimir components.
  local memcachedFrontendClientZoneAddress(zone) =
    'dnssrvnoa+memcached-frontend-zone-%(zone)s.%(namespace)s.svc.%(cluster_domain)s:11211' % ($._config { zone: zone }),

  local memcachedIndexQueriesClientZoneAddress(zone) =
    'dnssrvnoa+memcached-index-queries-zone-%(zone)s.%(namespace)s.svc.%(cluster_domain)s:11211' % ($._config { zone: zone }),

  local memcachedChunksClientZoneAddress(zone) =
    'dnssrvnoa+memcached-zone-%(zone)s.%(namespace)s.svc.%(cluster_domain)s:11211' % ($._config { zone: zone }),

  local memcachedMetadataClientZoneAddress(zone) =
    'dnssrvnoa+memcached-metadata-zone-%(zone)s.%(namespace)s.svc.%(cluster_domain)s:11211' % ($._config { zone: zone }),

  local memcachedMetadataClientMultiZoneAddress() =
    'dnssrvnoa+memcached-metadata-multi-zone.%(namespace)s.svc.%(cluster_domain)s:11211' % $._config,

  local memcachedRangeVectorSplittingClientZoneAddress(zone) =
    'dnssrvnoa+memcached-range-vector-splitting-zone-%(zone)s.%(namespace)s.svc.%(cluster_domain)s:11211' % ($._config { zone: zone }),

  rangeVectorSplittingZoneCachingConfig(zone):: if !$._config.query_engine_range_vector_splitting_enabled || !$._config.multi_zone_memcached_routing_enabled then {} else {
    'querier.mimir-query-engine.range-vector-splitting.memcached.addresses': memcachedRangeVectorSplittingClientZoneAddress(zone),
  },

  local queryFrontendZoneCachingConfig(zone) = if !$._config.cache_frontend_enabled || !$._config.multi_zone_memcached_routing_enabled then {} else {
    'query-frontend.results-cache.memcached.addresses': memcachedFrontendClientZoneAddress(zone),
  },

  query_frontend_zone_a_caching_config:: queryFrontendZoneCachingConfig('a'),
  query_frontend_zone_b_caching_config:: queryFrontendZoneCachingConfig('b'),
  query_frontend_zone_c_caching_config:: queryFrontendZoneCachingConfig('c'),

  local blocksMetadataZoneCachingConfig(zone) = if !$._config.cache_metadata_enabled || !$._config.multi_zone_memcached_routing_enabled then {} else {
    'blocks-storage.bucket-store.metadata-cache.memcached.addresses': memcachedMetadataClientZoneAddress(zone),
  },

  blocks_metadata_zone_a_caching_config:: blocksMetadataZoneCachingConfig('a'),
  blocks_metadata_zone_b_caching_config:: blocksMetadataZoneCachingConfig('b'),
  blocks_metadata_zone_c_caching_config:: blocksMetadataZoneCachingConfig('c'),

  local blocksChunksZoneCachingConfig(zone) = (
    if !$._config.cache_index_queries_enabled || !$._config.multi_zone_memcached_routing_enabled then {} else {
      'blocks-storage.bucket-store.index-cache.memcached.addresses': memcachedIndexQueriesClientZoneAddress(zone),
    }
  ) + (
    if !$._config.cache_chunks_enabled || !$._config.multi_zone_memcached_routing_enabled then {} else {
      'blocks-storage.bucket-store.chunks-cache.memcached.addresses': memcachedChunksClientZoneAddress(zone),
    }
  ),

  blocks_chunks_zone_a_caching_config:: blocksChunksZoneCachingConfig('a'),
  blocks_chunks_zone_b_caching_config:: blocksChunksZoneCachingConfig('b'),
  blocks_chunks_zone_c_caching_config:: blocksChunksZoneCachingConfig('c'),

  local rulerStorageZoneCachingConfig(zone) = if !$._config.cache_metadata_enabled || !$._config.multi_zone_memcached_routing_enabled then {} else {
    // When memcached is deployed multi-zone, the rulers still need a cross-zone cache because
    // of the rules' explicit cache invalidation.
    'ruler-storage.cache.memcached.addresses': memcachedMetadataClientMultiZoneAddress(),
  },

  ruler_storage_zone_a_caching_config:: rulerStorageZoneCachingConfig('a'),
  ruler_storage_zone_b_caching_config:: rulerStorageZoneCachingConfig('b'),
  ruler_storage_zone_c_caching_config:: rulerStorageZoneCachingConfig('c'),
}
