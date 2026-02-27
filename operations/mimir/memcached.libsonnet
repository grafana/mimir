local memcached = import 'memcached/memcached.libsonnet';

memcached {
  local statefulSet = $.apps.v1.statefulSet,
  local service = $.core.v1.service,
  local container = $.core.v1.container,
  local mount = $.core.v1.volumeMount,
  local volume = $.core.v1.volume,

  memcached_frontend_node_affinity_matchers:: [],
  memcached_index_queries_node_affinity_matchers:: [],
  memcached_chunks_node_affinity_matchers:: [],
  memcached_metadata_node_affinity_matchers:: [],
  memcached_range_vector_splitting_node_affinity_matchers:: [],

  memcached+:: {
    cpu_limits:: null,
    exporter_cpu_requests:: '0.05',
    exporter_cpu_limits:: null,
    exporter_memory_requests:: '50Mi',
    exporter_memory_limits:: '250Mi',
    deployment: {},
    statefulSet+:
      (if !std.isObject($._config.node_selector) then {} else statefulSet.mixin.spec.template.spec.withNodeSelectorMixin($._config.node_selector)),

    service:
      $.util.serviceFor(self.statefulSet) +
      service.mixin.spec.withClusterIp('None'),

    podDisruptionBudget: $.newMimirPdb(self.name),
  },

  // Creates a memcached instance used to cache query results.
  newMemcachedFrontend(name, nodeAffinityMatchers=[])::
    $.memcached {
      name: name,
      max_item_size: '%dm' % [$._config.cache_frontend_max_item_size_mb],
      overprovision_factor: 1.05,
      connection_limit: std.toString($._config.cache_frontend_connection_limit),
      min_ready_seconds: $._config.cache_frontend_min_ready_seconds,

      statefulSet+:
        statefulSet.mixin.spec.withReplicas($._config.memcached_frontend_replicas) +
        $.newMimirNodeAffinityMatchers(nodeAffinityMatchers),
    },

  memcached_frontend:
    if $._config.cache_frontend_enabled then
      $.newMemcachedFrontend('memcached-frontend', $.memcached_frontend_node_affinity_matchers)
    else {},

  // Creates a memcached instance used to temporarily cache index lookups.
  newMemcachedIndexQueries(name, nodeAffinityMatchers=[])::
    $.memcached {
      name: name,
      max_item_size: '%dm' % [$._config.cache_index_queries_max_item_size_mb],
      overprovision_factor: 1.05,
      connection_limit: std.toString($._config.cache_index_queries_connection_limit),
      min_ready_seconds: $._config.cache_index_queries_min_ready_seconds,

      statefulSet+:
        statefulSet.mixin.spec.withReplicas($._config.memcached_index_queries_replicas) +
        $.newMimirNodeAffinityMatchers(nodeAffinityMatchers),
    },

  memcached_index_queries:
    if $._config.cache_index_queries_enabled then
      $.newMemcachedIndexQueries('memcached-index-queries', $.memcached_index_queries_node_affinity_matchers)
    else {},

  // Creates a memcached instance used to cache chunks.
  newMemcachedChunks(name, nodeAffinityMatchers=[])::
    $.memcached {
      name: name,
      max_item_size: '%dm' % [$._config.cache_chunks_max_item_size_mb],

      // Save memory by more tightly provisioning memcached chunks.
      memory_limit_mb: 6 * 1024,
      overprovision_factor: 1.05,
      connection_limit: std.toString($._config.cache_chunks_connection_limit),
      min_ready_seconds: $._config.cache_chunks_min_ready_seconds,

      statefulSet+:
        statefulSet.mixin.spec.withReplicas($._config.memcached_chunks_replicas) +
        $.newMimirNodeAffinityMatchers(nodeAffinityMatchers),
    },

  memcached_chunks:
    if $._config.cache_chunks_enabled then
      $.newMemcachedChunks('memcached', $.memcached_chunks_node_affinity_matchers)
    else {},

  // Creates a memcached instance for caching TSDB blocks metadata (meta.json files, deletion marks, list of users and blocks).
  newMemcachedMetadata(name, nodeAffinityMatchers=[])::
    $.memcached {
      name: name,
      max_item_size: '%dm' % [$._config.cache_metadata_max_item_size_mb],
      connection_limit: std.toString($._config.cache_metadata_connection_limit),
      min_ready_seconds: $._config.cache_metadata_min_ready_seconds,

      // Metadata cache doesn't need much memory.
      memory_limit_mb: 512,
      overprovision_factor: 1.05,

      statefulSet+:
        statefulSet.mixin.spec.withReplicas($._config.memcached_metadata_replicas) +
        $.newMimirNodeAffinityMatchers(nodeAffinityMatchers),
    },

  memcached_metadata:
    if $._config.cache_metadata_enabled then
      $.newMemcachedMetadata('memcached-metadata', $.memcached_metadata_node_affinity_matchers)
    else {},

  // Creates a memcached instance used to cache intermediate results from range vector splitting queries.
  newMemcachedRangeVectorSplitting(name, nodeAffinityMatchers=[])::
    $.memcached {
      name: name,
      max_item_size: '%dm' % [$._config.cache_range_vector_splitting_max_item_size_mb],
      overprovision_factor: 1.05,
      connection_limit: std.toString($._config.cache_range_vector_splitting_connection_limit),
      min_ready_seconds: $._config.cache_range_vector_splitting_min_ready_seconds,

      statefulSet+:
        statefulSet.mixin.spec.withReplicas($._config.memcached_range_vector_splitting_replicas) +
        $.newMimirNodeAffinityMatchers(nodeAffinityMatchers),
    },

  memcached_range_vector_splitting:
    if $._config.query_engine_range_vector_splitting_enabled then
      $.newMemcachedRangeVectorSplitting('memcached-range-vector-splitting', $.memcached_range_vector_splitting_node_affinity_matchers)
    else {},
}
