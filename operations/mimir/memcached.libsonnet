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

  // Dummy object to use when caches are disabled.
  local disabledCache = {
    statefulSet: null,
    service: null,
    podDisruptionBudget: null,
    memcached_container:: null,
    memcached_exporter:: null,
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

  memcached_frontend::
    if $._config.cache_frontend_enabled then
      $.newMemcachedFrontend('memcached-frontend', $.memcached_frontend_node_affinity_matchers)
    else disabledCache,

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

  memcached_index_queries::
    if $._config.cache_index_queries_enabled then
      $.newMemcachedIndexQueries('memcached-index-queries', $.memcached_index_queries_node_affinity_matchers)
    else disabledCache,

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

  memcached_chunks::
    if $._config.cache_chunks_enabled then
      $.newMemcachedChunks('memcached', $.memcached_chunks_node_affinity_matchers)
    else disabledCache,

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

  memcached_metadata::
    if $._config.cache_metadata_enabled then
      $.newMemcachedMetadata('memcached-metadata', $.memcached_metadata_node_affinity_matchers)
    else disabledCache,

  // Set top-level objects for consistency with other Mimir workloads
  memcached_frontend_statefulset: $.memcached_frontend.statefulSet,
  memcached_frontend_service: $.memcached_frontend.service,
  memcached_frontend_pod_disruption_budget: $.memcached_frontend.podDisruptionBudget,
  memcached_frontend_memcached_container:: $.memcached_frontend.memcached_container,
  memcached_frontend_exporter_container:: $.memcached_frontend.memcached_exporter,

  memcached_index_queries_statefulset: $.memcached_index_queries.statefulSet,
  memcached_index_queries_service: $.memcached_index_queries.service,
  memcached_index_queries_pod_disruption_budget: $.memcached_index_queries.podDisruptionBudget,
  memcached_index_queries_memcached_container:: $.memcached_index_queries.memcached_container,
  memcached_index_queries_exporter_container:: $.memcached_index_queries.memcached_exporter,

  memcached_chunks_statefulset: $.memcached_chunks.statefulSet,
  memcached_chunks_service: $.memcached_chunks.service,
  memcached_chunks_pod_disruption_budget: $.memcached_chunks.podDisruptionBudget,
  memcached_chunks_memcached_container:: $.memcached_chunks.memcached_container,
  memcached_chunks_exporter_container:: $.memcached_chunks.memcached_exporter,

  memcached_metadata_statefulset: $.memcached_metadata.statefulSet,
  memcached_metadata_service: $.memcached_metadata.service,
  memcached_metadata_pod_disruption_budget: $.memcached_metadata.podDisruptionBudget,
  memcached_metadata_memcached_container:: $.memcached_metadata.memcached_container,
  memcached_metadata_exporter_container:: $.memcached_metadata.memcached_exporter,
}
