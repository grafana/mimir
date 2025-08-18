local memcached = import 'memcached/memcached.libsonnet';

memcached {
  local statefulSet = $.apps.v1.statefulSet,
  local service = $.core.v1.service,
  local container = $.core.v1.container,
  local mount = $.core.v1.volumeMount,
  local volume = $.core.v1.volume,

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

  // Dedicated memcached instance used to cache query results.
  memcached_frontend:
    if $._config.cache_frontend_enabled then
      $.memcached {
        name: 'memcached-frontend',
        max_item_size: '%dm' % [$._config.cache_frontend_max_item_size_mb],
        overprovision_factor: 1.05,
        connection_limit: std.toString($._config.cache_frontend_connection_limit),

        statefulSet+:
          statefulSet.mixin.spec.withReplicas($._config.memcached_frontend_replicas),
      }
    else {},

  // Dedicated memcached instance used to temporarily cache index lookups.
  memcached_index_queries:
    if $._config.cache_index_queries_enabled then
      $.memcached {
        name: 'memcached-index-queries',
        max_item_size: '%dm' % [$._config.cache_index_queries_max_item_size_mb],
        overprovision_factor: 1.05,
        connection_limit: std.toString($._config.cache_index_queries_connection_limit),

        statefulSet+:
          statefulSet.mixin.spec.withReplicas($._config.memcached_index_queries_replicas),
      }
    else {},

  // Memcached instance used to cache chunks.
  memcached_chunks:
    if $._config.cache_chunks_enabled then
      $.memcached {
        name: 'memcached',
        max_item_size: '%dm' % [$._config.cache_chunks_max_item_size_mb],

        // Save memory by more tightly provisioning memcached chunks.
        memory_limit_mb: 6 * 1024,
        overprovision_factor: 1.05,
        connection_limit: std.toString($._config.cache_chunks_connection_limit),

        statefulSet+:
          statefulSet.mixin.spec.withReplicas($._config.memcached_chunks_replicas),
      }
    else {},

  // Memcached instance for caching TSDB blocks metadata (meta.json files, deletion marks, list of users and blocks).
  memcached_metadata:
    if $._config.cache_metadata_enabled then
      $.memcached {
        name: 'memcached-metadata',
        max_item_size: '%dm' % [$._config.cache_metadata_max_item_size_mb],
        connection_limit: std.toString($._config.cache_metadata_connection_limit),

        // Metadata cache doesn't need much memory.
        memory_limit_mb: 512,
        overprovision_factor: 1.05,

        statefulSet+:
          statefulSet.mixin.spec.withReplicas($._config.memcached_metadata_replicas),
      }
    else {},
}
