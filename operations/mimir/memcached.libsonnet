local memcached = import 'memcached/memcached.libsonnet';

memcached {
  memcached+:: {
    cpu_limits:: null,

    deployment: {},

    local statefulSet = $.apps.v1.statefulSet,

    statefulSet:
      statefulSet.new(self.name, 3, [
        self.memcached_container,
        self.memcached_exporter,
      ], []) +
      statefulSet.mixin.spec.withServiceName(self.name) +
      $.util.antiAffinity,

    local service = $.core.v1.service,

    service:
      $.util.serviceFor(self.statefulSet) +
      service.mixin.spec.withClusterIp('None'),
  },

  // Dedicated memcached instance used to cache query results.
  memcached_frontend: $.memcached {
    name: 'memcached-frontend',
    max_item_size: '5m',
  },

  // Dedicated memcached instance used to temporarily cache index lookups.
  memcached_index_queries: if $._config.memcached_index_queries_enabled then
    $.memcached {
      name: 'memcached-index-queries',
      max_item_size: '%dm' % [$._config.memcached_index_queries_max_item_size_mb],
      connection_limit: 16384,
    }
  else {},

  // Memcached instance used to cache chunks.
  memcached_chunks: if $._config.memcached_chunks_enabled then
    $.memcached {
      name: 'memcached',
      max_item_size: '%dm' % [$._config.memcached_chunks_max_item_size_mb],

      // Save memory by more tightly provisioning memcached chunks.
      memory_limit_mb: 6 * 1024,
      overprovision_factor: 1.05,
      connection_limit: 16384,

      local container = $.core.v1.container,
    }
  else {},

  // Memcached instance for caching TSDB blocks metadata (meta.json files, deletion marks, list of users and blocks).
  memcached_metadata: if $._config.memcached_metadata_enabled then
    $.memcached {
      name: 'memcached-metadata',
      max_item_size: '%dm' % [$._config.memcached_metadata_max_item_size_mb],
      connection_limit: 16384,

      // Metadata cache doesn't need much memory.
      memory_limit_mb: 512,

      local statefulSet = $.apps.v1.statefulSet,
      statefulSet+:
        statefulSet.mixin.spec.withReplicas(1),
    },
}
