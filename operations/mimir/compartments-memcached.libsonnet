{
  _config+:: {
    // Deploy per-compartment memcached (chunks) and memcached-index-queries, one instance per zone
    // (compartments require multi-AZ memcached with multi-zone routing).
    compartments_memcached_enabled: $._config.compartments_enabled,
    no_compartments_memcached_enabled: !self.compartments_memcached_enabled,

    // Per-compartment replica counts.
    memcached_chunks_replicas_per_compartment: $._config.memcached_chunks_replicas,
    memcached_index_queries_replicas_per_compartment: $._config.memcached_index_queries_replicas,
  },

  // Per-compartment memcached is always deployed zonal (multi-AZ) and routed multi-zone, matching the
  // per-compartment store-gateways and ingesters that also require multi-AZ.
  assert !$._config.compartments_memcached_enabled || $._config.multi_zone_memcached_enabled
         : 'compartments_memcached_enabled requires multi_zone_memcached_enabled',
  assert !$._config.compartments_memcached_enabled || $._config.multi_zone_memcached_routing_enabled
         : 'compartments_memcached_enabled requires multi_zone_memcached_routing_enabled',

  local statefulSet = $.apps.v1.statefulSet,

  local isEnabled = $._config.compartments_memcached_enabled,
  local isNoCompartmentsEnabled = $._config.no_compartments_memcached_enabled,
  local numCompartments = $._config.compartments_read_count,

  local isMultiZoneEnabled = $._config.multi_zone_memcached_enabled,
  local isZoneAEnabled = isMultiZoneEnabled && std.length($._config.multi_zone_availability_zones) >= 1,
  local isZoneBEnabled = isMultiZoneEnabled && std.length($._config.multi_zone_availability_zones) >= 2,
  local isZoneCEnabled = isMultiZoneEnabled && std.length($._config.multi_zone_availability_zones) >= 3,

  local cacheChunksEnabled = $._config.cache_chunks_enabled,
  local cacheIndexQueriesEnabled = $._config.cache_index_queries_enabled,

  // Builders.
  newMemcachedChunksCompartmentZone(zone, compartmentIdx, nodeAffinityMatchers=[])::
    $.newMemcachedChunks('memcached-zone-%s-rc-%d' % [zone, compartmentIdx], nodeAffinityMatchers) + {
      statefulSet+:
        statefulSet.mixin.spec.withReplicas($._config.memcached_chunks_replicas_per_compartment) +
        statefulSet.mixin.spec.template.spec.withTolerationsMixin($.newMimirMultiZoneToleration()),
    },

  newMemcachedIndexQueriesCompartmentZone(zone, compartmentIdx, nodeAffinityMatchers=[])::
    $.newMemcachedIndexQueries('memcached-index-queries-zone-%s-rc-%d' % [zone, compartmentIdx], nodeAffinityMatchers) + {
      statefulSet+:
        statefulSet.mixin.spec.withReplicas($._config.memcached_index_queries_replicas_per_compartment) +
        statefulSet.mixin.spec.template.spec.withTolerationsMixin($.newMimirMultiZoneToleration()),
    },

  // Per-compartment zonal caches.
  memcached_chunks_zone_a_compartments: $.mimirCompartmentsCreateIf(isEnabled && isZoneAEnabled && cacheChunksEnabled, numCompartments, function(c)
    $.newMemcachedChunksCompartmentZone('a', c, $.memcached_chunks_zone_a_node_affinity_matchers)),
  memcached_chunks_zone_b_compartments: $.mimirCompartmentsCreateIf(isEnabled && isZoneBEnabled && cacheChunksEnabled, numCompartments, function(c)
    $.newMemcachedChunksCompartmentZone('b', c, $.memcached_chunks_zone_b_node_affinity_matchers)),
  memcached_chunks_zone_c_compartments: $.mimirCompartmentsCreateIf(isEnabled && isZoneCEnabled && cacheChunksEnabled, numCompartments, function(c)
    $.newMemcachedChunksCompartmentZone('c', c, $.memcached_chunks_zone_c_node_affinity_matchers)),

  memcached_index_queries_zone_a_compartments: $.mimirCompartmentsCreateIf(isEnabled && isZoneAEnabled && cacheIndexQueriesEnabled, numCompartments, function(c)
    $.newMemcachedIndexQueriesCompartmentZone('a', c, $.memcached_index_queries_zone_a_node_affinity_matchers)),
  memcached_index_queries_zone_b_compartments: $.mimirCompartmentsCreateIf(isEnabled && isZoneBEnabled && cacheIndexQueriesEnabled, numCompartments, function(c)
    $.newMemcachedIndexQueriesCompartmentZone('b', c, $.memcached_index_queries_zone_b_node_affinity_matchers)),
  memcached_index_queries_zone_c_compartments: $.mimirCompartmentsCreateIf(isEnabled && isZoneCEnabled && cacheIndexQueriesEnabled, numCompartments, function(c)
    $.newMemcachedIndexQueriesCompartmentZone('c', c, $.memcached_index_queries_zone_c_node_affinity_matchers)),

  // Store-gateway zone-c runs in zone-a's AZ on 2-AZ cells (memcached zone-c isn't deployed there), so it
  // uses zone-a's per-compartment cache.
  local memcachedZone(zone) = if zone == 'c' && !isZoneCEnabled then 'a' else zone,

  // Zonal per-compartment caching config for a store-gateway zone. Bundles both index-cache and
  // chunks-cache addresses.
  local blocksChunksCompartmentZoneCachingConfig(zone, compartmentIdx) =
    (if !cacheIndexQueriesEnabled || !isEnabled then {} else {
       'blocks-storage.bucket-store.index-cache.memcached.addresses':
         'dnssrvnoa+memcached-index-queries-zone-%(zone)s-rc-%(idx)d.%(namespace)s.svc.%(cluster_domain)s:11211' % ($._config { zone: memcachedZone(zone), idx: compartmentIdx }),
     }) +
    (if !cacheChunksEnabled || !isEnabled then {} else {
       'blocks-storage.bucket-store.chunks-cache.memcached.addresses':
         'dnssrvnoa+memcached-zone-%(zone)s-rc-%(idx)d.%(namespace)s.svc.%(cluster_domain)s:11211' % ($._config { zone: memcachedZone(zone), idx: compartmentIdx }),
     }),

  blocks_chunks_zone_a_caching_configs+:: $.mimirCompartmentsOverrides(super.blocks_chunks_zone_a_caching_configs, function(c) blocksChunksCompartmentZoneCachingConfig('a', c)),
  blocks_chunks_zone_b_caching_configs+:: $.mimirCompartmentsOverrides(super.blocks_chunks_zone_b_caching_configs, function(c) blocksChunksCompartmentZoneCachingConfig('b', c)),
  blocks_chunks_zone_c_caching_configs+:: $.mimirCompartmentsOverrides(super.blocks_chunks_zone_c_caching_configs, function(c) blocksChunksCompartmentZoneCachingConfig('c', c)),

  // Null out non-compartment caches.
  memcached_chunks: if !isNoCompartmentsEnabled then null else super.memcached_chunks,
  memcached_index_queries: if !isNoCompartmentsEnabled then null else super.memcached_index_queries,
  memcached_chunks_zone_a: if !isNoCompartmentsEnabled then null else super.memcached_chunks_zone_a,
  memcached_chunks_zone_b: if !isNoCompartmentsEnabled then null else super.memcached_chunks_zone_b,
  memcached_chunks_zone_c: if !isNoCompartmentsEnabled then null else super.memcached_chunks_zone_c,
  memcached_index_queries_zone_a: if !isNoCompartmentsEnabled then null else super.memcached_index_queries_zone_a,
  memcached_index_queries_zone_b: if !isNoCompartmentsEnabled then null else super.memcached_index_queries_zone_b,
  memcached_index_queries_zone_c: if !isNoCompartmentsEnabled then null else super.memcached_index_queries_zone_c,
}
