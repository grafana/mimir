{
  _config+:: {
    // Deploy per-compartment memcached (chunks) and memcached-index-queries. Per-zone when multi-AZ
    // memcached is enabled, otherwise a single per-compartment instance.
    compartments_memcached_enabled: $._config.compartments_enabled,
    no_compartments_memcached_enabled: !self.compartments_memcached_enabled,

    // Per-compartment replica counts.
    memcached_chunks_replicas_per_compartment: $._config.memcached_chunks_replicas,
    memcached_index_queries_replicas_per_compartment: $._config.memcached_index_queries_replicas,
  },

  // Per-compartment memcached must be deployed zonal (multi-AZ), matching the per-compartment store-gateways
  // and ingesters that also require multi-AZ. Single-zone may additionally be enabled during migrations.
  assert !$._config.compartments_memcached_enabled || $._config.multi_zone_memcached_enabled
         : 'compartments_memcached_enabled requires multi_zone_memcached_enabled',
  assert !$._config.compartments_memcached_enabled || !$._config.multi_zone_memcached_routing_enabled || $._config.multi_zone_memcached_enabled
         : 'compartments memcached: multi_zone_memcached_routing_enabled requires multi_zone_memcached_enabled',
  assert !$._config.compartments_memcached_enabled || $._config.multi_zone_memcached_routing_enabled || $._config.single_zone_memcached_enabled
         : 'compartments memcached: single-zone routing (multi_zone_memcached_routing_enabled=false) requires single_zone_memcached_enabled',

  local statefulSet = $.apps.v1.statefulSet,

  local isEnabled = $._config.compartments_memcached_enabled,
  local isNoCompartmentsEnabled = $._config.no_compartments_memcached_enabled,
  local numCompartments = $._config.compartments_read_count,

  local isSingleZoneEnabled = $._config.single_zone_memcached_enabled,
  local isMultiZoneEnabled = $._config.multi_zone_memcached_enabled,
  local isZoneAEnabled = isMultiZoneEnabled && std.length($._config.multi_zone_availability_zones) >= 1,
  local isZoneBEnabled = isMultiZoneEnabled && std.length($._config.multi_zone_availability_zones) >= 2,
  local isZoneCEnabled = isMultiZoneEnabled && std.length($._config.multi_zone_availability_zones) >= 3,

  local cacheChunksEnabled = $._config.cache_chunks_enabled,
  local cacheIndexQueriesEnabled = $._config.cache_index_queries_enabled,

  // Builders.
  newMemcachedChunksCompartment(compartmentIdx, nodeAffinityMatchers=[])::
    $.newMemcachedChunks('memcached-rc-%d' % compartmentIdx, nodeAffinityMatchers) + {
      statefulSet+: statefulSet.mixin.spec.withReplicas($._config.memcached_chunks_replicas_per_compartment),
    },

  newMemcachedChunksCompartmentZone(zone, compartmentIdx, nodeAffinityMatchers=[])::
    $.newMemcachedChunks('memcached-zone-%s-rc-%d' % [zone, compartmentIdx], nodeAffinityMatchers) + {
      statefulSet+:
        statefulSet.mixin.spec.withReplicas($._config.memcached_chunks_replicas_per_compartment) +
        statefulSet.mixin.spec.template.spec.withTolerationsMixin($.newMimirMultiZoneToleration()),
    },

  newMemcachedIndexQueriesCompartment(compartmentIdx, nodeAffinityMatchers=[])::
    $.newMemcachedIndexQueries('memcached-index-queries-rc-%d' % compartmentIdx, nodeAffinityMatchers) + {
      statefulSet+: statefulSet.mixin.spec.withReplicas($._config.memcached_index_queries_replicas_per_compartment),
    },

  newMemcachedIndexQueriesCompartmentZone(zone, compartmentIdx, nodeAffinityMatchers=[])::
    $.newMemcachedIndexQueries('memcached-index-queries-zone-%s-rc-%d' % [zone, compartmentIdx], nodeAffinityMatchers) + {
      statefulSet+:
        statefulSet.mixin.spec.withReplicas($._config.memcached_index_queries_replicas_per_compartment) +
        statefulSet.mixin.spec.template.spec.withTolerationsMixin($.newMimirMultiZoneToleration()),
    },

  // Single-zone per-compartment caches.
  memcached_chunks_compartments: $.mimirCompartmentsCreateIf(isEnabled && isSingleZoneEnabled && cacheChunksEnabled, numCompartments, function(c)
    $.newMemcachedChunksCompartment(c, $.memcached_chunks_node_affinity_matchers)),
  memcached_index_queries_compartments: $.mimirCompartmentsCreateIf(isEnabled && isSingleZoneEnabled && cacheIndexQueriesEnabled, numCompartments, function(c)
    $.newMemcachedIndexQueriesCompartment(c, $.memcached_index_queries_node_affinity_matchers)),

  // Multi-AZ per-compartment caches.
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

  // Resolved caching config for a store-gateway zone: the zonal per-compartment cache when routed
  // multi-zone, otherwise the single non-zonal one. Bundles both index-cache and chunks-cache addresses.
  local blocksChunksCompartmentZoneCachingConfig(zone, compartmentIdx) =
    (if !cacheIndexQueriesEnabled || !isEnabled then {} else {
       'blocks-storage.bucket-store.index-cache.memcached.addresses':
         if $._config.multi_zone_memcached_routing_enabled
         then 'dnssrvnoa+memcached-index-queries-zone-%(zone)s-rc-%(idx)d.%(namespace)s.svc.%(cluster_domain)s:11211' % ($._config { zone: memcachedZone(zone), idx: compartmentIdx })
         else 'dnssrvnoa+memcached-index-queries-rc-%(idx)d.%(namespace)s.svc.%(cluster_domain)s:11211' % ($._config { idx: compartmentIdx }),
     }) +
    (if !cacheChunksEnabled || !isEnabled then {} else {
       'blocks-storage.bucket-store.chunks-cache.memcached.addresses':
         if $._config.multi_zone_memcached_routing_enabled
         then 'dnssrvnoa+memcached-zone-%(zone)s-rc-%(idx)d.%(namespace)s.svc.%(cluster_domain)s:11211' % ($._config { zone: memcachedZone(zone), idx: compartmentIdx })
         else 'dnssrvnoa+memcached-rc-%(idx)d.%(namespace)s.svc.%(cluster_domain)s:11211' % ($._config { idx: compartmentIdx }),
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
