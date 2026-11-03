// Enables zone-aware tenant shuffle sharding on the multi-zone rulers.
(import 'test-multi-az-read-path.jsonnet') {
  _config+:: {
    multi_zone_ruler_ring_zone_enabled: true,
    multi_zone_ruler_zone_awareness_enabled: true,
  },
}
