// Migration step 6:
// - Route traffic to multi-zone ruler.
(import 'test-multi-az-read-path-migration-step-5.jsonnet') {
  _config+:: {
    multi_zone_ruler_routing_enabled: true,
  },
}
