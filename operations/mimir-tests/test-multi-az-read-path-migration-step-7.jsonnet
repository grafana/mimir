// Migration step 7:
// - Decommission single-zone ruler.
(import 'test-multi-az-read-path-migration-step-6.jsonnet') {
  _config+:: {
    single_zone_ruler_enabled: false,
  },
}
