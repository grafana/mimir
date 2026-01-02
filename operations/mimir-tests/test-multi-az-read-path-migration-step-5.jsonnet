// Migration step 5:
// - Deploy multi-zone ruler, alongside the single-zone one.
(import 'test-multi-az-read-path-migration-step-4.jsonnet') {
  _config+:: {
    single_zone_ruler_enabled: true,
    multi_zone_ruler_enabled: true,
  },
}
