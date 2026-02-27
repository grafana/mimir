// Migration step 3:
// - Migrate ingesters to multi-zone deployment.
(import 'test-multi-az-read-path-migration-step-2f.jsonnet') {
  _config+:: {
    multi_zone_ingester_multi_az_enabled: true,
  },
}
