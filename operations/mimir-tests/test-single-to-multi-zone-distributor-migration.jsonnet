// Based on test-multi-zone-distributor.jsonnet.
(import 'test-multi-zone-distributor.jsonnet') {
  _config+:: {
    single_zone_distributor_enabled: true,
  },
}
