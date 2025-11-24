// Based on test-multi-az-write-path.jsonnet.
(import 'test-multi-az-write-path.jsonnet') {
  _config+:: {
    single_zone_distributor_enabled: true,
  },
}
