// Based on test-multi-zone.jsonnet.
(import 'test-multi-zone.jsonnet') {
  _config+:: {
    multi_zone_etcd_enabled: true,
  },
}
