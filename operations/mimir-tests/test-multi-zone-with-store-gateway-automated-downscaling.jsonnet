// Based on test-multi-zone.jsonnet.
(import 'test-multi-zone.jsonnet') {
  _config+:: {
    store_gateway_automated_downscale_enabled: true,
    store_gateway_automated_downscale_min_time_between_zones: '20m',
  },
}
