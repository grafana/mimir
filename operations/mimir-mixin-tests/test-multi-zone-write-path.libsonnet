(import 'mixin-compiled.libsonnet') + {
  _config+:: {
    show_multi_zone_write_path_panels: true,

    // Enable the gateway and the rejected requests series too, so that all the panels which can
    // show per-zone series are rendered.
    gateway_enabled: true,
    show_rejected_requests_on_writes_dashboard: true,
  },
}
