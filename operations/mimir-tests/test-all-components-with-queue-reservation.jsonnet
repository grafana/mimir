// Based on test-all-components.jsonnet.
(import 'test-all-components.jsonnet') {
  _config+:: {
    queue_reservation_enabled: true,
  },
}
