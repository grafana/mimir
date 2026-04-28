// Based on test-all-components.jsonnet.
(import 'test-all-components.jsonnet') {
  _config+:: {
    ingester_deletion_protection_enabled: true,
    store_gateway_deletion_protection_enabled: true,

    ingester_priority_class: 'high-nonpreempting',
  },
}
