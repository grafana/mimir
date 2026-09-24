local mimir = import 'mimir/mimir.libsonnet';

mimir {
  _config+:: {
    namespace: 'default',
    external_url: 'http://test',

    storage_backend: 'gcs',
    blocks_storage_bucket_name: 'blocks-bucket',
    // Ensure option is propagated to -blocks-storage.<backend>.http.force-attempt-http2
    // only on the store-gateway manifest, not other components which take blocks_storage config.
    store_gateway_force_attempt_http2: true,

    ruler_enabled: true,
    ruler_storage_bucket_name: 'rules-bucket',

    alertmanager_enabled: true,
    alertmanager_storage_bucket_name: 'alerts-bucket',
  },
}
