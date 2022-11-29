local mimir = import 'mimir/mimir.libsonnet';

mimir {
  _config+:: {
    namespace: 'default',
    external_url: 'http://test',

    // Use consul for hash rings.
    memberlist_ring_enabled: false,

    storage_backend: 'gcs',
    blocks_storage_bucket_name: 'blocks-bucket',

    ruler_enabled: false,

    alertmanager_enabled: true,
    alertmanager_client_type: 'gcs',
    alertmanager_storage_bucket_name: 'alerts-bucket',
  },
}
