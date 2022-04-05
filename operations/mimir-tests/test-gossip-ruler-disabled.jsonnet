local mimir = import 'mimir/mimir.libsonnet';

mimir {
  _config+:: {
    namespace: 'default',
    external_url: 'http://test',

    memberlist_ring_enabled: true,

    blocks_storage_backend: 'gcs',
    blocks_storage_bucket_name: 'blocks-bucket',
    bucket_index_enabled: true,
    query_scheduler_enabled: true,

    ruler_enabled: false,

    alertmanager_enabled: true,
    alertmanager_client_type: 'gcs',
    alertmanager_gcs_bucket_name: 'alerts-bucket',
  },
}
