local mimir = import 'mimir/mimir.libsonnet';
local gossip = import 'mimir/gossip.libsonnet';

mimir + gossip {
  _config+:: {
    namespace: 'default',
    external_url: 'http://test',

    blocks_storage_backend: 'gcs',
    blocks_storage_bucket_name: 'blocks-bucket',
    bucket_index_enabled: true,
    query_scheduler_enabled: true,

    ruler_enabled: true,
    ruler_client_type: 'gcs',
    ruler_storage_bucket_name: 'rules-bucket',

    alertmanager_enabled: true,
    alertmanager_client_type: 'gcs',
    alertmanager_gcs_bucket_name: 'alerts-bucket',
    alertmanager+: {
      sharding_enabled: true,
    },
  },
}
