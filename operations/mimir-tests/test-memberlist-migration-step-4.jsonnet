local mimir = import 'mimir/mimir.libsonnet';

mimir {
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

    // Step 4: disable mirroring from primary (now memberlist) to secondary (now Consul) KV.
    memberlist_ring_enabled: true,
    multikv_migration_enabled: true,
    multikv_mirror_enabled: false,
    multikv_switch_primary_secondary: true,
  },
}
