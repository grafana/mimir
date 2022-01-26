local mimir = import 'mimir/mimir.libsonnet';

mimir {
  _config+:: {
    namespace: 'default',
    external_url: 'http://test',

    blocks_storage_backend: 'azure',
    blocks_storage_bucket_name: 'blocks-bucket',
    blocks_storage_azure_account_name: 'blocks-account-name',
    blocks_storage_azure_account_key: 'blocks-account-key',
    bucket_index_enabled: true,
    query_scheduler_enabled: true,

    ruler_enabled: true,
    ruler_client_type: 'azure',
    ruler_storage_bucket_name: 'rules-bucket',
    ruler_storage_azure_account_name: 'rules-account-name',
    ruler_storage_azure_account_key: 'rules-account-key',

    alertmanager_enabled: true,
    alertmanager_client_type: 'azure',
    alertmanager_azure_account_name: 'alerts-account-name',
    alertmanager_azure_account_key: 'alerts-account-key',
    alertmanager_azure_container_name: 'alerts-bucket',
    alertmanager+: {
      sharding_enabled: true,
    },
  },
}
