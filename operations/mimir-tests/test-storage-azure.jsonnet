local mimir = import 'mimir/mimir.libsonnet';

mimir {
  _config+:: {
    namespace: 'default',
    external_url: 'http://test',

    storage_backend: 'azure',
    storage_azure_account_name: 'azure-account-name',
    storage_azure_account_key: 'azure-account-key',

    blocks_storage_bucket_name: 'blocks-bucket',

    ruler_enabled: true,
    ruler_storage_bucket_name: 'rules-bucket',

    alertmanager_enabled: true,
    alertmanager_storage_bucket_name: 'alerts-bucket',
  },
}
