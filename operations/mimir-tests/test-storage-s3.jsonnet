local mimir = import 'mimir/mimir.libsonnet';

mimir {
  _config+:: {
    namespace: 'default',
    external_url: 'http://test',
    aws_region: 'eu-west-1',

    blocks_storage_backend: 's3',
    blocks_storage_bucket_name: 'blocks-bucket',
    cortex_bucket_index_enabled: true,
    query_scheduler_enabled: true,

    ruler_enabled: true,
    ruler_client_type: 's3',
    ruler_storage_bucket_name: 'rules-bucket',

    alertmanager_enabled: true,
    alertmanager_client_type: 's3',
    alertmanager_s3_bucket_name: 'alerts-bucket',
    alertmanager+: {
      sharding_enabled: true,
    },
  },
}
