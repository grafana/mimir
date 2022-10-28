local mimir = import 'mimir/mimir.libsonnet';

mimir {
  _config+:: {
    namespace: 'default',
    external_url: 'http://test',
    aws_region: 'eu-west-1',

    storage_backend: 's3',
    blocks_storage_bucket_name: 'blocks-bucket',
    bucket_index_enabled: true,

    ruler_enabled: true,
    ruler_storage_bucket_name: 'rules-bucket',

    alertmanager_enabled: true,
    alertmanager_storage_bucket_name: 'alerts-bucket',

    query_scheduler_enabled: true,
    query_scheduler_service_discovery_mode: 'ring',
    query_scheduler_service_discovery_ring_read_path_enabled: false,
  },
}
