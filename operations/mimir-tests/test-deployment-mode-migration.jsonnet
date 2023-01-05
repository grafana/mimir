local mimir = import 'mimir/mimir.libsonnet';

mimir {
  _config+:: {
    namespace: 'default',
    external_url: 'http://test',

    deployment_mode: 'migration',

    storage_backend: 'gcs',
    blocks_storage_bucket_name: 'blocks-bucket',

    ruler_enabled: true,
    ruler_storage_bucket_name: 'rules-bucket',
    ruler_remote_evaluation_enabled: false,

    alertmanager_enabled: true,
    alertmanager_storage_bucket_name: 'alerts-bucket',

    multi_zone_ingester_enabled: true,
    multi_zone_ingester_replicas: 3,

    multi_zone_store_gateway_enabled: true,
    multi_zone_store_gateway_replicas: 3,
  },
}
