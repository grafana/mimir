local mimir = import 'mimir/mimir.libsonnet';

mimir {
  _config+:: {
    namespace: 'default',
    external_url: 'http://test',
    aws_region: 'eu-west-1',

    storage_backend: 's3',
    blocks_storage_bucket_name: 'blocks-bucket',
    bucket_index_enabled: true,
    query_scheduler_enabled: true,

    ruler_enabled: true,
    ruler_storage_bucket_name: 'rules-bucket',

    alertmanager_enabled: true,
    alertmanager_storage_bucket_name: 'alerts-bucket',

    deployment_mode: 'read-write',
    multi_zone_ingester_enabled: true,
    multi_zone_store_gateway_enabled: true,

    autoscaling_mimir_read_enabled: true,
    autoscaling_mimir_read_min_replicas: 2,
    autoscaling_mimir_read_max_replicas: 20,
  },
}
