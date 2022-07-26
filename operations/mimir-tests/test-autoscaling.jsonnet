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
    ruler_remote_evaluation_enabled: true,
    ruler_client_type: 'gcs',
    ruler_storage_bucket_name: 'rules-bucket',

    alertmanager_enabled: true,
    alertmanager_client_type: 'gcs',
    alertmanager_gcs_bucket_name: 'alerts-bucket',

    autoscaling_querier_enabled: true,
    autoscaling_querier_min_replicas: 3,
    autoscaling_querier_max_replicas: 30,

    autoscaling_ruler_querier_enabled: true,
    autoscaling_ruler_querier_min_replicas: 3,
    autoscaling_ruler_querier_max_replicas: 30,
  },
}
