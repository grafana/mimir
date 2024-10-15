local mimir = import 'mimir/mimir.libsonnet';

mimir {
  _config+:: {
    namespace: 'default',
    external_url: 'http://test',

    storage_backend: 'gcs',
    blocks_storage_bucket_name: 'blocks-bucket',

    ruler_enabled: true,
    ruler_storage_bucket_name: 'rules-bucket',

    alertmanager_enabled: true,
    alertmanager_storage_bucket_name: 'alerts-bucket',

    shuffle_sharding+:: {
      ingester_write_path_enabled: true,
      ingester_read_path_enabled: true,
      querier_enabled: true,
      ruler_enabled: true,
      store_gateway_enabled: true,
      ingest_storage_partitions_enabled: true,
    },
  },
}
