local mimir = import 'mimir/mimir.libsonnet';

mimir {
  _config+:: {
    namespace: 'default',
    external_url: 'http://test',

    storage_backend: 'gcs',
    blocks_storage_bucket_name: 'blocks-bucket',

    multi_zone_ingester_enabled: true,
    multi_zone_ingester_replicas: 3,

    multi_zone_store_gateway_enabled: true,
    multi_zone_store_gateway_replicas: 3,

    enable_pvc_auto_deletion_for_compactors: true,
    enable_pvc_auto_deletion_for_store_gateways: true,
    enable_pvc_auto_deletion_for_ingesters: true,
  },
}
