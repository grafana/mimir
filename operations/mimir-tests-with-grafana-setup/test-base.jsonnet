local config_api = import 'config-api.libsonnet';
local limits_operator = import 'limits-operator.libsonnet';
local mimir = import 'mimir/mimir.libsonnet';
local autoscaling = import 'new-ingester-autoscaling.libsonnet';

mimir + autoscaling + limits_operator {
  _config+:: {
    namespace: 'default',
    external_url: 'http://test',

    storage_backend: 'gcs',
    blocks_storage_bucket_name: 'blocks-bucket',

    ruler_enabled: true,
    ruler_storage_bucket_name: 'rules-bucket',

    alertmanager_enabled: true,
    alertmanager_storage_bucket_name: 'alerts-bucket',

    multi_zone_ingester_enabled: true,
    multi_zone_ingester_replicas: 30,

    multi_zone_store_gateway_enabled: true,
    multi_zone_store_gateway_replicas: 3,
  },
}
