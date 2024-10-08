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

    multi_zone_ingester_enabled: true,
    multi_zone_ingester_replicas: 3,

    multi_zone_store_gateway_enabled: true,
    multi_zone_store_gateway_replicas: 3,

    local availabilityZones = ['us-east-2a', 'us-east-2b'],
    multi_zone_distributor_enabled: true,
    multi_zone_distributor_availability_zones: availabilityZones,

    autoscaling_distributor_enabled: true,
    autoscaling_distributor_min_replicas: 3,
    autoscaling_distributor_max_replicas: 30,
  },

  ingester_env_map+:: {
    A: 'all-ingesters',
  },

  ingester_zone_a_env_map+:: {
    Z: '123',
    A: 'ingester-a-only',
    GOGC: 'off',
    GOMEMLIMIT: '1Gi',
  },

  store_gateway_env_map+:: {
    A: 'all-store-gateways',
  },

  store_gateway_zone_b_env_map+:: {
    A: 'zone-b',
    GOGC: '1000',
  },
}
