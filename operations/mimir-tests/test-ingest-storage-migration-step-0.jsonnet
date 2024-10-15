// This is the initial state of Mimir namespace, before getting migrated to ingest storage.
(import 'test-multi-zone.jsonnet') {
  _config+:: {
    cluster: 'test-cluster',

    ruler_enabled: true,
    ruler_storage_bucket_name: 'rules-bucket',

    alertmanager_enabled: true,
    alertmanager_storage_bucket_name: 'alerts-bucket',

    // Configure features required by ingest storage migration.
    ruler_remote_evaluation_enabled: true,
    ingester_automated_downscale_enabled: true,

    shuffle_sharding+:: {
      ingester_write_path_enabled: true,
      ingester_read_path_enabled: true,
      querier_enabled: true,
      ruler_enabled: true,
      store_gateway_enabled: true,
    },
  },
}
