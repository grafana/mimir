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

    // Step 5: disable migration (ie. use of multi KV), but keep runtime config around for components that haven't restarted yet.
    // Note: this also removes Consul. That's fine, because it's not used anymore (mirroring to it was disabled in step 4).
    multikv_migration_enabled: false,
    multikv_mirror_enabled: false,
    multikv_switch_primary_secondary: true,
    multikv_migration_teardown: true,
  },
}
