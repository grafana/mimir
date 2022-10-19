local mimir = import 'mimir/mimir.libsonnet';

mimir {
  _config+:: {
    namespace: 'default',
    external_url: 'http://test',

    memberlist_ring_enabled: true,

    storage_backend: 'gcs',

    blocks_storage_bucket_name: 'blocks-bucket',
    bucket_index_enabled: true,
    query_scheduler_enabled: true,

    ruler_enabled: true,
    ruler_storage_bucket_name: 'rules-bucket',

    alertmanager_enabled: true,
    alertmanager_storage_bucket_name: 'alerts-bucket',

    // Add another config map to all Mimir components, and add runtime config
    configmaps+: {
      'new-config-map': '/etc/another-config',
    },
    runtime_config_files+: ['/etc/another-config/runtimeconfig.yaml'],
  },

  local configMap = $.core.v1.configMap,

  // New config map, referenced above.
  new_config_map:
    configMap.new('new-config-map') +
    configMap.withData({ a: 'b' }),
}
