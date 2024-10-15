local mimir = import 'mimir/mimir.libsonnet';

mimir {
  _config+:: {
    namespace: 'default',
    external_url: 'http://test',

    storage_backend: 'gcs',
    blocks_storage_bucket_name: 'blocks-bucket',

    cortex_compactor_concurrent_rollout_enabled: true,
    cortex_compactor_max_unavailable: '50%',
  },

  compactor_statefulset+:
    $.apps.v1.statefulSet.mixin.spec.withReplicas(15),
}
