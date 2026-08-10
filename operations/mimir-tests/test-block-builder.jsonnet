local mimir = import 'mimir/mimir.libsonnet';

mimir {
  _config+:: {
    namespace: 'default',
    external_url: 'http://test',

    storage_backend: 'gcs',
    blocks_storage_bucket_name: 'blocks-bucket',

    // Ingest storage is required by block-builder.
    ruler_enabled: true,
    ruler_storage_bucket_name: 'rules-bucket',
    ruler_remote_evaluation_enabled: true,
    ingest_storage_enabled: true,

    block_builder+: {
      enabled: true,
    },
  },
}
