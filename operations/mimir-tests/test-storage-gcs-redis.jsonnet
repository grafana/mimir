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

    cache_frontend_enabled: true,
    cache_frontend_backend: 'redis',
    cache_frontend_name: 'redis-frontend',

    cache_index_queries_enabled: true,
    cache_index_queries_backend: 'redis',
    cache_index_queries_name: 'redis-index-queries',

    cache_chunks_enabled: true,
    cache_chunks_backend: 'redis',
    cache_chunks_name: 'redis-chunks',

    cache_metadata_enabled: true,
    cache_metadata_backend: 'redis',
    cache_metadata_name: 'redis-metadata',
  },
}
