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

    memcached_frontend_replicas: 2,
    memcached_index_queries_replicas: 4,
    memcached_chunks_replicas: 6,
    memcached_metadata_replicas: 8,

    memcached_mtls_server_name: 'memcached-cluster',
    memcached_mtls_ca_cert_secret: 'memcached-ca-cert',
    memcached_mtls_server_cert_secret: 'memcached-server-cert',
    memcached_mtls_server_key_secret: 'memcached-server-key',
    memcached_mtls_client_cert_secret: 'memcached-client-cert',
    memcached_mtls_client_key_secret: 'memcached-client-key',

    memcached_frontend_mtls_enabled: true,
    memcached_index_queries_mtls_enabled: true,
    memcached_chunks_mtls_enabled: true,
    memcached_metadata_mtls_enabled: true,
  },
}
