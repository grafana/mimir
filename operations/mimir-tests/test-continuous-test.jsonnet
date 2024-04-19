local mimir = import 'mimir/mimir.libsonnet';

mimir {
  _config+:: {
    namespace: 'default',
    external_url: 'http://test',

    storage_backend: 'gcs',
    blocks_storage_bucket_name: 'blocks-bucket',

    continuous_test_enabled: true,
    continuous_test_tenant_id: '1234',
    continuous_test_write_endpoint: 'http://distributor.%s.svc.cluster.local' % [$._config.namespace],
    continuous_test_read_endpoint: 'http://query-frontend.%s.svc.cluster.local' % [$._config.namespace],
  },
}
