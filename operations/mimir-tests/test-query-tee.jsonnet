local mimir = import 'mimir/mimir.libsonnet';
local query_tee = import 'mimir/query-tee.libsonnet';

query_tee + mimir + {
  _config+:: {
    namespace: 'default',
    external_url: 'http://test',

    blocks_storage_backend: 'gcs',
    blocks_storage_bucket_name: 'blocks-bucket',

    query_tee_enabled: true,
    query_tee_node_port: 1234,
  },
}
