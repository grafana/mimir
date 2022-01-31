local mimir = import 'mimir/mimir.libsonnet';
local overrides_exporter = import 'mimir/overrides-exporter.libsonnet';

overrides_exporter + mimir + {
  _config+:: {
    namespace: 'default',
    external_url: 'http://test',

    blocks_storage_backend: 'gcs',
    blocks_storage_bucket_name: 'blocks-bucket',
  },
}
