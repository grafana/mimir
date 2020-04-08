local mixin = (import 'mixin.libsonnet') {
  _config: {
    storage_backend: 'cassandra',
    storage_engine: ['chunks'],
    tags: 'cortex',
    gcs_enabled: false,
  },
};

{
  [name]: std.manifestJsonEx(mixin.grafanaDashboards[name], ' ')
  for name in std.objectFields(mixin.grafanaDashboards)
}
