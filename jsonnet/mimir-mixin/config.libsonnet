{
  grafanaDashboardFolder: 'Cortex',
  grafanaDashboardShards: 4,

  _config+:: {
    storage_backend: "cassandra", #error 'must specify storage backend (cassandra, gcp)',
    // may contain 'chunks', 'tsdb' or both. Enables chunks- or tsdb- specific panels and dashboards.
    storage_engine: ['chunks'],
    gcs_enabled: false,
    tags: ['cortex'],
  },
}
