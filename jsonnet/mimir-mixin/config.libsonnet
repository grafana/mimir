{
  grafanaDashboardFolder: 'Cortex',
  grafanaDashboardShards: 4,

  _config+:: {
    // Switch for overall storage engine.
    // May contain 'chunks', 'tsdb' or both.
    // Enables chunks- or tsdb- specific panels and dashboards.
    storage_engine: ['chunks', 'tsdb'],

    // For chunks backend, switch for chunk index type.
    // May contain 'bigtable', 'dynamodb' or 'cassandra'.
    chunk_index_backend: ['bigtable', 'dynamodb', 'cassandra'],

    // For chunks backend, switch for chunk store type.
    // May contain 'bigtable', 'dynamodb', 'cassandra', 's3' or 'gcs'.
    chunk_store_backend: ['bigtable', 'dynamodb', 'cassandra', 's3', 'gcs'],

    // Tags for dashboards.
    tags: ['cortex'],

    // If Cortex is deployed as a single binary, set to true to
    // modify the job selectors in the dashboard queries.
    singleBinary: false,

    job_names: {
      ingester: 'ingester',
      distributor: 'distributor',
      querier: 'querier',
      query_frontend: 'query-frontend',
      table_manager: 'table-manager',
      store_gateway: 'store-gateway',
      gateway: 'cortex-gw',
    },

    cortex_p99_latency_threshold_seconds: 2.5,
    alert_namespace_matcher: '',

    // Whether resources dashboards are enabled (based on cAdvisor metrics).
    resources_dashboards_enabled: false,
  },
}
