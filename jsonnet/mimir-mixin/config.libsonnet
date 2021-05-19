{
  grafanaDashboardFolder: 'Cortex',
  grafanaDashboardShards: 4,

  _config+:: {
    // Switch for overall storage engine.
    // May contain 'chunks', 'blocks' or both.
    // Enables chunks- or blocks- specific panels and dashboards.
    storage_engine: ['blocks'],

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

    // These are used by the dashboards and allow for the simultaneous display of
    // microservice and single binary cortex clusters.
    job_names: {
      ingester: '(ingester.*|cortex$)',  // Match also ingester-blocks, which is used during the migration from chunks to blocks.
      distributor: '(distributor|cortex$)',
      querier: '(querier|cortex$)',
      ruler: '(ruler|cortex$)',
      query_frontend: '(query-frontend|cortex$)',
      query_scheduler: 'query-scheduler',  // Not part of single-binary.
      table_manager: '(table-manager|cortex$)',
      store_gateway: '(store-gateway|cortex$)',
      gateway: '(gateway|cortex-gw)',
      compactor: 'compactor.*',  // Match also custom compactor deployments.
    },

    // Labels used to in alert aggregations - should uniquely identify
    // a single Cortex cluster.
    alert_aggregation_labels: 'cluster, namespace',
    cortex_p99_latency_threshold_seconds: 2.5,

    // Whether resources dashboards are enabled (based on cAdvisor metrics).
    resources_dashboards_enabled: false,

    // The label used to differentiate between different application instances (i.e. 'pod' in a kubernetes install).
    per_instance_label: 'pod',

    // The label used to differentiate between different nodes (i.e. servers).
    per_node_label: 'instance',
  },
}
