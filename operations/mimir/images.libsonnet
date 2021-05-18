{
  _images+:: {
    // Various third-party images.
    memcached: 'memcached:1.5.17-alpine',
    memcachedExporter: 'prom/memcached-exporter:v0.6.0',

    // Our services.
    cortex: 'cortexproject/cortex:v1.9.0',

    alertmanager: self.cortex,
    distributor: self.cortex,
    ingester: self.cortex,
    querier: self.cortex,
    query_frontend: self.cortex,
    tableManager: self.cortex,
    compactor: self.cortex,
    flusher: self.cortex,
    ruler: self.cortex,
    store_gateway: self.cortex,
    query_scheduler: self.cortex,

    cortex_tools: 'grafana/cortex-tools:v0.4.0',
    query_tee: 'quay.io/cortexproject/query-tee:v1.9.0',
    testExporter: 'cortexproject/test-exporter:v1.9.0',
  },
}
