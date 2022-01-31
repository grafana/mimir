{
  _images+:: {
    // Various third-party images.
    memcached: 'memcached:1.6.9-alpine',
    memcachedExporter: 'prom/memcached-exporter:v0.6.0',

    // Our services.
    mimir: 'cortexproject/cortex:v1.9.0',

    alertmanager: self.mimir,
    distributor: self.mimir,
    ingester: self.mimir,
    querier: self.mimir,
    query_frontend: self.mimir,
    compactor: self.mimir,
    flusher: self.mimir,
    ruler: self.mimir,
    store_gateway: self.mimir,
    query_scheduler: self.mimir,
    overrides_exporter: self.mimir,

    query_tee: 'quay.io/cortexproject/query-tee:v1.9.0',
    testExporter: 'cortexproject/test-exporter:v1.9.0',
  },
}
