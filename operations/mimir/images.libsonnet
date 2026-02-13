{
  _images+:: {
    // Various third-party images.
    memcached: 'memcached:1.6.34-alpine',
    memcachedExporter: 'prom/memcached-exporter:v0.15.3',

    // Our services.
    mimir: 'grafana/mimir:3.0.3',

    alertmanager: self.mimir,
    distributor: self.mimir,
    ingester: self.mimir,
    querier: self.mimir,
    query_frontend: self.mimir,
    compactor: self.mimir,
    ruler: self.mimir,
    store_gateway: self.mimir,
    query_scheduler: self.mimir,
    overrides_exporter: self.mimir,

    query_tee: 'grafana/query-tee:3.0.3',
    continuous_test: self.mimir,
  },
}
