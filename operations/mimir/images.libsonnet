{
  _images+:: {
    // Various third-party images.
    memcached: 'memcached:1.6.28-alpine',
    memcachedExporter: 'prom/memcached-exporter:v0.15.0',

    // Our services.
    mimir: 'grafana/mimir:2.14.1',

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

    query_tee: 'grafana/query-tee:2.14.1',
    continuous_test: 'grafana/mimir-continuous-test:2.14.1',

    // Read-write deployment mode.
    mimir_write: self.mimir,
    mimir_read: self.mimir,
    mimir_backend: self.mimir,

    // See: https://github.com/grafana/rollout-operator
    rollout_operator: 'grafana/rollout-operator:v0.20.0',
  },
}
