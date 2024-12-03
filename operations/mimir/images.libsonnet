{
  _images+:: {
    // Various third-party images.
    memcached: 'memcached:1.6.22-alpine',
    memcachedExporter: 'prom/memcached-exporter:v0.14.3',

    // Our services.
    mimir: 'grafana/mimir:2.13.1',

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

    query_tee: 'grafana/query-tee:2.13.1',
    continuous_test: 'grafana/mimir-continuous-test:2.13.1',

    // Read-write deployment mode.
    mimir_write: self.mimir,
    mimir_read: self.mimir,
    mimir_backend: self.mimir,

    // See: https://github.com/grafana/rollout-operator
    rollout_operator: 'grafana/rollout-operator:v0.17.0',
  },
}
