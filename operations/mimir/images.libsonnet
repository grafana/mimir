{
  _images+:: {
    // Various third-party images.
    memcached: 'memcached:1.6.16-alpine',
    memcachedExporter: 'prom/memcached-exporter:v0.6.0',

    // Our services.
    mimir: 'grafana/mimir:2.3.1',

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

    query_tee: 'grafana/query-tee:2.3.1',
    continuous_test: 'grafana/mimir-continuous-test:main-8a8648e81',

    // See: https://github.com/grafana/rollout-operator
    rollout_operator: 'grafana/rollout-operator:v0.1.1',
  },
}
