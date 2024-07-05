{
  _images+:: {
    // Can be set to a registry prefix to use for all images.
    prefix: '',

    // Various third-party images.
    memcached: self.prefix + 'memcached:1.6.28-alpine',
    memcachedExporter: self.prefix + 'prom/memcached-exporter:v0.14.4',

    // Our services.
    mimir: self.prefix + 'grafana/mimir:2.12.0',

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

    query_tee: self.prefix + 'grafana/query-tee:2.12.0',
    continuous_test: self.prefix + 'grafana/mimir-continuous-test:2.12.0',

    // Read-write deployment mode.
    mimir_write: self.mimir,
    mimir_read: self.mimir,
    mimir_backend: self.mimir,

    // See: https://github.com/grafana/rollout-operator
    rollout_operator: self.prefix + 'grafana/rollout-operator:v0.17.0',
  },
}
