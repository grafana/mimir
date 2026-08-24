{
  _images+:: {
    // Various third-party images.
    memcached: 'memcached:1.6.42-alpine@sha256:43a2e7f74aebfff0c9921f4d367299ced9eacaeaccdc8bb4bc122a4fba2cd909',
    memcachedExporter: 'prom/memcached-exporter:v0.16.0@sha256:fa03aba2f2aa6f572bf56ba07dd2960c62433805427be0fddc8b21b8074c1728',

    // Our services.
    mimir: 'grafana/mimir:3.2.0',

    alertmanager: self.mimir,
    distributor: self.mimir,
    ingester: self.mimir,
    querier: self.mimir,
    query_frontend: self.mimir,
    compactor: self.mimir,
    compactor_scheduler: self.mimir,
    block_builder: self.mimir,
    block_builder_scheduler: self.mimir,
    ruler: self.mimir,
    store_gateway: self.mimir,
    query_scheduler: self.mimir,
    overrides_exporter: self.mimir,

    query_tee: 'grafana/query-tee:3.2.0',
    continuous_test: self.mimir,
    mimirtool: 'grafana/mimirtool:3.2.0',
  },
}
