{
  _images+:: {
    // Various third-party images.
    memcached: 'memcached:1.5.17-alpine',
    memcachedExporter: 'prom/memcached-exporter:v0.6.0',

    // Our services.
    cortex: 'cortexproject/cortex:v1.1.0',

    distributor: self.cortex,
    ingester: self.cortex,
    querier: self.cortex,
    query_frontend: self.cortex,
    tableManager: self.cortex,
    compactor: self.cortex,
    flusher: self.cortex,
    ruler: self.cortex,
    store_gateway: self.cortex,

    query_tee: 'quay.io/cortexproject/query-tee:master-5d7b05c3',
    // TODO(gouthamve/jtlisi): Upstream the ruler and AM configs.
    alertmanager: 'jtlisi/cortex:20190819_alertmanager_update-faa66aa43',
    testExporter: 'cortexproject/test-exporter:master-be013707',
  },
}
