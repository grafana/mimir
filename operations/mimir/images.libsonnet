{
  _images+:: {
    // Various third-party images.
    memcached: 'memcached:1.5.17-alpine',
    memcachedExporter: 'prom/memcached-exporter:v0.6.0',
    postgresql: 'postgres:9.6.11-alpine',

    // Our services.
    cortex: 'cortexproject/cortex:master-37c1f178',

    distributor: self.cortex,
    ingester: self.cortex,
    querier: self.cortex,
    query_frontend: self.cortex,
    tableManager: self.cortex,
    // TODO(gouthamve/jtlisi): Upstream the ruler and AM configs.
    ruler: 'jtlisi/cortex:20190806_prommanager_ruler_with_api-50343f8d',
    alertmanager: 'jtlisi/cortex:20190819_alertmanager_update-165b393a',
    testExporter: 'cortexproject/test-exporter:master-ef99cdaf',
  },
}
