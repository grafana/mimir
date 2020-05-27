local utils = import 'mixin-utils/utils.libsonnet';

(import 'dashboard-utils.libsonnet') {
  'cortex-reads.json':
    $.dashboard('Cortex / Reads')
    .addClusterSelectorTemplates()
    .addRow(
      $.row('Gateway')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_request_duration_seconds_count{%s, route=~"api_prom_api_v1_.+"}' % $.jobMatcher($._config.job_names.gateway))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector($._config.job_names.gateway) + [utils.selector.re('route', 'api_prom_api_v1_.+')])
      )
    )
    .addRow(
      $.row('Query Frontend')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_request_duration_seconds_count{%s, route=~"api_prom_api_v1_.+"}' % $.jobMatcher($._config.job_names.query_frontend))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector($._config.job_names.query_frontend) + [utils.selector.re('route', 'api_prom_api_v1_.+')])
      )
    )
    .addRow(
      $.row('Cache - Query Results')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_cache_request_duration_seconds_count{%s}' % $.jobMatcher($._config.job_names.query_frontend))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_cache_request_duration_seconds', $.jobSelector($._config.job_names.query_frontend))
      )
    )
    .addRow(
      $.row('Querier')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_request_duration_seconds_count{%s, route=~"api_prom_api_v1_.+"}' % $.jobMatcher($._config.job_names.querier))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector($._config.job_names.querier) + [utils.selector.re('route', 'api_prom_api_v1_.+')])
      )
    )
    .addRow(
      $.row('Ingester')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_request_duration_seconds_count{%s,route=~"/cortex.Ingester/Query(Stream)?|/cortex.Ingester/MetricsForLabelMatchers|/cortex.Ingester/LabelValues|/cortex.Ingester/MetricsMetadata"}' % $.jobMatcher($._config.job_names.ingester))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector($._config.job_names.ingester) + [utils.selector.re('route', '/cortex.Ingester/Query(Stream)?|/cortex.Ingester/MetricsForLabelMatchers|/cortex.Ingester/LabelValues|/cortex.Ingester/MetricsMetadata')])
      )
    )
    .addRowIf(
      std.setMember('tsdb', $._config.storage_engine),
      $.row('Store-gateway')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_request_duration_seconds_count{%s,route=~"/gatewaypb.StoreGateway/.*"}' % $.jobMatcher($._config.job_names.store_gateway))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector($._config.job_names.store_gateway) + [utils.selector.re('route', '/gatewaypb.StoreGateway/.*')])
      )
    )
    .addRowIf(
      std.setMember('chunks', $._config.storage_engine),
      $.row('Memcached - Chunks storage - Index')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_cache_request_duration_seconds_count{%s,method="store.index-cache-read.memcache.fetch"}' % $.jobMatcher($._config.job_names.querier))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_cache_request_duration_seconds', $.jobSelector($._config.job_names.querier) + [utils.selector.eq('method', 'store.index-cache-read.memcache.fetch')])
      )
    )
    .addRowIf(
      std.setMember('chunks', $._config.storage_engine),
      $.row('Memcached - Chunks storage - Chunks')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_cache_request_duration_seconds_count{%s,method="chunksmemcache.fetch"}' % $.jobMatcher($._config.job_names.querier))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_cache_request_duration_seconds', $.jobSelector($._config.job_names.querier) + [utils.selector.eq('method', 'chunksmemcache.fetch')])
      )
    )
    .addRowIf(
      std.setMember('tsdb', $._config.storage_engine),
      $.row('Memcached - Blocks Storage - Index header')
      .addPanel(
        $.panel('QPS') +
        $.queryPanel('sum by(operation) (rate(cortex_storegateway_blocks_index_cache_memcached_operation_duration_seconds_count{%s}[$__interval]))' % $.jobMatcher($._config.job_names.store_gateway), '{{operation}}') +
        $.stack +
        { yaxes: $.yaxes('ops') },
      )
      .addPanel(
        $.panel('Latency (getmulti)') +
        $.latencyPanel('cortex_storegateway_blocks_index_cache_memcached_operation_duration_seconds', '{%s,operation="getmulti"}' % $.jobMatcher($._config.job_names.store_gateway))
      )
      .addPanel(
        $.panel('Hit ratio') +
        $.queryPanel('sum by(item_type) (rate(cortex_storegateway_blocks_index_cache_hits_total{%s}[$__interval])) / sum by(item_type) (rate(cortex_storegateway_blocks_index_cache_requests_total{%s}[$__interval]))' % [$.jobMatcher($._config.job_names.store_gateway), $.jobMatcher($._config.job_names.store_gateway)], '{{item_type}}') +
        { yaxes: $.yaxes('percentunit') },
      )
    )
    .addRowIf(
      std.setMember('tsdb', $._config.storage_engine),
      $.row('Memcached - Blocks Storage - Chunks')
      .addPanel(
        $.panel('QPS') +
        $.queryPanel('sum by(operation) (rate(cortex_storegateway_thanos_memcached_operations_total{%s,name="chunks-cache"}[$__interval]))' % $.jobMatcher($._config.job_names.store_gateway), '{{operation}}') +
        $.stack +
        { yaxes: $.yaxes('ops') },
      )
      .addPanel(
        $.panel('Latency (getmulti)') +
        $.latencyPanel('cortex_storegateway_thanos_memcached_operation_duration_seconds', '{%s,operation="getmulti",name="chunks-cache"}' % $.jobMatcher($._config.job_names.store_gateway))
      )
      .addPanel(
        $.panel('Hit ratio') +
        $.queryPanel('sum(rate(cortex_storegateway_thanos_cache_memcached_hits_total{%s,name="chunks-cache"}[$__interval])) / sum(rate(cortex_storegateway_thanos_cache_memcached_requests_total{%s,name="chunks-cache"}[$__interval]))' % [$.jobMatcher($._config.job_names.store_gateway), $.jobMatcher($._config.job_names.store_gateway)], 'chunks') +
        { yaxes: $.yaxes('percentunit') },
      )
    )
    .addRowIf(
      std.setMember('chunks', $._config.storage_engine) &&
      std.setMember('cassandra', $._config.chunk_index_backend + $._config.chunk_store_backend),
      $.row('Cassandra')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_cassandra_request_duration_seconds_count{%s, operation="SELECT"}' % $.jobMatcher($._config.job_names.querier))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_cassandra_request_duration_seconds', $.jobSelector($._config.job_names.querier) + [utils.selector.eq('operation', 'SELECT')])
      )
    )
    .addRowIf(
      std.setMember('chunks', $._config.storage_engine) &&
      std.setMember('bigtable', $._config.chunk_index_backend + $._config.chunk_store_backend),
      $.row('BigTable')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_bigtable_request_duration_seconds_count{%s, operation="/google.bigtable.v2.Bigtable/ReadRows"}' % $.jobMatcher($._config.job_names.querier))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_bigtable_request_duration_seconds', $.jobSelector($._config.job_names.querier) + [utils.selector.eq('operation', '/google.bigtable.v2.Bigtable/ReadRows')])
      ),
    )
    .addRowIf(
      std.setMember('chunks', $._config.storage_engine) &&
      std.setMember('dynamodb', $._config.chunk_index_backend + $._config.chunk_store_backend),
      $.row('DynamoDB')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_dynamo_request_duration_seconds_count{%s, operation="DynamoDB.QueryPages"}' % $.jobMatcher($._config.job_names.querier))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_dynamo_request_duration_seconds', $.jobSelector($._config.job_names.querier) + [utils.selector.eq('operation', 'DynamoDB.QueryPages')])
      ),
    )
    .addRowIf(
      std.setMember('chunks', $._config.storage_engine) &&
      std.setMember('gcs', $._config.chunk_store_backend),
      $.row('GCS')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_gcs_request_duration_seconds_count{%s, operation="GET"}' % $.jobMatcher($._config.job_names.querier))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_gcs_request_duration_seconds', $.jobSelector($._config.job_names.querier) + [utils.selector.eq('operation', 'GET')])
      )
    )
    // Object store metrics for the store-gateway.
    .addRowIf(
      std.setMember('tsdb', $._config.storage_engine),
      $.objectStorePanels1('Store-gateway - Blocks Object Store', 'store-gateway'),
    )
    .addRowIf(
      std.setMember('tsdb', $._config.storage_engine),
      $.objectStorePanels2('', 'store-gateway'),
    )
    // Object store metrics for the querier.
    .addRowIf(
      std.setMember('tsdb', $._config.storage_engine),
      $.objectStorePanels1('Querier - Blocks Object Store', 'querier'),
    )
    .addRowIf(
      std.setMember('tsdb', $._config.storage_engine),
      $.objectStorePanels2('', 'querier'),
    ),
}
