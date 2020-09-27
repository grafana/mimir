local utils = import 'mixin-utils/utils.libsonnet';

(import 'dashboard-utils.libsonnet') {
  'cortex-reads.json':
    $.dashboard('Cortex / Reads')
    .addClusterSelectorTemplates()
    .addRow(
      $.row('Gateway')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_request_duration_seconds_count{%s, route=~"(prometheus|api_prom)_api_v1_.+"}' % $.jobMatcher($._config.job_names.gateway))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector($._config.job_names.gateway) + [utils.selector.re('route', '(prometheus|api_prom)_api_v1_.+')])
      )
      .addPanel(
        $.panel('Per %s p99 Latency' % $._config.per_instance_label) +
        $.hiddenLegendQueryPanel(
          'histogram_quantile(0.99, sum by(le, %s) (rate(cortex_request_duration_seconds_bucket{%s, route=~"(prometheus|api_prom)_api_v1_.+"}[$__interval])))' % [$._config.per_instance_label, $.jobMatcherEquality($._config.job_names.gateway)], ''
        ) +
        { yaxes: $.yaxes('s') }
      )
    )
    .addRow(
      $.row('Query Frontend')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_request_duration_seconds_count{%s, route=~"(prometheus|api_prom)_api_v1_.+"}' % $.jobMatcher($._config.job_names.query_frontend))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector($._config.job_names.query_frontend) + [utils.selector.re('route', '(prometheus|api_prom)_api_v1_.+')])
      )
      .addPanel(
        $.panel('Per %s p99 Latency' % $._config.per_instance_label) +
        $.hiddenLegendQueryPanel(
          'histogram_quantile(0.99, sum by(le, %s) (rate(cortex_request_duration_seconds_bucket{%s, route=~"(prometheus|api_prom)_api_v1_.+"}[$__interval])))' % [$._config.per_instance_label, $.jobMatcherEquality($._config.job_names.query_frontend)], ''
        ) +
        { yaxes: $.yaxes('s') }
      )
    )
    .addRow(
      $.row('Cache - Query Results')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_cache_request_duration_seconds_count{method=~"frontend.+", %s}' % $.jobMatcher($._config.job_names.query_frontend))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_cache_request_duration_seconds', $.jobSelector($._config.job_names.query_frontend) + [utils.selector.re('method', 'frontend.+')])
      )
    )
    .addRow(
      $.row('Querier')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_request_duration_seconds_count{%s, route=~"(prometheus|api_prom)_api_v1_.+"}' % $.jobMatcher($._config.job_names.querier))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector($._config.job_names.querier) + [utils.selector.re('route', '(prometheus|api_prom)_api_v1_.+')])
      )
      .addPanel(
        $.panel('Per %s p99 Latency' % $._config.per_instance_label) +
        $.hiddenLegendQueryPanel(
          'histogram_quantile(0.99, sum by(le, %s) (rate(cortex_request_duration_seconds_bucket{%s, route=~"(prometheus|api_prom)_api_v1_.+"}[$__interval])))' % [$._config.per_instance_label, $.jobMatcherEquality($._config.job_names.querier)], ''
        ) +
        { yaxes: $.yaxes('s') }
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
      .addPanel(
        $.panel('Per %s p99 Latency' % $._config.per_instance_label) +
        $.hiddenLegendQueryPanel(
          'histogram_quantile(0.99, sum by(le, %s) (rate(cortex_request_duration_seconds_bucket{%s, route=~"/cortex.Ingester/Query(Stream)?|/cortex.Ingester/MetricsForLabelMatchers|/cortex.Ingester/LabelValues|/cortex.Ingester/MetricsMetadata"}[$__interval])))' % [$._config.per_instance_label, $.jobMatcherEquality($._config.job_names.ingester)], ''
        ) +
        { yaxes: $.yaxes('s') }
      )
    )
    .addRowIf(
      std.member($._config.storage_engine, 'blocks'),
      $.row('Store-gateway')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_request_duration_seconds_count{%s,route=~"/gatewaypb.StoreGateway/.*"}' % $.jobMatcher($._config.job_names.store_gateway))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector($._config.job_names.store_gateway) + [utils.selector.re('route', '/gatewaypb.StoreGateway/.*')])
      )
      .addPanel(
        $.panel('Per %s p99 Latency' % $._config.per_instance_label) +
        $.hiddenLegendQueryPanel(
          'histogram_quantile(0.99, sum by(le, %s) (rate(cortex_request_duration_seconds_bucket{%s, route=~"/gatewaypb.StoreGateway/.*"}[$__interval])))' % [$._config.per_instance_label, $.jobMatcherEquality($._config.job_names.store_gateway)], ''
        ) +
        { yaxes: $.yaxes('s') }
      )
    )
    .addRowIf(
      std.member($._config.storage_engine, 'chunks'),
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
      std.member($._config.storage_engine, 'chunks'),
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
      std.member($._config.storage_engine, 'blocks'),
      $.row('Memcached – Blocks Storage – Index header (Store-gateway)')
      .addPanel(
        $.panel('QPS') +
        $.queryPanel('sum by(operation) (rate(thanos_memcached_operations_total{component="store-gateway",name="index-cache", %s}[$__interval]))' % $.jobMatcher($._config.job_names.store_gateway), '{{operation}}') +
        $.stack +
        { yaxes: $.yaxes('ops') },
      )
      .addPanel(
        $.panel('Latency (getmulti)') +
        $.latencyPanel('thanos_memcached_operation_duration_seconds', '{%s,operation="getmulti",component="store-gateway",name="index-cache"}' % $.jobMatcher($._config.job_names.store_gateway))
      )
      .addPanel(
        $.panel('Hit ratio') +
        $.queryPanel('sum by(item_type) (rate(thanos_store_index_cache_hits_total{component="store-gateway",%s}[$__interval])) / sum by(item_type) (rate(thanos_store_index_cache_requests_total{component="store-gateway",%s}[$__interval]))' % [$.jobMatcher($._config.job_names.store_gateway), $.jobMatcher($._config.job_names.store_gateway)], '{{item_type}}') +
        { yaxes: $.yaxes('percentunit') },
      )
    )
    .addRowIf(
      std.member($._config.storage_engine, 'blocks'),
      $.thanosMemcachedCache('Memcached – Blocks Storage – Chunks (Store-gateway)', $._config.job_names.store_gateway, 'store-gateway', 'chunks-cache')
    )
    .addRowIf(
      std.member($._config.storage_engine, 'blocks'),
      $.thanosMemcachedCache('Memcached – Blocks Storage – Metadada (Store-gateway)', $._config.job_names.store_gateway, 'store-gateway', 'metadata-cache')
    )
    .addRowIf(
      std.member($._config.storage_engine, 'blocks'),
      $.thanosMemcachedCache('Memcached – Blocks Storage – Metadada (Querier)', $._config.job_names.querier, 'querier', 'metadata-cache')
    )
    .addRowIf(
      std.member($._config.storage_engine, 'chunks') &&
      std.member($._config.chunk_index_backend + $._config.chunk_store_backend, 'cassandra'),
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
      std.member($._config.storage_engine, 'chunks') &&
      std.member($._config.chunk_index_backend + $._config.chunk_store_backend, 'bigtable'),
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
      std.member($._config.storage_engine, 'chunks') &&
      std.member($._config.chunk_index_backend + $._config.chunk_store_backend, 'dynamodb'),
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
      std.member($._config.storage_engine, 'chunks') &&
      std.member($._config.chunk_store_backend, 'gcs'),
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
      std.member($._config.storage_engine, 'blocks'),
      $.objectStorePanels1('Store-gateway - Blocks Object Store', 'store-gateway'),
    )
    .addRowIf(
      std.member($._config.storage_engine, 'blocks'),
      $.objectStorePanels2('', 'store-gateway'),
    )
    // Object store metrics for the querier.
    .addRowIf(
      std.member($._config.storage_engine, 'blocks'),
      $.objectStorePanels1('Querier - Blocks Object Store', 'querier'),
    )
    .addRowIf(
      std.member($._config.storage_engine, 'blocks'),
      $.objectStorePanels2('', 'querier'),
    ),
}
