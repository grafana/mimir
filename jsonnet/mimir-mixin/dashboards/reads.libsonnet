local utils = import 'mixin-utils/utils.libsonnet';

(import 'dashboard-utils.libsonnet') {
  'cortex-reads.json':
    $.dashboard('Cortex / Reads')
    .addClusterSelectorTemplates()
    .addRow(
      $.row('Gateway')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_request_duration_seconds_count{%s, route=~"api_prom_api_v1_.+"}' % $.jobMatcher('cortex-gw'))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector('cortex-gw') + [utils.selector.re('route', 'api_prom_api_v1_.+')])
      )
    )
    .addRow(
      $.row('Query Frontend')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_request_duration_seconds_count{%s, route=~"api_prom_api_v1_.+"}' % $.jobMatcher('query-frontend'))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector('query-frontend') + [utils.selector.re('route', 'api_prom_api_v1_.+')])
      )
    )
    .addRow(
      $.row('Cache - Query Results')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_cache_request_duration_seconds_count{%s}' % $.jobMatcher('query-frontend'))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_cache_request_duration_seconds', $.jobSelector('query-frontend'))
      )
    )
    .addRow(
      $.row('Querier')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_request_duration_seconds_count{%s, route=~"api_prom_api_v1_.+"}' % $.jobMatcher('querier'))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector('querier') + [utils.selector.re('route', 'api_prom_api_v1_.+')])
      )
    )
    .addRow(
      $.row('Ingester')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_request_duration_seconds_count{%s,route=~"/cortex.Ingester/Query(Stream)?|/cortex.Ingester/MetricsForLabelMatchers"}' % $.jobMatcher('querier'))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector('querier') + [utils.selector.re('route', '/cortex.Ingester/Query(Stream)?|/cortex.Ingester/MetricsForLabelMatchers')])
      )
    )
    .addRowIf(
      std.setMember('chunks', $._config.storage_engine),
      $.row('Memcached - Index')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_cache_request_duration_seconds_count{%s,method="store.index-cache-read.memcache.fetch"}' % $.jobMatcher('querier'))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_cache_request_duration_seconds', $.jobSelector('querier') + [utils.selector.eq('method', 'store.index-cache-read.memcache.fetch')])
      )
    )
    .addRowIf(
      std.setMember('chunks', $._config.storage_engine),
      $.row('Memcached - Chunks')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_cache_request_duration_seconds_count{%s,method="chunksmemcache.fetch"}' % $.jobMatcher('querier'))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_cache_request_duration_seconds', $.jobSelector('querier') + [utils.selector.eq('method', 'chunksmemcache.fetch')])
      )
    )
    .addRowIf(
      std.setMember('tsdb', $._config.storage_engine),
      $.row('Memcached - Blocks Index')
      .addPanel(
        $.panel('QPS') +
        $.queryPanel('sum by(operation) (rate(cortex_storegateway_blocks_index_cache_memcached_operation_duration_seconds_count{%s}[$__interval]))' % $.jobMatcher('store-gateway'), '{{operation}}') +
        $.stack,
      )
      .addPanel(
        $.panel('Latency (getmulti)') +
        $.latencyPanel('cortex_storegateway_blocks_index_cache_memcached_operation_duration_seconds', '{%s,operation="getmulti"}' % $.jobMatcher('store-gateway'))
      )
    )
    .addRowIf(
      std.setMember('chunks', $._config.storage_engine) &&
      std.setMember('cassandra', $._config.chunk_index_backend + $._config.chunk_store_backend),
      $.row('Cassandra')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_cassandra_request_duration_seconds_count{%s, operation="SELECT"}' % $.jobMatcher('querier'))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_cassandra_request_duration_seconds', $.jobSelector('querier') + [utils.selector.eq('operation', 'SELECT')])
      )
    )
    .addRowIf(
      std.setMember('chunks', $._config.storage_engine) &&
      std.setMember('bigtable', $._config.chunk_index_backend + $._config.chunk_store_backend),
      $.row('BigTable')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_bigtable_request_duration_seconds_count{%s, operation="/google.bigtable.v2.Bigtable/ReadRows"}' % $.jobMatcher('querier'))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_bigtable_request_duration_seconds', $.jobSelector('querier') + [utils.selector.eq('operation', '/google.bigtable.v2.Bigtable/ReadRows')])
      ),
    )
    .addRowIf(
      std.setMember('chunks', $._config.storage_engine) &&
      std.setMember('dynamodb', $._config.chunk_index_backend + $._config.chunk_store_backend),
      $.row('DynamoDB')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_dynamo_request_duration_seconds_count{%s, operation="DynamoDB.QueryPages"}' % $.jobMatcher('querier'))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_dynamo_request_duration_seconds', $.jobSelector('querier') + [utils.selector.eq('operation', 'DynamoDB.QueryPages')])
      ),
    )
    .addRowIf(
      std.setMember('chunks', $._config.storage_engine) &&
      std.setMember('gcs', $._config.chunk_store_backend),
      $.row('GCS')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_gcs_request_duration_seconds_count{%s, operation="GET"}' % $.jobMatcher('querier'))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_gcs_request_duration_seconds', $.jobSelector('querier') + [utils.selector.eq('operation', 'GET')])
      )
    )
    .addRowIf(
      std.setMember('tsdb', $._config.storage_engine),
      $.row('Store-gateway - Blocks')
      .addPanel(
        $.successFailurePanel(
          'Block Loads / sec',
          'sum(rate(cortex_storegateway_bucket_store_block_loads_total{%s}[$__interval])) - sum(rate(cortex_storegateway_bucket_store_block_load_failures_total{%s}[$__interval]))' % [$.namespaceMatcher(), $.namespaceMatcher()],
          'sum(rate(cortex_storegateway_bucket_store_block_load_failures_total{%s}[$__interval]))' % $.namespaceMatcher(),
        )
      )
      .addPanel(
        $.successFailurePanel(
          'Block Drops / sec',
          'sum(rate(cortex_storegateway_bucket_store_block_drops_total{%s}[$__interval])) - sum(rate(cortex_storegateway_bucket_store_block_drop_failures_total{%s}[$__interval]))' % [$.namespaceMatcher(), $.namespaceMatcher()],
          'sum(rate(cortex_storegateway_bucket_store_block_drop_failures_total{%s}[$__interval]))' % $.namespaceMatcher(),
        )
      )
      .addPanel(
        $.panel('Per-block prepares and preloads duration') +
        $.latencyPanel('cortex_storegateway_bucket_store_series_get_all_duration_seconds', '{%s}' % $.namespaceMatcher()),
      )
      .addPanel(
        $.panel('Series merge duration') +
        $.latencyPanel('cortex_storegateway_bucket_store_series_merge_duration_seconds', '{%s}' % $.namespaceMatcher()),
      )
    )
    // Object store metrics for the store-gateway.
    .addRowIf(
      std.setMember('tsdb', $._config.storage_engine),
      $.objectStorePanels1('Store-gateway - Blocks Object Store', 'cortex_storegateway'),
    )
    .addRowIf(
      std.setMember('tsdb', $._config.storage_engine),
      $.objectStorePanels2('', 'cortex_storegateway'),
    )
    // Object store metrics for the querier.
    .addRowIf(
      std.setMember('tsdb', $._config.storage_engine),
      $.objectStorePanels1('Querier - Blocks Object Store', 'cortex_querier'),
    )
    .addRowIf(
      std.setMember('tsdb', $._config.storage_engine),
      $.objectStorePanels2('', 'cortex_querier'),
    ),
}
