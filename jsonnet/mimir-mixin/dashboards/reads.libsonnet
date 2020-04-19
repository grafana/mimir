local utils = import 'mixin-utils/utils.libsonnet';

(import 'dashboard-utils.libsonnet') {
  'cortex-reads.json':
    $.dashboard('Cortex / Reads')
    .addClusterSelectorTemplates()
    .addRow(
      $.row('Gateway')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_request_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/cortex-gw", route=~"(api_prom_api_v1_query_range|api_prom_api_v1_query|api_prom_api_v1_label_name_values|api_prom_api_v1_series|api_prom_api_v1_labels)"}')
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_request_duration_seconds', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/cortex-gw'), utils.selector.re('route', '(api_prom_api_v1_query_range|api_prom_api_v1_query|api_prom_api_v1_label_name_values|api_prom_api_v1_series|api_prom_api_v1_labels)')])
      )
    )
    .addRow(
      $.row('Query Frontend')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_request_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/query-frontend"}')
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_request_duration_seconds', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/query-frontend'), utils.selector.neq('route', '/frontend.Frontend/Process')])
      )
    )
    .addRow(
      $.row('Cache - Query Results')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_cache_request_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/query-frontend"}')
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_cache_request_duration_seconds', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/query-frontend')])
      )
    )
    .addRow(
      $.row('Querier')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_request_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/querier"}')
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_request_duration_seconds', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/querier')])
      )
    )
    .addRow(
      $.row('Ingester')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_request_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/ingester",route!~"/cortex.Ingester/Push|metrics|ready|traces"}')
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_request_duration_seconds', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/ingester'), utils.selector.nre('route', '/cortex.Ingester/Push|metrics|ready')])
      )
    )
    .addRowIf(
      std.setMember('chunks', $._config.storage_engine),
      $.row('Memcached - Index')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_cache_request_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/querier",method="store.index-cache-read.memcache.fetch"}')
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_cache_request_duration_seconds', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/querier'), utils.selector.eq('method', 'store.index-cache-read.memcache.fetch')])
      )
    )
    .addRowIf(
      std.setMember('chunks', $._config.storage_engine),
      $.row('Memcached - Chunks')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_cache_request_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/querier",method="chunksmemcache.fetch"}')
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_cache_request_duration_seconds', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/querier'), utils.selector.eq('method', 'chunksmemcache.fetch')])
      )
    )
    .addRowIf(
      std.setMember('tsdb', $._config.storage_engine),
      $.row('Memcached - Blocks Index')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_querier_blocks_index_cache_memcached_operation_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/querier",operation="getmulti"}')
      )
      .addPanel(
        $.panel('Latency') +
        $.latencyPanel('cortex_querier_blocks_index_cache_memcached_operation_duration_seconds', '{cluster=~"$cluster", job=~"($namespace)/querier", operation="getmulti"}')
      )
    )
    .addRowIf(
      std.setMember('chunks', $._config.storage_engine) &&
      std.setMember('cassandra', $._config.chunk_index_backend + $._config.chunk_store_backend),
      $.row('Cassandra')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_cassandra_request_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/querier", operation="SELECT"}')
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_cassandra_request_duration_seconds', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/querier'), utils.selector.eq('operation', 'SELECT')])
      )
    )
    .addRowIf(
      std.setMember('chunks', $._config.storage_engine) &&
      std.setMember('bigtable', $._config.chunk_index_backend + $._config.chunk_store_backend),
      $.row('BigTable')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_bigtable_request_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/querier", operation="/google.bigtable.v2.Bigtable/ReadRows"}')
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_bigtable_request_duration_seconds', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/querier'), utils.selector.eq('operation', '/google.bigtable.v2.Bigtable/ReadRows')])
      ),
    )
    .addRowIf(
      std.setMember('chunks', $._config.storage_engine) &&
      std.setMember('dynamodb', $._config.chunk_index_backend + $._config.chunk_store_backend),
      $.row('DynamoDB')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_dynamo_request_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/querier", operation="DynamoDB.QueryPages"}')
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_dynamo_request_duration_seconds', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/querier'), utils.selector.eq('operation', 'DynamoDB.QueryPages')])
      ),
    )
    .addRowIf(
      std.setMember('chunks', $._config.storage_engine) &&
      std.setMember('gcs', $._config.chunk_store_backend),
      $.row('GCS')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_gcs_request_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/querier", operation="GET"}')
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_gcs_request_duration_seconds', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/querier'), utils.selector.eq('operation', 'GET')])
      )
    )
    .addRowIf(
      std.setMember('tsdb', $._config.storage_engine),
      $.row('Querier - Blocks Storage')
      .addPanel(
        $.successFailurePanel(
          'Block Loads / sec',
          'sum(rate(cortex_querier_bucket_store_block_loads_total{cluster=~"$cluster"}[$__interval])) - sum(rate(cortex_querier_bucket_store_block_load_failures_total{cluster=~"$cluster"}[$__interval]))',
          'sum(rate(cortex_querier_bucket_store_block_load_failures_total{cluster=~"$cluster"}[$__interval]))'
        )
      )
      .addPanel(
        $.successFailurePanel(
          'Block Drops / sec',
          'sum(rate(cortex_querier_bucket_store_block_drops_total{cluster=~"$cluster"}[$__interval])) - sum(rate(cortex_querier_bucket_store_block_drop_failures_total{cluster=~"$cluster"}[$__interval]))',
          'sum(rate(cortex_querier_bucket_store_block_drop_failures_total{cluster=~"$cluster"}[$__interval]))'
        )
      )
      .addPanel(
        $.panel('Per-block prepares and preloads duration') +
        $.latencyPanel('cortex_querier_bucket_store_series_get_all_duration_seconds', '{cluster=~"$cluster"}'),
      )
      .addPanel(
        $.panel('Series merge duration') +
        $.latencyPanel('cortex_querier_bucket_store_series_merge_duration_seconds', '{cluster=~"$cluster"}'),
      )
    )
    .addRowIf(
      std.setMember('tsdb', $._config.storage_engine),
      $.objectStorePanels1('Blocks Object Store Stats (Querier)', 'cortex_querier'),
    )
    .addRowIf(
      std.setMember('tsdb', $._config.storage_engine),
      $.objectStorePanels2('', 'cortex_querier'),
    ),
}
