local utils = import 'mixin-utils/utils.libsonnet';

(import 'dashboard-utils.libsonnet') {
  'cortex-writes.json':
    $.dashboard('Cortex / Writes')
    .addClusterSelectorTemplates()
    .addRow(
      ($.row('Headlines') +
       {
         height: '100px',
         showTitle: false,
       })
      .addPanel(
        $.panel('Samples / s') +
        $.statPanel('sum(cluster_namespace:cortex_distributor_received_samples:rate5m{%s})' % $.namespaceMatcher(), format='reqps')
      )
      .addPanel(
        $.panel('Active Series') +
        $.statPanel(|||
          sum(cortex_ingester_memory_series{%(ingester)s}
          / on(namespace) group_left
          max by (namespace) (cortex_distributor_replication_factor{%(distributor)s}))
        ||| % {
          ingester: $.jobMatcher('ingester'),
          distributor: $.jobMatcher('distributor'),
        }, format='short')
      )
      .addPanel(
        $.panel('QPS') +
        $.statPanel('sum(rate(cortex_request_duration_seconds_count{%s, route="api_prom_push"}[5m]))' % $.jobMatcher('cortex-gw'), format='reqps')
      )
    )
    .addRow(
      $.row('Gateway')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_request_duration_seconds_count{%s, route="api_prom_push"}' % $.jobMatcher('cortex-gw'))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector('cortex-gw') + [utils.selector.eq('route', 'api_prom_push')])
      )
    )
    .addRow(
      $.row('Distributor')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_request_duration_seconds_count{%s, route="api_prom_push"}' % $.jobMatcher('distributor'))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector('distributor') + [utils.selector.eq('route', 'api_prom_push')])
      )
    )
    .addRow(
      $.row('Etcd (HA Dedupe)')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_kv_request_duration_seconds_count{%s}' % $.jobMatcher('distributor'))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_kv_request_duration_seconds', $.jobSelector('distributor'))
      )
    )
    .addRow(
      $.row('Ingester')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_request_duration_seconds_count{%s,route="/cortex.Ingester/Push"}' % $.jobMatcher('ingester'))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector('ingester') + [utils.selector.eq('route', '/cortex.Ingester/Push')])
      )
    )
    .addRow(
      $.row('Consul (Ring)')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_kv_request_duration_seconds_count{%s}' % $.jobMatcher('ingester'))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_kv_request_duration_seconds', $.jobSelector('ingester'))
      )
    )
    .addRowIf(
      std.setMember('chunks', $._config.storage_engine),
      $.row('Memcached')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_memcache_request_duration_seconds_count{%s,method="Memcache.Put"}' % $.jobMatcher('ingester'))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_memcache_request_duration_seconds', $.jobSelector('ingester') + [utils.selector.eq('method', 'Memcache.Put')])
      )
    )
    .addRowIf(
      std.setMember('chunks', $._config.storage_engine) &&
      std.setMember('cassandra', $._config.chunk_index_backend + $._config.chunk_store_backend),
      $.row('Cassandra')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_cassandra_request_duration_seconds_count{%s, operation="INSERT"}' % $.jobMatcher('ingester'))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_cassandra_request_duration_seconds', $.jobSelector('ingester') + [utils.selector.eq('operation', 'INSERT')])
      )
    )
    .addRowIf(
      std.setMember('chunks', $._config.storage_engine) &&
      std.setMember('bigtable', $._config.chunk_index_backend + $._config.chunk_store_backend),
      $.row('BigTable')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_bigtable_request_duration_seconds_count{%s, operation="/google.bigtable.v2.Bigtable/MutateRows"}' % $.jobMatcher('ingester'))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_bigtable_request_duration_seconds', $.jobSelector('ingester') + [utils.selector.eq('operation', '/google.bigtable.v2.Bigtable/MutateRows')])
      )
    )
    .addRowIf(
      std.setMember('chunks', $._config.storage_engine) &&
      std.setMember('dynamodb', $._config.chunk_index_backend + $._config.chunk_store_backend),
      $.row('DynamoDB')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_dynamo_request_duration_seconds_count{%s, operation="DynamoDB.BatchWriteItem"}' % $.jobMatcher('ingester'))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_dynamo_request_duration_seconds', $.jobSelector('ingester') + [utils.selector.eq('operation', 'DynamoDB.BatchWriteItem')])
      )
    )
    .addRowIf(
      std.setMember('chunks', $._config.storage_engine) &&
      std.setMember('gcs', $._config.chunk_store_backend),
      $.row('GCS')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_gcs_request_duration_seconds_count{%s, operation="POST"}' % $.jobMatcher('ingester'))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_gcs_request_duration_seconds', $.jobSelector('ingester') + [utils.selector.eq('operation', 'POST')])
      )
    )
    .addRowIf(
      std.setMember('tsdb', $._config.storage_engine),
      $.row('Blocks Shipper')
      .addPanel(
        $.successFailurePanel(
          'Uploaded blocks / sec',
          'sum(rate(cortex_ingester_shipper_uploads_total{%s}[$__interval])) - sum(rate(cortex_ingester_shipper_upload_failures_total{%s}[$__interval]))' % [$.namespaceMatcher(), $.namespaceMatcher()],
          'sum(rate(cortex_ingester_shipper_upload_failures_total{%s}[$__interval]))' % [$.namespaceMatcher()],
        ),
      )
    )
    .addRowIf(
      std.setMember('tsdb', $._config.storage_engine),
      $.objectStorePanels1('Blocks Object Store Stats (Ingester)', 'cortex_ingester'),
    )
    .addRowIf(
      std.setMember('tsdb', $._config.storage_engine),
      $.objectStorePanels2('', 'cortex_ingester'),
    ),
}
