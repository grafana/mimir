local utils = import 'mixin-utils/utils.libsonnet';

(import 'dashboard-utils.libsonnet') {
  'cortex-writes.json':
    ($.dashboard('Cortex / Writes') + { uid: '0156f6d15aa234d452a33a4f13c838e3' })
    .addClusterSelectorTemplates()
    .addRow(
      ($.row('Headlines') +
       {
         height: '100px',
         showTitle: false,
       })
      .addPanel(
        $.panel('Samples / s') +
        $.statPanel(
          'sum(%(job_aggregation_prefix)s:cortex_distributor_received_samples:rate5m{%(job)s})' % (
            $._config {
              job: $.jobMatcher($._config.job_names.distributor),
            }
          ),
          format='reqps'
        )
      )
      .addPanel(
        $.panel('Active Series') +
        $.statPanel(|||
          sum(cortex_ingester_memory_series{%(ingester)s}
          / on(%(labels)s) group_left
          max by (%(labels)s) (cortex_distributor_replication_factor{%(distributor)s}))
        ||| % {
          ingester: $.jobMatcher($._config.job_names.ingester),
          distributor: $.jobMatcher($._config.job_names.distributor),
          labels: $._config.job_aggregation_labels_active_series,
        }, format='short')
      )
      .addPanel(
        $.panel('Tenants') +
        $.statPanel('count(count by(user) (cortex_ingester_active_series{%s}))' % $.jobMatcher($._config.job_names.ingester), format='short')
      )
      .addPanel(
        $.panel('QPS') +
        $.statPanel('sum(rate(cortex_request_duration_seconds_count{%s, route=~"api_(v1|prom)_push"}[5m]))' % $.jobMatcher($._config.job_names.gateway), format='reqps')
      )
    )
    .addRow(
      $.row('Gateway')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_request_duration_seconds_count{%s, route=~"api_(v1|prom)_push"}' % $.jobMatcher($._config.job_names.gateway))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector($._config.job_names.gateway) + [utils.selector.re('route', 'api_(v1|prom)_push')])
      )
      .addPanel(
        $.panel('Per %s p99 Latency' % $._config.per_instance_label) +
        $.hiddenLegendQueryPanel(
          'histogram_quantile(0.99, sum by(le, %s) (rate(cortex_request_duration_seconds_bucket{%s, route=~"api_(v1|prom)_push"}[$__rate_interval])))' % [$._config.per_instance_label, $.jobMatcher($._config.job_names.gateway)], ''
        ) +
        { yaxes: $.yaxes('s') }
      )
    )
    .addRow(
      $.row('Distributor')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_request_duration_seconds_count{%s, route=~"/distributor.Distributor/Push|/httpgrpc.*|api_(v1|prom)_push"}' % $.jobMatcher($._config.job_names.distributor))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector($._config.job_names.distributor) + [utils.selector.re('route', '/distributor.Distributor/Push|/httpgrpc.*|api_(v1|prom)_push')])
      )
      .addPanel(
        $.panel('Per %s p99 Latency' % $._config.per_instance_label) +
        $.hiddenLegendQueryPanel(
          'histogram_quantile(0.99, sum by(le, %s) (rate(cortex_request_duration_seconds_bucket{%s, route=~"/distributor.Distributor/Push|/httpgrpc.*|api_(v1|prom)_push"}[$__rate_interval])))' % [$._config.per_instance_label, $.jobMatcher($._config.job_names.distributor)], ''
        ) +
        { yaxes: $.yaxes('s') }
      )
    )
    .addRow(
      $.row('KV Store (HA Dedupe)')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_kv_request_duration_seconds_count{%s}' % $.jobMatcher($._config.job_names.distributor))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_kv_request_duration_seconds', $.jobSelector($._config.job_names.distributor))
      )
    )
    .addRow(
      $.row('Ingester')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_request_duration_seconds_count{%s,route="/cortex.Ingester/Push"}' % $.jobMatcher($._config.job_names.ingester))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector($._config.job_names.ingester) + [utils.selector.eq('route', '/cortex.Ingester/Push')])
      )
      .addPanel(
        $.panel('Per %s p99 Latency' % $._config.per_instance_label) +
        $.hiddenLegendQueryPanel(
          'histogram_quantile(0.99, sum by(le, %s) (rate(cortex_request_duration_seconds_bucket{%s, route="/cortex.Ingester/Push"}[$__rate_interval])))' % [$._config.per_instance_label, $.jobMatcher($._config.job_names.ingester)], ''
        ) +
        { yaxes: $.yaxes('s') }
      )
    )
    .addRow(
      $.row('KV Store (Ring)')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_kv_request_duration_seconds_count{%s}' % $.jobMatcher($._config.job_names.ingester))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_kv_request_duration_seconds', $.jobSelector($._config.job_names.ingester))
      )
    )
    .addRowIf(
      std.member($._config.storage_engine, 'chunks'),
      $.row('Memcached')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_memcache_request_duration_seconds_count{%s,method="Memcache.Put"}' % $.jobMatcher($._config.job_names.ingester))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_memcache_request_duration_seconds', $.jobSelector($._config.job_names.ingester) + [utils.selector.eq('method', 'Memcache.Put')])
      )
    )
    .addRowIf(
      std.member($._config.storage_engine, 'chunks') &&
      std.member($._config.chunk_index_backend + $._config.chunk_store_backend, 'cassandra'),
      $.row('Cassandra')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_cassandra_request_duration_seconds_count{%s, operation="INSERT"}' % $.jobMatcher($._config.job_names.ingester))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_cassandra_request_duration_seconds', $.jobSelector($._config.job_names.ingester) + [utils.selector.eq('operation', 'INSERT')])
      )
    )
    .addRowIf(
      std.member($._config.storage_engine, 'chunks') &&
      std.member($._config.chunk_index_backend + $._config.chunk_store_backend, 'bigtable'),
      $.row('BigTable')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_bigtable_request_duration_seconds_count{%s, operation="/google.bigtable.v2.Bigtable/MutateRows"}' % $.jobMatcher($._config.job_names.ingester))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_bigtable_request_duration_seconds', $.jobSelector($._config.job_names.ingester) + [utils.selector.eq('operation', '/google.bigtable.v2.Bigtable/MutateRows')])
      )
    )
    .addRowIf(
      std.member($._config.storage_engine, 'chunks') &&
      std.member($._config.chunk_index_backend + $._config.chunk_store_backend, 'dynamodb'),
      $.row('DynamoDB')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_dynamo_request_duration_seconds_count{%s, operation="DynamoDB.BatchWriteItem"}' % $.jobMatcher($._config.job_names.ingester))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_dynamo_request_duration_seconds', $.jobSelector($._config.job_names.ingester) + [utils.selector.eq('operation', 'DynamoDB.BatchWriteItem')])
      )
    )
    .addRowIf(
      std.member($._config.storage_engine, 'chunks') &&
      std.member($._config.chunk_store_backend, 'gcs'),
      $.row('GCS')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_gcs_request_duration_seconds_count{%s, operation="POST"}' % $.jobMatcher($._config.job_names.ingester))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_gcs_request_duration_seconds', $.jobSelector($._config.job_names.ingester) + [utils.selector.eq('operation', 'POST')])
      )
    )
    .addRowIf(
      std.member($._config.storage_engine, 'blocks'),
      $.row('Ingester - Blocks storage - Shipper')
      .addPanel(
        $.successFailurePanel(
          'Uploaded blocks / sec',
          'sum(rate(cortex_ingester_shipper_uploads_total{%s}[$__rate_interval])) - sum(rate(cortex_ingester_shipper_upload_failures_total{%s}[$__rate_interval]))' % [$.jobMatcher($._config.job_names.ingester), $.jobMatcher($._config.job_names.ingester)],
          'sum(rate(cortex_ingester_shipper_upload_failures_total{%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.ingester),
        ),
      )
      .addPanel(
        $.panel('Upload latency') +
        $.latencyPanel('thanos_objstore_bucket_operation_duration_seconds', '{%s,component="ingester",operation="upload"}' % $.jobMatcher($._config.job_names.ingester)),
      )
    )
    .addRowIf(
      std.member($._config.storage_engine, 'blocks'),
      $.row('Ingester - Blocks storage - TSDB Head')
      .addPanel(
        $.successFailurePanel(
          'Compactions / sec',
          'sum(rate(cortex_ingester_tsdb_compactions_total{%s}[$__rate_interval]))' % [$.jobMatcher($._config.job_names.ingester)],
          'sum(rate(cortex_ingester_tsdb_compactions_failed_total{%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.ingester),
        ),
      )
      .addPanel(
        $.panel('Compactions latency') +
        $.latencyPanel('cortex_ingester_tsdb_compaction_duration_seconds', '{%s}' % $.jobMatcher($._config.job_names.ingester)),
      )
    )
    .addRowIf(
      std.member($._config.storage_engine, 'blocks'),
      $.row('Ingester - Blocks storage - TSDB WAL')
      .addPanel(
        $.successFailurePanel(
          'WAL truncations / sec',
          'sum(rate(cortex_ingester_tsdb_wal_truncations_total{%s}[$__rate_interval])) - sum(rate(cortex_ingester_tsdb_wal_truncations_failed_total{%s}[$__rate_interval]))' % [$.jobMatcher($._config.job_names.ingester), $.jobMatcher($._config.job_names.ingester)],
          'sum(rate(cortex_ingester_tsdb_wal_truncations_failed_total{%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.ingester),
        ),
      )
      .addPanel(
        $.successFailurePanel(
          'Checkpoints created / sec',
          'sum(rate(cortex_ingester_tsdb_checkpoint_creations_total{%s}[$__rate_interval])) - sum(rate(cortex_ingester_tsdb_checkpoint_creations_failed_total{%s}[$__rate_interval]))' % [$.jobMatcher($._config.job_names.ingester), $.jobMatcher($._config.job_names.ingester)],
          'sum(rate(cortex_ingester_tsdb_checkpoint_creations_failed_total{%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.ingester),
        ),
      )
      .addPanel(
        $.panel('WAL truncations latency (includes checkpointing)') +
        $.queryPanel('sum(rate(cortex_ingester_tsdb_wal_truncate_duration_seconds_sum{%s}[$__rate_interval])) / sum(rate(cortex_ingester_tsdb_wal_truncate_duration_seconds_count{%s}[$__rate_interval]))' % [$.jobMatcher($._config.job_names.ingester), $.jobMatcher($._config.job_names.ingester)], 'avg') +
        { yaxes: $.yaxes('s') },
      )
      .addPanel(
        $.panel('Corruptions / sec') +
        $.queryPanel([
          'sum(rate(cortex_ingester_wal_corruptions_total{%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.ingester),
          'sum(rate(cortex_ingester_tsdb_mmap_chunk_corruptions_total{%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.ingester),
        ], [
          'WAL',
          'mmap-ed chunks',
        ]) +
        $.stack + {
          yaxes: $.yaxes('ops'),
          aliasColors: {
            WAL: '#E24D42',
            'mmap-ed chunks': '#E28A42',
          },
        },
      )
    ),
}
