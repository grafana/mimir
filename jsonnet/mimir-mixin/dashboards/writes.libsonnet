local utils = import 'mixin-utils/utils.libsonnet';

(import 'dashboard-utils.libsonnet') {
  'cortex-writes.json':
    ($.dashboard('Cortex / Writes') + { uid: '0156f6d15aa234d452a33a4f13c838e3' })
    .addClusterSelectorTemplates()
    .addRow(
      ($.row('Writes Summary') { height: '125px', showTitle: false })
      .addPanel(
        $.textPanel('', |||
          <p>
            This dashboard shows various health metrics for the Cortex write path.
            It is broken into sections for each service on the write path, 
            and organized by the order in which the write request flows.
            <br/>
            Incoming metrics data travels from the gateway → distributor → ingester.
          </p> 
          <p>
            It also includes metrics for the key-value (KV) stores used to manage
            the High Availability Tracker and the Ingesters.
          </p>
        |||),
      )
    ).addRow(
      ($.row('Headlines') +
       {
         height: '100px',
         showTitle: false,
       })
      .addPanel(
        $.panel('Samples / s') +
        $.statPanel(
          'sum(%(group_prefix_jobs)s:cortex_distributor_received_samples:rate5m{%(job)s})' % (
            $._config {
              job: $.jobMatcher($._config.job_names.distributor),
            }
          ),
          format='short'
        )
      )
      .addPanel(
        $.panel('Active Series') +
        $.statPanel(|||
          sum(cortex_ingester_memory_series{%(ingester)s}
          / on(%(group_by_cluster)s) group_left
          max by (%(group_by_cluster)s) (cortex_distributor_replication_factor{%(distributor)s}))
        ||| % ($._config) {
          ingester: $.jobMatcher($._config.job_names.ingester),
          distributor: $.jobMatcher($._config.job_names.distributor),
        }, format='short')
      )
      .addPanel(
        $.panel('Tenants') +
        $.statPanel('count(count by(user) (cortex_ingester_active_series{%s}))' % $.jobMatcher($._config.job_names.ingester), format='short')
      )
      .addPanel(
        $.panel('Requests Per Second') +
        $.statPanel('sum(rate(cortex_request_duration_seconds_count{%s, route=~"api_(v1|prom)_push"}[5m]))' % $.jobMatcher($._config.job_names.gateway), format='reqps')
      )
    )
    .addRow(
      $.row('Gateway')
      .addPanel(
        $.panel('Requests Per Second') +
        $.qpsPanel('cortex_request_duration_seconds_count{%s, route=~"api_(v1|prom)_push"}' % $.jobMatcher($._config.job_names.gateway)) +
        $.panelDescriptionRps('gateway')
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector($._config.job_names.gateway) + [utils.selector.re('route', 'api_(v1|prom)_push')]) +
        $.panelDescriptionLatency('gateway')
      )
      .addPanel(
        $.panel('Per %s p99 Latency' % $._config.per_instance_label) +
        $.hiddenLegendQueryPanel(
          'histogram_quantile(0.99, sum by(le, %s) (rate(cortex_request_duration_seconds_bucket{%s, route=~"api_(v1|prom)_push"}[$__rate_interval])))' % [$._config.per_instance_label, $.jobMatcher($._config.job_names.gateway)], ''
        ) +
        { yaxes: $.yaxes('s') } +
        $.panelDescriptionP99Latency('gateway')
      )
    )
    .addRow(
      $.row('Distributor')
      .addPanel(
        $.panel('Requests Per Second') +
        $.qpsPanel('cortex_request_duration_seconds_count{%s, route=~"/distributor.Distributor/Push|/httpgrpc.*|api_(v1|prom)_push"}' % $.jobMatcher($._config.job_names.distributor)) +
        $.panelDescriptionRps('distributor')
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector($._config.job_names.distributor) + [utils.selector.re('route', '/distributor.Distributor/Push|/httpgrpc.*|api_(v1|prom)_push')]) +
        $.panelDescriptionLatency('distributor')
      )
      .addPanel(
        $.panel('Per %s p99 Latency' % $._config.per_instance_label) +
        $.hiddenLegendQueryPanel(
          'histogram_quantile(0.99, sum by(le, %s) (rate(cortex_request_duration_seconds_bucket{%s, route=~"/distributor.Distributor/Push|/httpgrpc.*|api_(v1|prom)_push"}[$__rate_interval])))' % [$._config.per_instance_label, $.jobMatcher($._config.job_names.distributor)], ''
        ) +
        { yaxes: $.yaxes('s') } +
        $.panelDescriptionP99Latency('distributor')
      )
    )
    .addRow(
      $.row('KV Store (HA Dedupe)')
      .addPanel(
        $.panel('Requests Per Second') +
        $.qpsPanel('cortex_kv_request_duration_seconds_count{%s}' % $.jobMatcher($._config.job_names.distributor)) +
        $.panelDescriptionRpsKvStoreDedupe()
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_kv_request_duration_seconds', $.jobSelector($._config.job_names.distributor)) +
        $.panelDescriptionLatencyKvStore()
      )
    )
    .addRow(
      $.row('Ingester')
      .addPanel(
        $.panel('Requests Per Second') +
        $.qpsPanel('cortex_request_duration_seconds_count{%s,route="/cortex.Ingester/Push"}' % $.jobMatcher($._config.job_names.ingester)) +
        $.panelDescriptionRps('ingester')
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector($._config.job_names.ingester) + [utils.selector.eq('route', '/cortex.Ingester/Push')]) +
        $.panelDescriptionLatency('ingester')
      )
      .addPanel(
        $.panel('Per %s p99 Latency' % $._config.per_instance_label) +
        $.hiddenLegendQueryPanel(
          'histogram_quantile(0.99, sum by(le, %s) (rate(cortex_request_duration_seconds_bucket{%s, route="/cortex.Ingester/Push"}[$__rate_interval])))' % [$._config.per_instance_label, $.jobMatcher($._config.job_names.ingester)], ''
        ) +
        { yaxes: $.yaxes('s') } +
        $.panelDescriptionP99Latency('ingester')
      )
    )
    .addRow(
      $.row('KV Store (Ring)')
      .addPanel(
        $.panel('Requests Per Second') +
        $.qpsPanel('cortex_kv_request_duration_seconds_count{%s}' % $.jobMatcher($._config.job_names.ingester)) +
        $.panelDescriptionRpsKvStoreRing()
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_kv_request_duration_seconds', $.jobSelector($._config.job_names.ingester)) +
        $.panelDescriptionLatencyKvStore()
      )
    )
    .addRowIf(
      std.member($._config.storage_engine, 'chunks'),
      $.row('Memcached')
      .addPanel(
        $.panel('Requests Per Second') +
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
        $.panel('Requests Per Second') +
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
        $.panel('Requests Per Second') +
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
        $.panel('Requests Per Second') +
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
        $.panel('Requests Per Second') +
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
        ) +
        $.panelDescription(
          'Uploaded blocks / sec',
          |||
            The rate of blocks being uploaded from the ingesters 
            to the long term storage/object store.
          |||
        ),
      )
      .addPanel(
        $.panel('Upload latency') +
        $.latencyPanel('thanos_objstore_bucket_operation_duration_seconds', '{%s,component="ingester",operation="upload"}' % $.jobMatcher($._config.job_names.ingester)) +
        $.panelDescription(
          'Upload latency',
          |||
            The average, median (50th percentile), and 99th percentile time 
            the ingester takes to upload blocks to the long term storage/object store.
          |||
        ),
      )
    )
    .addRowIf(
      std.member($._config.storage_engine, 'blocks'),
      $.row('Ingester - Blocks storage - TSDB Head')
      .addPanel(
        $.textPanel('', |||
          <p>
            The ingester(s) maintain a local TSDB per-tenant on disk. 
            These panels contain metrics specific to the rate of 
            compaction of data on the ingesters’ local TSDBs.
          </p> 
        |||),
      )
      .addPanel(
        $.successFailurePanel(
          'Compactions / sec',
          'sum(rate(cortex_ingester_tsdb_compactions_total{%s}[$__rate_interval]))' % [$.jobMatcher($._config.job_names.ingester)],
          'sum(rate(cortex_ingester_tsdb_compactions_failed_total{%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.ingester),
        ) +
        $.panelDescription(
          'Compactions / sec',
          |||
            This is the rate of compaction operations local to the ingesters, 
            where every 2 hours by default, a new TSDB block is created 
            by compacting the head block.
          |||
        ),
      )
      .addPanel(
        $.panel('Compactions latency') +
        $.latencyPanel('cortex_ingester_tsdb_compaction_duration_seconds', '{%s}' % $.jobMatcher($._config.job_names.ingester)) +
        $.panelDescription(
          'Compaction Latency',
          |||
            The average, median (50th percentile), and 99th percentile time 
            the ingester takes to compact the head block into a new TSDB block 
            on its local filesystem.
          |||
        ),
      )
    )
    .addRowIf(
      std.member($._config.storage_engine, 'blocks'),
      ($.row('Ingester - Blocks storage - TSDB WAL') {height: "32px"})
      .addPanel(
        $.textPanel('', |||
          <p>
            These panels contain metrics for the optional write-ahead-log (WAL) 
            that can be enabled for the local TSDBs on the ingesters. 
          </p> 
        |||),
      )
    )
    .addRowIf(
      std.member($._config.storage_engine, 'blocks'),
      ($.row('') {showTitle: false})
      .addPanel(
        $.successFailurePanel(
          'WAL truncations / sec',
          'sum(rate(cortex_ingester_tsdb_wal_truncations_total{%s}[$__rate_interval])) - sum(rate(cortex_ingester_tsdb_wal_truncations_failed_total{%s}[$__rate_interval]))' % [$.jobMatcher($._config.job_names.ingester), $.jobMatcher($._config.job_names.ingester)],
          'sum(rate(cortex_ingester_tsdb_wal_truncations_failed_total{%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.ingester),
        ) +
        $.panelDescription(
          'WAL Truncations / sec',
          |||
            The WAL is truncated each time a new TSDB block is written 
            (by default this is every 2h). This panel measures the rate of 
            truncations.
          |||
        ),
      )
      .addPanel(
        $.successFailurePanel(
          'Checkpoints created / sec',
          'sum(rate(cortex_ingester_tsdb_checkpoint_creations_total{%s}[$__rate_interval])) - sum(rate(cortex_ingester_tsdb_checkpoint_creations_failed_total{%s}[$__rate_interval]))' % [$.jobMatcher($._config.job_names.ingester), $.jobMatcher($._config.job_names.ingester)],
          'sum(rate(cortex_ingester_tsdb_checkpoint_creations_failed_total{%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.ingester),
        ) +
        $.panelDescription(
          'Checkpoints created / sec',
          |||
            Checkpoints are created as part of the WAL truncation process. 
            This metric measures the rate of checkpoint creation.
          |||
        ),
      )
      .addPanel(
        $.panel('WAL truncations latency (includes checkpointing)') +
        $.queryPanel('sum(rate(cortex_ingester_tsdb_wal_truncate_duration_seconds_sum{%s}[$__rate_interval])) / sum(rate(cortex_ingester_tsdb_wal_truncate_duration_seconds_count{%s}[$__rate_interval]))' % [$.jobMatcher($._config.job_names.ingester), $.jobMatcher($._config.job_names.ingester)], 'avg') +
        { yaxes: $.yaxes('s') } +
        $.panelDescription(
          'WAL Truncations Latency (including checkpointing)',
          |||
            Average time taken to perform a full WAL truncation, 
            including the time taken for the checkpointing to complete.
          |||
        ),
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
        } +
        $.panelDescription(
          'Corruptions / sec',
          |||
            Rate of corrupted WAL and mmap-ed chunks.
          |||
        ),
      )
    ),
} +
(
  {
    panelDescriptionRps(service)::
      $.panelDescription(
        'Requests Per Second',
        |||
          Write requests per second made to the %s(s).
        ||| % service
      ),

    panelDescriptionRpsKvStoreDedupe()::
      $.panelDescription(
        'Requests Per Second',
        |||
          Requests per second made to the key-value store 
          that manages high-availability deduplication. 
        |||
      ),

    panelDescriptionRpsKvStoreRing()::
      $.panelDescription(
        'Requests Per Second',
        |||
          Requests per second made to the key-value store 
          used to manage which ingesters own which metrics series.
        |||
      ),


    panelDescriptionLatency(service)::
      $.panelDescription(
        'Latency',
        |||
          Across all %s instances, the average, median 
          (50th percentile), and 99th percentile time to respond 
          to a request.  
        ||| % service
      ),

    panelDescriptionLatencyKvStore()::
      $.panelDescription(
        'Latency',
        |||
          The average, median (50th percentile), and 99th percentile time 
          the KV store takes to respond to a request.  
        |||
      ),

    panelDescriptionP99Latency(service)::
      $.panelDescription(
        'Per Instance P99 Latency',
        |||
          The 99th percentile latency for each individual 
          instance of the %s service.
        ||| % service
      ),
  }
)
