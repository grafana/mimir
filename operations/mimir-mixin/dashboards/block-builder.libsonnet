local filename = 'mimir-block-builder.json';

(import 'dashboard-utils.libsonnet') +
{
  [filename]:
    assert std.md5(filename) == 'c565e768420d79d0a632b3135d47cb30' : 'UID of the dashboard has changed, please update references to dashboard.';
    ($.dashboard('Block-builder') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates()
    .addRow(
      $.row('Summary')
      .addPanel(
        $.timeseriesPanel('Kafka fetched records / sec') +
        $.panelDescription(
          'Kafka fetched records / sec',
          'Overview of per-second rate of records fetched from Kafka.',
        ) +
        $.queryPanel(
          [
            'sum (rate(cortex_ingest_storage_reader_fetch_records_total{%(job)s}[$__rate_interval]))' % { job: $.jobMatcher($._config.job_names.block_builder) },
            'sum (rate(cortex_ingest_storage_reader_read_errors_total{%(job)s}[$__rate_interval]))' % { job: $.jobMatcher($._config.job_names.block_builder) },
          ],
          [
            'successful',
            'read errors',
          ],
        ) +
        $.stack,
      )
      .addPanel(
        $.timeseriesPanel('Per pod Kafka fetched records / sec') +
        $.panelDescription(
          'Per pod Kafka fetched records / sec',
          'Overview of per-second rate of records fetched from Kafka split by pods.',
        ) +
        $.queryPanel(
          'sum by (pod) (rate(cortex_ingest_storage_reader_fetch_records_total{%(job)s}[$__rate_interval]))' % { job: $.jobMatcher($._config.job_names.block_builder) },
          '{{pod}}'
        ) +
        $.stack,
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.timeseriesPanel('Partition processing / sec') +
        $.panelDescription(
          'Partition processing / sec',
          'Per-partition rate of consumption cycles.',
        ) +
        $.queryPanel(
          'sum by (partition) (histogram_count(increase(cortex_blockbuilder_process_partition_duration_seconds{%(job)s}[1m])))' % [$.jobMatcher($._config.job_names.block_builder)],
          '{{partition}}'
        )
      )
      .addPanel(
        $.timeseriesPanel('Lag records') +
        $.panelDescription(
          'Per partition records lag',
          'Number of records in the backlog of a partition, as seen when starting a consumption cycle.'
        ) +
        $.queryPanel(
          'max by (partition) (cortex_blockbuilder_consumer_lag_records{%(job)s}) > 0' % [$.jobMatcher($._config.job_names.block_builder)],
          '{{partition}}',
        )
      )
      .addPanel(
        $.timeseriesPanel('Partition processing duration') +
        $.panelDescription(
          'Partition processing duration',
          'Amount of time that it takes to process a partition.'
        ) +
        $.queryPanel(
          [
            'histogram_quantile(0.50, sum (rate(cortex_blockbuilder_process_partition_duration_seconds{%(job)s}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.block_builder)],
            'histogram_quantile(0.99, sum (rate(cortex_blockbuilder_process_partition_duration_seconds{%(job)s}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.block_builder)],
            'histogram_avg(sum (rate(cortex_blockbuilder_process_partition_duration_seconds{%(job)s}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.block_builder)],
          ],
          [
            '50th percentile',
            '99th percentile',
            'average',
          ],
        ) +
        { fieldConfig+: { defaults+: { unit: 's' } } },
      )
    )
    .addRow(
      $.row('TSDB')
      .addPanel(
        $.timeseriesPanel('Partition compactios / sec') +
        $.panelDescription(
          'Partition compactios / sec',
          'Per-second rate of compact and upload operattions for blocks of the processed partitions.'
        ) +
        $.queryPanel(
          [
            'sum (histogram_count(rate(cortex_blockbuilder_tsdb_compact_and_upload_duration_seconds{%(job)s}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.block_builder)],
            'sum (rate(cortex_blockbuilder_tsdb_compact_and_upload_failed_total{%(job)s}[$__rate_interval]))' % [$.jobMatcher($._config.job_names.block_builder)],
          ],
          [
            'successful',
            'failed',
          ],
        ) +
        $.stack,
      )
      .addPanel(
        $.timeseriesPanel('Partition compaction duration') +
        $.panelDescription(
          'Partition compaction duration',
          'Amount of time that it takes to compact and upload blocks of the processed partitions.'
        ) +
        $.queryPanel(
          [
            'histogram_quantile(0.50, sum (rate(cortex_blockbuilder_tsdb_compact_and_upload_duration_seconds{%(job)s}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.block_builder)],
            'histogram_quantile(0.99, sum (rate(cortex_blockbuilder_tsdb_compact_and_upload_duration_seconds{%(job)s}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.block_builder)],
            'histogram_avg(sum (rate(cortex_blockbuilder_tsdb_compact_and_upload_duration_seconds{%(job)s}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.block_builder)],
          ],
          [
            '50th percentile',
            '99th percentile',
            'average',
          ],
        ) +
        { fieldConfig+: { defaults+: { unit: 's' } } },
      )
    )
    .addRow(
      $.row('Resources')
      .addPanel(
        $.containerCPUUsagePanelByComponent('block_builder'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanelByComponent('block_builder'),
      )
    ),
}
