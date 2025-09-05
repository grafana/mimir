local filename = 'mimir-block-builder.json';

(import 'dashboard-utils.libsonnet') +
{
  [filename]:
    assert std.md5(filename) == 'c565e768420d79d0a632b3135d47cb30' : 'UID of the dashboard has changed, please update references to dashboard.';
    ($.dashboard('Block-builder') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates()
    .addRow(
      $.row('Scheduler summary')
      .addPanel(
        $.timeseriesPanel('Jobs') +
        $.panelDescription(
          'Outstanding jobs',
          'Number of outstanding and active jobs.',
        ) +
        $.queryPanel(
          [
            'sum(cortex_blockbuilder_scheduler_outstanding_jobs{%(job)s})' % { job: $.jobMatcher($._config.job_names.block_builder_scheduler) },
            'sum(cortex_blockbuilder_scheduler_assigned_jobs{%(job)s})' % { job: $.jobMatcher($._config.job_names.block_builder_scheduler) },
          ],
          [
            'outstanding',
            'active',
          ],
        ) +
        { fieldConfig+: { defaults+: { custom+: { unit: 'short', fillOpacity: 100 } } } },
      )
      .addPanel(
        $.timeseriesPanel('Job Update Duration') +
        $.panelDescription(
          'Scheduler Job Update Duration',
          'Amount of time the scheduler took to calculate the jobs.'
        ) +
        $.queryPanel(
          [
            'histogram_quantile(0.50, sum (rate(cortex_blockbuilder_scheduler_schedule_update_seconds{%(job)s}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.block_builder_scheduler)],
            'histogram_quantile(0.99, sum (rate(cortex_blockbuilder_scheduler_schedule_update_seconds{%(job)s}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.block_builder_scheduler)],
            'histogram_avg(sum (rate(cortex_blockbuilder_scheduler_schedule_update_seconds{%(job)s}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.block_builder_scheduler)],
          ],
          [
            '50th percentile',
            '99th percentile',
            'average',
          ],
        ) +
        { fieldConfig+: { defaults+: { unit: 's' } } },
      )
      .addPanel(
        $.timeseriesPanel('Errors') +
        $.panelDescription(
          'Errors',
          'Various errors exposed by the scheduler.',
        ) +
        $.queryPanel(
          [
            'sum(increase(cortex_blockbuilder_scheduler_fetch_offsets_failed_total{%(job)s}[$__rate_interval]))' % { job: $.jobMatcher($._config.job_names.block_builder_scheduler) },
            'sum(increase(cortex_blockbuilder_scheduler_flush_failed_total{%(job)s}[$__rate_interval]))' % { job: $.jobMatcher($._config.job_names.block_builder_scheduler) },
            'sum(increase(cortex_blockbuilder_scheduler_job_gap_detected{%(job)s}[$__rate_interval]))' % { job: $.jobMatcher($._config.job_names.block_builder_scheduler) },
          ],
          [
            'fetch offsets failed',
            'flush failed',
            'offset gap detected',
          ],
        )
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.timeseriesPanel('Partition Lag') +
        $.panelDescription(
          'Partition Lag',
          'Number of records in the backlog of a partition.',
        ) +
        $.queryPanel(
          '(cortex_blockbuilder_scheduler_partition_end_offset{%(job)s} -cortex_blockbuilder_scheduler_partition_committed_offset{%(job)s}) > 0' % { job: $.jobMatcher($._config.job_names.block_builder_scheduler) },
          '{{partition}}',
        ) +
        { fieldConfig+: { defaults+: { custom+: { unit: 'short', fillOpacity: 0 } } } },
      )
      .addPanel(
        $.timeseriesPanel('Pending Jobs by Partition') +
        $.panelDescription(
          'Pending jobs by partition',
          |||
            Number of pending jobs by a partition.

            Positive number of pending jobs indicates that there is a backlog of jobs that aren't yet eligible for scheduling.
          |||,
        ) +
        $.queryPanel(
          'sum by (partition) (cortex_blockbuilder_scheduler_pending_jobs{%(job)s}) > 0' % { job: $.jobMatcher($._config.job_names.block_builder_scheduler) },
          '{{partition}}',
        ) +
        { fieldConfig+: { defaults+: { custom+: { unit: 'short', fillOpacity: 0 } } } },
      )
    )
    .addRow(
      $.row('Block builder summary')
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
        ),
      )
      .addPanel(
        $.timeseriesPanel('Job processing duration') +
        $.panelDescription(
          'Job processing duration',
          'Amount of time that it takes to process a job.'
        ) +
        $.queryPanel(
          [
            'histogram_quantile(0.50, sum (rate(cortex_blockbuilder_consume_job_duration_seconds{%(job)s}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.block_builder)],
            'histogram_quantile(0.99, sum (rate(cortex_blockbuilder_consume_job_duration_seconds{%(job)s}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.block_builder)],
            'histogram_avg(sum (rate(cortex_blockbuilder_consume_job_duration_seconds{%(job)s}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.block_builder)],
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
        $.timeseriesPanel('Partition compactions / sec') +
        $.panelDescription(
          'Partition compactions / sec',
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
      $.row('Block builder resources')
      .addPanel(
        $.containerCPUUsagePanelByComponent('block_builder'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanelByComponent('block_builder'),
      )
    )
    .addRowIf(
      $._config.autoscaling.block_builder.enabled,
      $.row('Block builder - autoscaling')
      .addPanel(
        $.autoScalingActualReplicas('block_builder')
      )
      .addPanel(
        $.autoScalingDesiredReplicasByAverageValueScalingMetricPanel('block_builder', scalingMetricName='', scalingMetricID='')
      )
      .addPanel(
        $.autoScalingFailuresPanel('block_builder')
      )
    )
    .addRow(
      $.row('Scheduler resources')
      .addPanel(
        $.containerCPUUsagePanelByComponent('block_builder_scheduler'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanelByComponent('block_builder_scheduler'),
      )
    ),
}
