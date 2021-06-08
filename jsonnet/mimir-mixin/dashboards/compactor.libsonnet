local utils = import 'mixin-utils/utils.libsonnet';

(import 'dashboard-utils.libsonnet') {
  'cortex-compactor.json':
    ($.dashboard('Cortex / Compactor') + { uid: '9c408e1d55681ecb8a22c9fab46875cc' })
    .addClusterSelectorTemplates()
    .addRow(
      $.row('Summary')
      .addPanel(
        $.textPanel('', |||
          - **Per-instance runs**: number of times a compactor instance triggers a compaction across all tenants its shard manage.
          - **Tenants compaction progress**: in a multi-tenant cluster it shows the progress of tenants compacted while compaction is running. Reset to 0 once the compaction run is completed for all tenants in the shard.
        |||),
      )
      .addPanel(
        $.startedCompletedFailedPanel(
          'Per-instance runs / sec',
          'sum(rate(cortex_compactor_runs_started_total{%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.compactor),
          'sum(rate(cortex_compactor_runs_completed_total{%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.compactor),
          'sum(rate(cortex_compactor_runs_failed_total{%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.compactor)
        ) +
        $.bars +
        { yaxes: $.yaxes('ops') },
      )
      .addPanel(
        $.panel('Tenants compaction progress') +
        $.queryPanel(|||
          (
            cortex_compactor_tenants_processing_succeeded{%s} +
            cortex_compactor_tenants_processing_failed{%s} +
            cortex_compactor_tenants_skipped{%s}
          ) / cortex_compactor_tenants_discovered{%s}
        ||| % [$.jobMatcher($._config.job_names.compactor), $.jobMatcher($._config.job_names.compactor), $.jobMatcher($._config.job_names.compactor), $.jobMatcher($._config.job_names.compactor)], '{{%s}}' % $._config.per_instance_label) +
        { yaxes: $.yaxes({ format: 'percentunit', max: 1 }) },
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.textPanel('', |||
          - **Compacted blocks**: number of blocks generated as a result of a compaction operation.
          - **Per-block compaction duration**: time taken to generate a single compacted block.
        |||),
      )
      .addPanel(
        $.panel('Compacted blocks / sec') +
        $.queryPanel('sum(rate(prometheus_tsdb_compactions_total{%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.compactor), 'blocks') +
        { yaxes: $.yaxes('ops') },
      )
      .addPanel(
        $.panel('Per-block compaction duration') +
        $.latencyPanel('prometheus_tsdb_compaction_duration_seconds', '{%s}' % $.jobMatcher($._config.job_names.compactor))
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.textPanel('', |||
          - **Average blocks / tenant**: the average number of blocks per tenant.
          - **Tenants with largest number of blocks**: the 10 tenants with the largest number of blocks.
        |||),
      )
      .addPanel(
        $.panel('Average blocks / tenant') +
        $.queryPanel('avg(max by(user) (cortex_bucket_blocks_count{%s}))' % $.jobMatcher($._config.job_names.compactor), 'avg'),
      )
      .addPanel(
        $.panel('Tenants with largest number of blocks') +
        $.queryPanel('topk(10, max by(user) (cortex_bucket_blocks_count{%s}))' % $.jobMatcher($._config.job_names.compactor), '{{user}}'),
      )
    )
    .addRow(
      $.row('Garbage Collector')
      .addPanel(
        $.panel('Blocks marked for deletion / sec') +
        $.queryPanel('sum(rate(cortex_compactor_blocks_marked_for_deletion_total{%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.compactor), 'blocks') +
        { yaxes: $.yaxes('ops') },
      )
      .addPanel(
        $.successFailurePanel(
          'Blocks deletions / sec',
          // The cortex_compactor_blocks_cleaned_total tracks the number of successfully
          // deleted blocks.
          'sum(rate(cortex_compactor_blocks_cleaned_total{%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.compactor),
          'sum(rate(cortex_compactor_block_cleanup_failures_total{%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.compactor),
        ) + { yaxes: $.yaxes('ops') }
      )
    )
    .addRow(
      $.row('Metadata Sync')
      .addPanel(
        $.successFailurePanel(
          'Metadata Syncs / sec',
          // The cortex_compactor_meta_syncs_total metric is incremented each time a per-tenant
          // metadata sync is triggered.
          'sum(rate(cortex_compactor_meta_syncs_total{%s}[$__rate_interval])) - sum(rate(cortex_compactor_meta_sync_failures_total{%s}[$__rate_interval]))' % [$.jobMatcher($._config.job_names.compactor), $.jobMatcher($._config.job_names.compactor)],
          'sum(rate(cortex_compactor_meta_sync_failures_total{%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.compactor),
        ) + { yaxes: $.yaxes('ops') }
      )
      .addPanel(
        $.panel('Metadata Sync Duration') +
        // This metric tracks the duration of a per-tenant metadata sync.
        $.latencyPanel('cortex_compactor_meta_sync_duration_seconds', '{%s}' % $.jobMatcher($._config.job_names.compactor)),
      )
    )
    .addRows($.getObjectStoreRows('Object Store', 'compactor')),
}
