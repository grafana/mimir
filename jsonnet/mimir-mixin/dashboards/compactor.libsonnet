local utils = import 'mixin-utils/utils.libsonnet';

(import 'dashboard-utils.libsonnet') {
  'cortex-compactor.json':
    $.dashboard('Cortex / Compactor')
    .addClusterSelectorTemplates()
    .addRow(
      $.row('Compactions')
      .addPanel(
        $.startedCompletedFailedPanel(
          'Per-instance runs / sec',
          'sum(rate(cortex_compactor_runs_started_total{%s}[$__interval]))' % $.jobMatcher('compactor'),
          'sum(rate(cortex_compactor_runs_completed_total{%s}[$__interval]))' % $.jobMatcher('compactor'),
          'sum(rate(cortex_compactor_runs_failed_total{%s}[$__interval]))' % $.jobMatcher('compactor')
        ) +
        $.bars +
        { yaxes: $.yaxes('ops') },
      )
      .addPanel(
        $.successFailurePanel(
          'Per-tenant runs / sec',
          'sum(rate(cortex_compactor_group_compactions_total{%s}[$__interval])) - sum(rate(cortex_compactor_group_compactions_failures_total{%s}[$__interval]))' % [$.jobMatcher('compactor'), $.jobMatcher('compactor')],
          'sum(rate(cortex_compactor_group_compactions_failures_total{%s}[$__interval]))' % $.jobMatcher('compactor'),
        ) +
        $.bars +
        { yaxes: $.yaxes('ops') },
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.panel('Compacted blocks / sec') +
        $.queryPanel('sum(rate(prometheus_tsdb_compactions_total{%s}[$__interval]))' % $.jobMatcher('compactor'), 'blocks') +
        { yaxes: $.yaxes('ops') },
      )
      .addPanel(
        $.panel('Compaction Duration') +
        $.latencyPanel('prometheus_tsdb_compaction_duration_seconds', '{%s}' % $.jobMatcher('compactor'))
      )
    )
    .addRow(
      $.row('Garbage Collector')
      .addPanel(
        $.panel('Blocks marked for deletion / sec') +
        $.queryPanel('sum(rate(cortex_compactor_blocks_marked_for_deletion_total{%s}[$__interval]))' % $.jobMatcher('compactor'), 'blocks') +
        { yaxes: $.yaxes('ops') },
      )
      .addPanel(
        $.successFailurePanel(
          'Blocks deletions / sec',
          // The cortex_compactor_blocks_cleaned_total tracks the number of successfully
          // deleted blocks.
          'sum(rate(cortex_compactor_blocks_cleaned_total{%s}[$__interval]))' % $.jobMatcher('compactor'),
          'sum(rate(cortex_compactor_block_cleanup_failures_total{%s}[$__interval]))' % $.jobMatcher('compactor'),
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
          'sum(rate(cortex_compactor_meta_syncs_total{%s}[$__interval])) - sum(rate(cortex_compactor_meta_sync_failures_total{%s}[$__interval]))' % [$.jobMatcher('compactor'), $.jobMatcher('compactor')],
          'sum(rate(cortex_compactor_meta_sync_failures_total{%s}[$__interval]))' % $.jobMatcher('compactor'),
        ) + { yaxes: $.yaxes('ops') }
      )
      .addPanel(
        $.panel('Metadata Sync Duration') +
        // This metric tracks the duration of a per-tenant metadata sync.
        $.latencyPanel('cortex_compactor_meta_sync_duration_seconds', '{%s}' % $.jobMatcher('compactor')),
      )
    )
    .addRow($.objectStorePanels1('Object Store', 'cortex_compactor'))
    .addRow($.objectStorePanels2('', 'cortex_compactor')),
}
