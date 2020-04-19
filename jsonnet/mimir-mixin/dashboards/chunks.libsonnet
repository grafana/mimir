local utils = import 'mixin-utils/utils.libsonnet';

(import 'dashboard-utils.libsonnet') {
  'cortex-chunks.json':
    $.dashboard('Cortex / Chunks')
    .addClusterSelectorTemplates()
    .addRow(
      $.row('Active Series / Chunks')
      .addPanel(
        $.panel('Series') +
        $.queryPanel('sum(cortex_ingester_memory_series{%s})' % $.jobMatcher('ingester'), 'series'),
      )
      .addPanel(
        $.panel('Chunks per series') +
        $.queryPanel('sum(cortex_ingester_memory_chunks{%s}) / sum(cortex_ingester_memory_series{%s})' % [$.jobMatcher('ingester'), $.jobMatcher('ingester')], 'chunks'),
      )
    )
    .addRow(
      $.row('Flush Stats')
      .addPanel(
        $.panel('Utilization') +
        $.latencyPanel('cortex_ingester_chunk_utilization', '{%s}' % $.jobMatcher('ingester'), multiplier='1') +
        { yaxes: $.yaxes('percentunit') },
      )
      .addPanel(
        $.panel('Age') +
        $.latencyPanel('cortex_ingester_chunk_age_seconds', '{%s}' % $.jobMatcher('ingester')),
      ),
    )
    .addRow(
      $.row('Flush Stats')
      .addPanel(
        $.panel('Size') +
        $.latencyPanel('cortex_ingester_chunk_length', '{%s}' % $.jobMatcher('ingester'), multiplier='1') +
        { yaxes: $.yaxes('short') },
      )
      .addPanel(
        $.panel('Entries') +
        $.queryPanel('sum(rate(cortex_chunk_store_index_entries_per_chunk_sum{%s}[5m])) / sum(rate(cortex_chunk_store_index_entries_per_chunk_count{%s}[5m])' % [$.jobMatcher('ingester'), $.jobMatcher('ingester')], 'entries'),
      ),
    )
    .addRow(
      $.row('Flush Stats')
      .addPanel(
        $.panel('Queue Length') +
        $.queryPanel('cortex_ingester_flush_queue_length{%s}' % $.jobMatcher('ingester'), '{{instance}}'),
      )
      .addPanel(
        $.panel('Flush Rate') +
        $.qpsPanel('cortex_ingester_chunk_age_seconds_count{%s}' % $.jobMatcher('ingester')),
      ),
    ),
}
