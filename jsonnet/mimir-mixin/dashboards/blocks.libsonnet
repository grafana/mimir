local utils = import 'mixin-utils/utils.libsonnet';

(import 'dashboard-utils.libsonnet') {
  'cortex-blocks.json':
    $.dashboard('Cortex / Blocks')
    .addMultiTemplate('cluster', 'kube_pod_container_info{image=~".*cortex.*"}', 'cluster')
    .addMultiTemplate('namespace', 'kube_pod_container_info{image=~".*cortex.*"}', 'namespace')
    // repeated from Cortex / Chunks
    .addRow(
      $.row('Active Series / Chunks')
      .addPanel(
        $.panel('Series') +
        $.queryPanel('sum(cortex_ingester_memory_series{cluster=~"$cluster", job=~"($namespace)/ingester"})', 'series'),
      )
      // Chunks per series doesn't make sense for Blocks storage
    )
    .addRow(
      $.row('Compactor')
      .addPanel(
        $.successFailurePanel(
          'Compactor Runs / second',
          'sum(rate(cortex_compactor_runs_completed_total{cluster=~"$cluster"}[$__interval]))',
          'sum(rate(cortex_compactor_runs_failed_total{cluster=~"$cluster"}[$__interval]))'
        )
      )
      .addPanel(
        $.successFailurePanel(
          'Per-tenant Compaction Runs / seconds',
          'sum(rate(cortex_compactor_group_compactions_total{cluster=~"$cluster", job=~"($namespace)/compactor"}[$__interval])) - sum(rate(cortex_compactor_group_compactions_failures_total{cluster=~"$cluster", job=~"($namespace)/compactor"}[$__interval]))',
          'sum(rate(cortex_compactor_group_compactions_failures_total{cluster=~"$cluster", job=~"($namespace)/compactor"}[$__interval]))',
        )
      )
    )
    .addRow(
      $.row('Compactor – Blocks Garbage Collections')
      .addPanel(
        $.successFailurePanel(
          'Collections Rate',
          'sum(rate(cortex_compactor_garbage_collection_total{cluster=~"$cluster", job=~"($namespace)/compactor"}[$__interval])) - sum(rate(cortex_compactor_garbage_collection_failures_total{cluster=~"$cluster", job=~"($namespace)/compactor"}[$__interval]))',
          'sum(rate(cortex_compactor_garbage_collection_failures_total{cluster=~"$cluster", job=~"($namespace)/compactor"}[$__interval]))',
        )
      )
      .addPanel(
        $.panel('Collections Duration') +
        $.latencyPanel('cortex_compactor_garbage_collection_duration_seconds', '{cluster=~"$cluster", job=~"($namespace)/compactor"}')
      )
      .addPanel(
        $.panel('Collected Blocks Rate') +
        $.queryPanel('sum(rate(cortex_compactor_garbage_collected_blocks_total{cluster=~"$cluster"}[$__interval]))', 'blocks')
      )
    )
    .addRow(
      $.row('Compactor - Meta Syncs')
      .addPanel(
        $.successFailurePanel(
          'Meta Syncs / sec',
          'sum(rate(cortex_compactor_sync_meta_total{cluster=~"$cluster", job=~"($namespace)/compactor"}[$__interval])) - sum(rate(cortex_compactor_sync_meta_failures_total{cluster=~"$cluster", job=~"($namespace)/compactor"}[$__interval]))',
          'sum(rate(cortex_compactor_sync_meta_failures_total{cluster=~"$cluster", job=~"($namespace)/compactor"}[$__interval]))',
        )
      )
      .addPanel(
        $.panel('Meta Sync Durations') +
        $.latencyPanel('cortex_compactor_sync_meta_duration_seconds', '{cluster=~"$cluster"}'),
      )
    )
    .addRow(
      $.row('Prometheus TSDB Compactions')
      .addPanel(
        $.panel('Compactions Rate') +
        $.queryPanel('sum(rate(prometheus_tsdb_compactions_total{cluster=~"$cluster", job=~"($namespace)/compactor"}[$__interval]))', 'rate')
      )
      .addPanel(
        $.panel('Compaction Duration') +
        $.latencyPanel('prometheus_tsdb_compaction_duration_seconds', '{cluster=~"$cluster", job=~"($namespace)/compactor"}')
      )
      .addPanel(
        $.panel('Chunk Size Bytes') +
        $.latencyPanel('prometheus_tsdb_compaction_chunk_size_bytes', '{cluster=~"$cluster", job=~"($namespace)/compactor"}') +
        { yaxes: $.yaxes('bytes') }
      )
      .addPanel(
        $.panel('Chunk Samples') +
        $.latencyPanel('prometheus_tsdb_compaction_chunk_samples', '{cluster=~"$cluster", job=~"($namespace)/compactor"}') +
        { yaxes: $.yaxes('short') }
      )
      .addPanel(
        $.panel('Chunk Range (seconds)') +
        $.latencyPanel('prometheus_tsdb_compaction_chunk_range_seconds', '{cluster=~"$cluster", job=~"($namespace)/compactor"}')
      )
    )
    .addRow($.objectStorePanels1('Object Store Stats', 'cortex_compactor'))
    .addRow($.objectStorePanels2('', 'cortex_compactor')),
}
