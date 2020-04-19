local utils = import 'mixin-utils/utils.libsonnet';

(import 'dashboard-utils.libsonnet') {
  'cortex-chunks.json':
    $.dashboard('Cortex / Chunks')
    .addMultiTemplate('cluster', 'kube_pod_container_info{image=~".*cortex.*"}', 'cluster')
    .addMultiTemplate('namespace', 'kube_pod_container_info{image=~".*cortex.*"}', 'namespace')
    .addRow(
      $.row('Active Series / Chunks')
      .addPanel(
        $.panel('Series') +
        $.queryPanel('sum(cortex_ingester_memory_series{cluster=~"$cluster", job=~"($namespace)/ingester"})', 'series'),
      )
      .addPanel(
        $.panel('Chunks per series') +
        $.queryPanel('sum(cortex_ingester_memory_chunks{cluster=~"$cluster", job=~"($namespace)/ingester"}) / sum(cortex_ingester_memory_series{cluster=~"$cluster", job=~"($namespace)/ingester"})', 'chunks'),
      )
    )
    .addRow(
      $.row('Flush Stats')
      .addPanel(
        $.panel('Utilization') +
        $.latencyPanel('cortex_ingester_chunk_utilization', '{cluster=~"$cluster", job=~"($namespace)/ingester"}', multiplier='1') +
        { yaxes: $.yaxes('percentunit') },
      )
      .addPanel(
        $.panel('Age') +
        $.latencyPanel('cortex_ingester_chunk_age_seconds', '{cluster=~"$cluster", job=~"($namespace)/ingester"}'),
      ),
    )
    .addRow(
      $.row('Flush Stats')
      .addPanel(
        $.panel('Size') +
        $.latencyPanel('cortex_ingester_chunk_length', '{cluster=~"$cluster", job=~"($namespace)/ingester"}', multiplier='1') +
        { yaxes: $.yaxes('short') },
      )
      .addPanel(
        $.panel('Entries') +
        $.queryPanel('sum(rate(cortex_chunk_store_index_entries_per_chunk_sum{cluster=~"$cluster", job=~"($namespace)/ingester"}[5m])) / sum(rate(cortex_chunk_store_index_entries_per_chunk_count{cluster=~"$cluster", job=~"($namespace)/ingester"}[5m]))', 'entries'),
      ),
    )
    .addRow(
      $.row('Flush Stats')
      .addPanel(
        $.panel('Queue Length') +
        $.queryPanel('cortex_ingester_flush_queue_length{cluster=~"$cluster", job=~"($namespace)/ingester"}', '{{instance}}'),
      )
      .addPanel(
        $.panel('Flush Rate') +
        $.qpsPanel('cortex_ingester_chunk_age_seconds_count{cluster=~"$cluster", job=~"($namespace)/ingester"}'),
      ),
    ),
}
