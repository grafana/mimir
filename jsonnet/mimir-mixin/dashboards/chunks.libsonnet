local utils = import 'mixin-utils/utils.libsonnet';

(import 'dashboard-utils.libsonnet') {
  'cortex-chunks.json':
    $.dashboard('Cortex / Chunks')
    .addClusterSelectorTemplates()
    .addRow(
      $.row('Active Series / Chunks')
      .addPanel(
        $.panel('Series') +
        $.queryPanel('sum(cortex_ingester_memory_series{%s})' % $.jobMatcher($._config.job_names.ingester), 'series'),
      )
      .addPanel(
        $.panel('Chunks per series') +
        $.queryPanel('sum(cortex_ingester_memory_chunks{%s}) / sum(cortex_ingester_memory_series{%s})' % [$.jobMatcher($._config.job_names.ingester), $.jobMatcher($._config.job_names.ingester)], 'chunks'),
      )
    )
    .addRow(
      $.row('Flush Stats')
      .addPanel(
        $.panel('Utilization') +
        $.latencyPanel('cortex_ingester_chunk_utilization', '{%s}' % $.jobMatcher($._config.job_names.ingester), multiplier='1') +
        { yaxes: $.yaxes('percentunit') },
      )
      .addPanel(
        $.panel('Age') +
        $.latencyPanel('cortex_ingester_chunk_age_seconds', '{%s}' % $.jobMatcher($._config.job_names.ingester)),
      ),
    )
    .addRow(
      $.row('Flush Stats')
      .addPanel(
        $.panel('Size') +
        $.latencyPanel('cortex_ingester_chunk_length', '{%s}' % $.jobMatcher($._config.job_names.ingester), multiplier='1') +
        { yaxes: $.yaxes('short') },
      )
      .addPanel(
        $.panel('Entries') +
        $.queryPanel('sum(rate(cortex_chunk_store_index_entries_per_chunk_sum{%s}[5m])) / sum(rate(cortex_chunk_store_index_entries_per_chunk_count{%s}[5m]))' % [$.jobMatcher($._config.job_names.ingester), $.jobMatcher($._config.job_names.ingester)], 'entries'),
      ),
    )
    .addRow(
      $.row('Flush Stats')
      .addPanel(
        $.panel('Queue Length') +
        $.queryPanel('cortex_ingester_flush_queue_length{%s}' % $.jobMatcher($._config.job_names.ingester), '{{instance}}'),
      )
      .addPanel(
        $.panel('Flush Rate') +
        $.qpsPanel('cortex_ingester_chunk_age_seconds_count{%s}' % $.jobMatcher($._config.job_names.ingester)),
      ),
    ),

  'cortex-wal.json':
    $.dashboard('Cortex / WAL')
    .addClusterSelectorTemplates()
    .addRow(
      $.row('')
      .addPanel(
        $.panel('Bytes Logged (WAL+Checkpoint) / ingester / second') +
        $.queryPanel('avg(rate(cortex_ingester_wal_logged_bytes_total{%(m)s}[$__interval])) + avg(rate(cortex_ingester_checkpoint_logged_bytes_total{%(m)s}[$__interval]))' % { m: $.jobMatcher($._config.job_names.ingester) }, 'bytes') +
        { yaxes: $.yaxes('bytes') },
      )
    )
    .addRow(
      $.row('WAL')
      .addPanel(
        $.panel('Records logged / ingester / second') +
        $.queryPanel('avg(rate(cortex_ingester_wal_records_logged_total{%s}[$__interval]))' % $.jobMatcher($._config.job_names.ingester), 'records'),
      )
      .addPanel(
        $.panel('Bytes per record') +
        $.queryPanel('avg(rate(cortex_ingester_wal_logged_bytes_total{%(m)s}[$__interval]) / rate(cortex_ingester_wal_records_logged_total{%(m)s}[$__interval]))' % { m: $.jobMatcher($._config.job_names.ingester) }, 'bytes') +
        { yaxes: $.yaxes('bytes') },
      )
      .addPanel(
        $.panel('Bytes per sample') +
        $.queryPanel('avg(rate(cortex_ingester_wal_logged_bytes_total{%(m)s}[$__interval]) / rate(cortex_ingester_ingested_samples_total{%(m)s}[$__interval]))' % { m: $.jobMatcher($._config.job_names.ingester) }, 'bytes') +
        { yaxes: $.yaxes('bytes') },
      )
      .addPanel(
        $.panel('Min(available disk space)') +
        $.queryPanel('min(kubelet_volume_stats_available_bytes{cluster=~"$cluster", namespace=~"$namespace", persistentvolumeclaim=~"ingester.*"})', 'bytes') +
        { yaxes: $.yaxes('bytes') },
      )
    )
    .addRow(
      $.row('Checkpoint')
      .addPanel(
        $.panel('Checkpoint creation/deletion / sec') +
        $.queryPanel('rate(cortex_ingester_checkpoint_creations_total{%s}[$__interval])' % $.jobMatcher($._config.job_names.ingester), '{{instance}}-creation') +
        $.queryPanel('rate(cortex_ingester_checkpoint_deletions_total{%s}[$__interval])' % $.jobMatcher($._config.job_names.ingester), '{{instance}}-deletion'),
      )
      .addPanel(
        $.panel('Checkpoint creation/deletion failed / sec') +
        $.queryPanel('rate(cortex_ingester_checkpoint_creations_failed_total{%s}[$__interval])' % $.jobMatcher($._config.job_names.ingester), '{{instance}}-creation') +
        $.queryPanel('rate(cortex_ingester_checkpoint_deletions_failed_total{%s}[$__interval])' % $.jobMatcher($._config.job_names.ingester), '{{instance}}-deletion'),
      )
    ),
}
