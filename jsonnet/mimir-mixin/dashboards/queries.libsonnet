local utils = import 'mixin-utils/utils.libsonnet';

(import 'dashboard-utils.libsonnet') {

  'cortex-queries.json':
    $.dashboard('Cortex / Queries')
    .addClusterSelectorTemplates()
    .addRow(
      $.row('Query Frontend')
      .addPanel(
        $.panel('Queue Duration') +
        $.latencyPanel('cortex_query_frontend_queue_duration_seconds', '{%s}' % $.jobMatcher('query-frontend')),
      )
      .addPanel(
        $.panel('Retries') +
        $.latencyPanel('cortex_query_frontend_retries', '{%s}' % $.jobMatcher('query-frontend'), multiplier=1) +
        { yaxes: $.yaxes('short') },
      )
      .addPanel(
        $.panel('Queue Length') +
        $.queryPanel('cortex_query_frontend_queue_length{%s}' % $.jobMatcher('query-frontend'), '{{cluster}} / {{namespace}} / {{instance}}'),
      )
    )
    .addRow(
      $.row('Query Frontend - Results Cache')
      .addPanel(
        $.panel('Cache Hit %') +
        $.queryPanel('sum(rate(cortex_cache_hits{%s}[1m])) / sum(rate(cortex_cache_fetched_keys{%s}[1m]))' % [$.jobMatcher('query-frontend'), $.jobMatcher('query-frontend')], 'Hit Rate') +
        { yaxes: $.yaxes({ format: 'percentunit', max: 1 }) },
      )
      .addPanel(
        $.panel('Cache misses') +
        $.queryPanel('sum(rate(cortex_cache_fetched_keys{%s}[1m])) - sum(rate(cortex_cache_hits{%s}[1m]))' % [$.jobMatcher('query-frontend'), $.jobMatcher('query-frontend')], 'Miss Rate'),
      )
    )
    .addRow(
      $.row('Query Frontend - Sharding/Splitting')
      .addPanel(
        $.panel('Intervals per Query') +
        $.queryPanel('sum(rate(cortex_frontend_split_queries_total{%s}[1m])) / sum(rate(cortex_frontend_query_range_duration_seconds_count{%s, method="split_by_interval"}[1m]))' % [$.jobMatcher('query-frontend'), $.jobMatcher('query-frontend')], 'partition rate'),
      )
      .addPanel(
        $.panel('Sharded Queries %') +
        $.queryPanel('sum(rate(cortex_frontend_mapped_asts_total{%s}[1m])) / sum(rate(cortex_frontend_split_queries_total{%s}[1m])) * 100' % [$.jobMatcher('query-frontend'), $.jobMatcher('query-frontend')], 'shard rate'),
      )
      .addPanel(
        $.panel('Sharding factor') +
        $.queryPanel('sum(rate(cortex_frontend_sharded_queries_total{%s}[1m])) / sum(rate(cortex_frontend_mapped_asts_total{%s}[1m]))' % [$.jobMatcher('query-frontend'), $.jobMatcher('query-frontend')], 'Average'),
      )
    )
    .addRow(
      $.row('Querier')
      .addPanel(
        $.panel('Stages') +
        $.queryPanel('max by (slice) (prometheus_engine_query_duration_seconds{quantile="0.9",%s}) * 1e3' % $.jobMatcher('querier'), '{{slice}}') +
        { yaxes: $.yaxes('ms') } +
        $.stack,
      )
      .addPanel(
        $.panel('Chunk cache misses') +
        $.queryPanel('sum(rate(cortex_cache_fetched_keys{%s,name="chunksmemcache"}[1m])) - sum(rate(cortex_cache_hits{%s,name="chunksmemcache"}[1m]))' % [$.jobMatcher('querier'), $.jobMatcher('querier')], 'Hit rate'),
      )
      .addPanel(
        $.panel('Chunk cache corruptions') +
        $.queryPanel('sum(rate(cortex_cache_corrupt_chunks_total{%s}[1m]))' % $.jobMatcher('querier'), 'Corrupt chunks'),
      )
    )
    .addRow(
      $.row('Querier - Index Cache')
      .addPanel(
        $.panel('Total entries') +
        $.queryPanel('sum(querier_cache_added_new_total{cache="store.index-cache-read.fifocache",%s}) - sum(querier_cache_evicted_total{cache="store.index-cache-read.fifocache",%s})' % [$.jobMatcher('querier'), $.jobMatcher('querier')], 'Entries'),
      )
      .addPanel(
        $.panel('Cache Hit %') +
        $.queryPanel('(sum(rate(querier_cache_gets_total{cache="store.index-cache-read.fifocache",%s}[1m])) - sum(rate(querier_cache_misses_total{cache="store.index-cache-read.fifocache",%s}[1m]))) / sum(rate(querier_cache_gets_total{cache="store.index-cache-read.fifocache",%s}[1m]))' % [$.jobMatcher('querier'), $.jobMatcher('querier'), $.jobMatcher('querier')], 'hit rate')
        { yaxes: $.yaxes({ format: 'percentunit', max: 1 }) },
      )
      .addPanel(
        $.panel('Churn Rate') +
        $.queryPanel('sum(rate(querier_cache_evicted_total{cache="store.index-cache-read.fifocache",%s}[1m]))' % $.jobMatcher('querier'), 'churn rate'),
      )
    )
    .addRow(
      $.row('Ingester')
      .addPanel(
        $.panel('Series per Query') +
        utils.latencyRecordingRulePanel('cortex_ingester_queried_series', $.jobSelector('ingester'), multiplier=1) +
        { yaxes: $.yaxes('short') },
      )
      .addPanel(
        $.panel('Chunks per Query') +
        utils.latencyRecordingRulePanel('cortex_ingester_queried_chunks', $.jobSelector('ingester'), multiplier=1) +
        { yaxes: $.yaxes('short') },
      )
      .addPanel(
        $.panel('Samples per Query') +
        utils.latencyRecordingRulePanel('cortex_ingester_queried_samples', $.jobSelector('ingester'), multiplier=1) +
        { yaxes: $.yaxes('short') },
      )
    )
    .addRow(
      $.row('Chunk Store')
      .addPanel(
        $.panel('Index Lookups per Query') +
        utils.latencyRecordingRulePanel('cortex_chunk_store_index_lookups_per_query', $.jobSelector('querier'), multiplier=1) +
        { yaxes: $.yaxes('short') },
      )
      .addPanel(
        $.panel('Series (pre-intersection) per Query') +
        utils.latencyRecordingRulePanel('cortex_chunk_store_series_pre_intersection_per_query', $.jobSelector('querier'), multiplier=1) +
        { yaxes: $.yaxes('short') },
      )
      .addPanel(
        $.panel('Series (post-intersection) per Query') +
        utils.latencyRecordingRulePanel('cortex_chunk_store_series_post_intersection_per_query', $.jobSelector('querier'), multiplier=1) +
        { yaxes: $.yaxes('short') },
      )
      .addPanel(
        $.panel('Chunks per Query') +
        utils.latencyRecordingRulePanel('cortex_chunk_store_chunks_per_query', $.jobSelector('querier'), multiplier=1) +
        { yaxes: $.yaxes('short') },
      )
    ),
}
