local utils = import 'mixin-utils/utils.libsonnet';

(import 'dashboard-utils.libsonnet') {

  'cortex-queries.json':
    $.dashboard('Cortex / Queries')
    .addClusterSelectorTemplates()
    .addRow(
      $.row('Query Frontend')
      .addPanel(
        $.panel('Queue Duration') +
        $.latencyPanel('cortex_query_frontend_queue_duration_seconds', '{cluster=~"$cluster", job=~"($namespace)/query-frontend"}'),
      )
      .addPanel(
        $.panel('Retries') +
        $.latencyPanel('cortex_query_frontend_retries', '{cluster=~"$cluster", job=~"($namespace)/query-frontend"}', multiplier=1) +
        { yaxes: $.yaxes('short') },
      )
      .addPanel(
        $.panel('Queue Length') +
        $.queryPanel('cortex_query_frontend_queue_length{cluster=~"$cluster", job=~"($namespace)/query-frontend"}', '{{cluster}} / {{namespace}} / {{instance}}'),
      )
    )
    .addRow(
      $.row('Query Frontend - Results Cache')
      .addPanel(
        $.panel('Cache Hit %') +
        $.queryPanel('sum(rate(cortex_cache_hits{cluster=~"$cluster",job=~"($namespace)/query-frontend"}[1m])) / sum(rate(cortex_cache_fetched_keys{cluster=~"$cluster",job=~"($namespace)/query-frontend"}[1m]))', 'Hit Rate') +
        { yaxes: $.yaxes({ format: 'percentunit', max: 1 }) },
      )
      .addPanel(
        $.panel('Cache misses') +
        $.queryPanel('sum(rate(cortex_cache_fetched_keys{cluster=~"$cluster",job=~"($namespace)/query-frontend"}[1m])) - sum(rate(cortex_cache_hits{cluster=~"$cluster",job=~"($namespace)/query-frontend"}[1m]))', 'Miss Rate'),
      )
    )
    .addRow(
      $.row('Query Frontend - Sharding/Splitting')
      .addPanel(
        $.panel('Intervals per Query') +
        $.queryPanel('sum(rate(cortex_frontend_split_queries_total{cluster="$cluster", namespace="$namespace"}[1m])) / sum(rate(cortex_frontend_query_range_duration_seconds_count{cluster="$cluster", namespace="$namespace", method="split_by_interval"}[1m]))', 'partition rate'),
      )
      .addPanel(
        $.panel('Sharded Queries %') +
        $.queryPanel('sum(rate(cortex_frontend_mapped_asts_total{cluster="$cluster", namespace="$namespace"}[1m])) / sum(rate(cortex_frontend_split_queries_total{cluster="$cluster", namespace="$namespace"}[1m])) * 100', 'shard rate'),
      )
      .addPanel(
        $.panel('Sharding factor') +
        $.queryPanel('sum(rate(cortex_frontend_sharded_queries_total{cluster="$cluster", namespace="$namespace"}[1m])) / sum(rate(cortex_frontend_mapped_asts_total{cluster="$cluster", namespace="$namespace"}[1m]))', 'Average'),
      )
    )
    .addRow(
      $.row('Querier')
      .addPanel(
        $.panel('Stages') +
        $.queryPanel('max by (slice) (prometheus_engine_query_duration_seconds{quantile="0.9",cluster=~"$cluster",job=~"($namespace)/querier"}) * 1e3', '{{slice}}') +
        { yaxes: $.yaxes('ms') } +
        $.stack,
      )
      .addPanel(
        $.panel('Chunk cache misses') +
        $.queryPanel('sum(rate(cortex_cache_fetched_keys{cluster=~"$cluster",job=~"($namespace)/querier",name="chunksmemcache"}[1m])) - sum(rate(cortex_cache_hits{cluster=~"$cluster",job=~"($namespace)/querier",name="chunksmemcache"}[1m]))', 'Hit rate'),
      )
      .addPanel(
        $.panel('Chunk cache corruptions') +
        $.queryPanel('sum(rate(cortex_cache_corrupt_chunks_total{cluster=~"$cluster",job=~"($namespace)/querier"}[1m]))', 'Corrupt chunks'),
      )
    )
    .addRow(
      $.row('Querier - Index Cache')
      .addPanel(
        $.panel('Total entries') +
        $.queryPanel('sum(querier_cache_added_new_total{cache="store.index-cache-read.fifocache", cluster=~"$cluster",job=~"($namespace)/querier"}) - sum(querier_cache_evicted_total{cache="store.index-cache-read.fifocache", cluster=~"$cluster",job=~"($namespace)/querier"})', 'Entries'),
      )
      .addPanel(
        $.panel('Cache Hit %') +
        $.queryPanel('(sum(rate(querier_cache_gets_total{cache="store.index-cache-read.fifocache", cluster=~"$cluster",job=~"($namespace)/querier"}[1m])) - sum(rate(querier_cache_misses_total{cache="store.index-cache-read.fifocache", cluster=~"$cluster",job=~"($namespace)/querier"}[1m]))) / sum(rate(querier_cache_gets_total{cache="store.index-cache-read.fifocache", cluster=~"$cluster",job=~"($namespace)/querier"}[1m]))', 'hit rate')
        { yaxes: $.yaxes({ format: 'percentunit', max: 1 }) },
      )
      .addPanel(
        $.panel('Churn Rate') +
        $.queryPanel('sum(rate(querier_cache_evicted_total{cache="store.index-cache-read.fifocache", cluster=~"$cluster",job=~"($namespace)/querier"}[1m]))', 'churn rate'),
      )
    )
    .addRow(
      $.row('Ingester')
      .addPanel(
        $.panel('Series per Query') +
        utils.latencyRecordingRulePanel('cortex_ingester_queried_series', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/ingester')], multiplier=1) +
        { yaxes: $.yaxes('short') },
      )
      .addPanel(
        $.panel('Chunks per Query') +
        utils.latencyRecordingRulePanel('cortex_ingester_queried_chunks', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/ingester')], multiplier=1) +
        { yaxes: $.yaxes('short') },
      )
      .addPanel(
        $.panel('Samples per Query') +
        utils.latencyRecordingRulePanel('cortex_ingester_queried_samples', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/ingester')], multiplier=1) +
        { yaxes: $.yaxes('short') },
      )
    )
    .addRow(
      $.row('Chunk Store')
      .addPanel(
        $.panel('Index Lookups per Query') +
        utils.latencyRecordingRulePanel('cortex_chunk_store_index_lookups_per_query', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/querier')], multiplier=1) +
        { yaxes: $.yaxes('short') },
      )
      .addPanel(
        $.panel('Series (pre-intersection) per Query') +
        utils.latencyRecordingRulePanel('cortex_chunk_store_series_pre_intersection_per_query', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/querier')], multiplier=1) +
        { yaxes: $.yaxes('short') },
      )
      .addPanel(
        $.panel('Series (post-intersection) per Query') +
        utils.latencyRecordingRulePanel('cortex_chunk_store_series_post_intersection_per_query', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/querier')], multiplier=1) +
        { yaxes: $.yaxes('short') },
      )
      .addPanel(
        $.panel('Chunks per Query') +
        utils.latencyRecordingRulePanel('cortex_chunk_store_chunks_per_query', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/querier')], multiplier=1) +
        { yaxes: $.yaxes('short') },
      )
    ),
}
