local utils = import 'mixin-utils/utils.libsonnet';

(import 'dashboard-utils.libsonnet') {

  'cortex-queries.json':
    $.dashboard('Cortex / Queries')
    .addClusterSelectorTemplates()
    .addRow(
      $.row('Query Frontend')
      .addPanel(
        $.panel('Queue Duration') +
        $.latencyPanel('cortex_query_frontend_queue_duration_seconds', '{%s}' % $.jobMatcher($._config.job_names.query_frontend)),
      )
      .addPanel(
        $.panel('Retries') +
        $.latencyPanel('cortex_query_frontend_retries', '{%s}' % $.jobMatcher($._config.job_names.query_frontend), multiplier=1) +
        { yaxes: $.yaxes('short') },
      )
      .addPanel(
        $.panel('Queue Length') +
        $.queryPanel('cortex_query_frontend_queue_length{%s}' % $.jobMatcher($._config.job_names.query_frontend), '{{cluster}} / {{namespace}} / {{instance}}'),
      )
    )
    .addRow(
      $.row('Query Frontend - Results Cache')
      .addPanel(
        $.panel('Cache Hit %') +
        $.queryPanel('sum(rate(cortex_cache_hits{%s}[1m])) / sum(rate(cortex_cache_fetched_keys{%s}[1m]))' % [$.jobMatcher($._config.job_names.query_frontend), $.jobMatcher($._config.job_names.query_frontend)], 'Hit Rate') +
        { yaxes: $.yaxes({ format: 'percentunit', max: 1 }) },
      )
      .addPanel(
        $.panel('Cache misses') +
        $.queryPanel('sum(rate(cortex_cache_fetched_keys{%s}[1m])) - sum(rate(cortex_cache_hits{%s}[1m]))' % [$.jobMatcher($._config.job_names.query_frontend), $.jobMatcher($._config.job_names.query_frontend)], 'Miss Rate'),
      )
    )
    .addRow(
      $.row('Query Frontend - Sharding/Splitting')
      .addPanel(
        $.panel('Intervals per Query') +
        $.queryPanel('sum(rate(cortex_frontend_split_queries_total{%s}[1m])) / sum(rate(cortex_frontend_query_range_duration_seconds_count{%s, method="split_by_interval"}[1m]))' % [$.jobMatcher($._config.job_names.query_frontend), $.jobMatcher($._config.job_names.query_frontend)], 'partition rate'),
      )
      .addPanel(
        $.panel('Sharded Queries %') +
        $.queryPanel('sum(rate(cortex_frontend_mapped_asts_total{%s}[1m])) / sum(rate(cortex_frontend_split_queries_total{%s}[1m])) * 100' % [$.jobMatcher($._config.job_names.query_frontend), $.jobMatcher($._config.job_names.query_frontend)], 'shard rate'),
      )
      .addPanel(
        $.panel('Sharding factor') +
        $.queryPanel('sum(rate(cortex_frontend_sharded_queries_total{%s}[1m])) / sum(rate(cortex_frontend_mapped_asts_total{%s}[1m]))' % [$.jobMatcher($._config.job_names.query_frontend), $.jobMatcher($._config.job_names.query_frontend)], 'Average'),
      )
    )
    .addRow(
      $.row('Querier')
      .addPanel(
        $.panel('Stages') +
        $.queryPanel('max by (slice) (prometheus_engine_query_duration_seconds{quantile="0.9",%s}) * 1e3' % $.jobMatcher($._config.job_names.querier), '{{slice}}') +
        { yaxes: $.yaxes('ms') } +
        $.stack,
      )
      .addPanel(
        $.panel('Chunk cache misses') +
        $.queryPanel('sum(rate(cortex_cache_fetched_keys{%s,name="chunksmemcache"}[1m])) - sum(rate(cortex_cache_hits{%s,name="chunksmemcache"}[1m]))' % [$.jobMatcher($._config.job_names.querier), $.jobMatcher($._config.job_names.querier)], 'Hit rate'),
      )
      .addPanel(
        $.panel('Chunk cache corruptions') +
        $.queryPanel('sum(rate(cortex_cache_corrupt_chunks_total{%s}[1m]))' % $.jobMatcher($._config.job_names.querier), 'Corrupt chunks'),
      )
    )
    .addRowIf(
      std.setMember('chunks', $._config.storage_engine),
      $.row('Querier - Chunks storage - Index Cache')
      .addPanel(
        $.panel('Total entries') +
        $.queryPanel('sum(querier_cache_added_new_total{cache="store.index-cache-read.fifocache",%s}) - sum(querier_cache_evicted_total{cache="store.index-cache-read.fifocache",%s})' % [$.jobMatcher($._config.job_names.querier), $.jobMatcher($._config.job_names.querier)], 'Entries'),
      )
      .addPanel(
        $.panel('Cache Hit %') +
        $.queryPanel('(sum(rate(querier_cache_gets_total{cache="store.index-cache-read.fifocache",%s}[1m])) - sum(rate(querier_cache_misses_total{cache="store.index-cache-read.fifocache",%s}[1m]))) / sum(rate(querier_cache_gets_total{cache="store.index-cache-read.fifocache",%s}[1m]))' % [$.jobMatcher($._config.job_names.querier), $.jobMatcher($._config.job_names.querier), $.jobMatcher($._config.job_names.querier)], 'hit rate')
        { yaxes: $.yaxes({ format: 'percentunit', max: 1 }) },
      )
      .addPanel(
        $.panel('Churn Rate') +
        $.queryPanel('sum(rate(querier_cache_evicted_total{cache="store.index-cache-read.fifocache",%s}[1m]))' % $.jobMatcher($._config.job_names.querier), 'churn rate'),
      )
    )
    .addRow(
      $.row('Ingester')
      .addPanel(
        $.panel('Series per Query') +
        utils.latencyRecordingRulePanel('cortex_ingester_queried_series', $.jobSelector($._config.job_names.ingester), multiplier=1) +
        { yaxes: $.yaxes('short') },
      )
      .addPanel(
        $.panel('Chunks per Query') +
        utils.latencyRecordingRulePanel('cortex_ingester_queried_chunks', $.jobSelector($._config.job_names.ingester), multiplier=1) +
        { yaxes: $.yaxes('short') },
      )
      .addPanel(
        $.panel('Samples per Query') +
        utils.latencyRecordingRulePanel('cortex_ingester_queried_samples', $.jobSelector($._config.job_names.ingester), multiplier=1) +
        { yaxes: $.yaxes('short') },
      )
    )
    .addRowIf(
      std.setMember('chunks', $._config.storage_engine),
      $.row('Querier - Chunks storage - Store')
      .addPanel(
        $.panel('Index Lookups per Query') +
        utils.latencyRecordingRulePanel('cortex_chunk_store_index_lookups_per_query', $.jobSelector($._config.job_names.querier), multiplier=1) +
        { yaxes: $.yaxes('short') },
      )
      .addPanel(
        $.panel('Series (pre-intersection) per Query') +
        utils.latencyRecordingRulePanel('cortex_chunk_store_series_pre_intersection_per_query', $.jobSelector($._config.job_names.querier), multiplier=1) +
        { yaxes: $.yaxes('short') },
      )
      .addPanel(
        $.panel('Series (post-intersection) per Query') +
        utils.latencyRecordingRulePanel('cortex_chunk_store_series_post_intersection_per_query', $.jobSelector($._config.job_names.querier), multiplier=1) +
        { yaxes: $.yaxes('short') },
      )
      .addPanel(
        $.panel('Chunks per Query') +
        utils.latencyRecordingRulePanel('cortex_chunk_store_chunks_per_query', $.jobSelector($._config.job_names.querier), multiplier=1) +
        { yaxes: $.yaxes('short') },
      )
    )
    .addRowIf(
      std.setMember('tsdb', $._config.storage_engine),
      $.row('Store-gateway - Blocks')
      .addPanel(
        $.panel('Blocks queried / sec') +
        $.queryPanel('sum(rate(cortex_storegateway_bucket_store_series_blocks_queried_sum{%s}[$__interval]))' % $.jobMatcher($._config.job_names.store_gateway), 'blocks') +
        { yaxes: $.yaxes('ops') },
      )
      .addPanel(
        $.panel('Data fetched / sec') +
        $.queryPanel('sum by(data_type) (rate(cortex_storegateway_bucket_store_series_data_fetched_sum{%s}[$__interval]))' % $.jobMatcher($._config.job_names.store_gateway), '{{data_type}}') +
        $.stack +
        { yaxes: $.yaxes('ops') },
      )
      .addPanel(
        $.panel('Data touched / sec') +
        $.queryPanel('sum by(data_type) (rate(cortex_storegateway_bucket_store_series_data_touched_sum{%s}[$__interval]))' % $.jobMatcher($._config.job_names.store_gateway), '{{data_type}}') +
        $.stack +
        { yaxes: $.yaxes('ops') },
      )
    )
    .addRowIf(
      std.setMember('tsdb', $._config.storage_engine),
      $.row('')
      .addPanel(
        $.panel('Series fetch duration (per request)') +
        $.latencyPanel('cortex_storegateway_bucket_store_series_get_all_duration_seconds', '{%s}' % $.jobMatcher($._config.job_names.store_gateway)),
      )
      .addPanel(
        $.panel('Series merge duration (per request)') +
        $.latencyPanel('cortex_storegateway_bucket_store_series_merge_duration_seconds', '{%s}' % $.jobMatcher($._config.job_names.store_gateway)),
      )
      .addPanel(
        $.panel('Series returned (per request)') +
        $.queryPanel('sum(rate(cortex_storegateway_bucket_store_series_result_series_sum{%s}[$__interval])) / sum(rate(cortex_storegateway_bucket_store_series_result_series_count{%s}[$__interval]))' % [$.jobMatcher($._config.job_names.store_gateway), $.jobMatcher($._config.job_names.store_gateway)], 'avg series returned'),
      )
    )
    .addRowIf(
      std.setMember('tsdb', $._config.storage_engine),
      $.row('')
      .addPanel(
        $.panel('Blocks currently loaded') +
        $.queryPanel('cortex_storegateway_bucket_store_blocks_loaded{%s}' % $.jobMatcher($._config.job_names.store_gateway), '{{instance}}')
      )
      .addPanel(
        $.successFailurePanel(
          'Blocks loaded / sec',
          'sum(rate(cortex_storegateway_bucket_store_block_loads_total{%s}[$__interval])) - sum(rate(cortex_storegateway_bucket_store_block_load_failures_total{%s}[$__interval]))' % [$.jobMatcher($._config.job_names.store_gateway), $.jobMatcher($._config.job_names.store_gateway)],
          'sum(rate(cortex_storegateway_bucket_store_block_load_failures_total{%s}[$__interval]))' % $.jobMatcher($._config.job_names.store_gateway),
        )
      )
      .addPanel(
        $.successFailurePanel(
          'Blocks dropped / sec',
          'sum(rate(cortex_storegateway_bucket_store_block_drops_total{%s}[$__interval])) - sum(rate(cortex_storegateway_bucket_store_block_drop_failures_total{%s}[$__interval]))' % [$.jobMatcher($._config.job_names.store_gateway), $.jobMatcher($._config.job_names.store_gateway)],
          'sum(rate(cortex_storegateway_bucket_store_block_drop_failures_total{%s}[$__interval]))' % $.jobMatcher($._config.job_names.store_gateway),
        )
      )
    ),
}
