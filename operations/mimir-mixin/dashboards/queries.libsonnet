local utils = import 'mixin-utils/utils.libsonnet';

(import 'dashboard-utils.libsonnet') {
  'mimir-queries.json':
    ($.dashboard('Queries') + { uid: 'd9931b1054053c8b972d320774bb8f1d' })
    .addClusterSelectorTemplates()
    .addRow(
      $.row('Query-frontend')
      .addPanel(
        $.panel('Queue duration') +
        $.latencyPanel('cortex_query_frontend_queue_duration_seconds', '{%s}' % $.jobMatcher($._config.job_names.query_frontend)),
      )
      .addPanel(
        $.panel('Retries') +
        $.latencyPanel('cortex_query_frontend_retries', '{%s}' % $.jobMatcher($._config.job_names.query_frontend), multiplier=1) +
        { yaxes: $.yaxes('short') },
      )
      .addPanel(
        $.panel('Queue length (per %s)' % $._config.per_instance_label) +
        $.queryPanel(
          'sum by(%s) (cortex_query_frontend_queue_length{%s})' % [$._config.per_instance_label, $.jobMatcher($._config.job_names.query_frontend)],
          '{{%s}}' % $._config.per_instance_label
        ),
      )
      .addPanel(
        $.panel('Queue length (per user)') +
        $.queryPanel(
          'sum by(user) (cortex_query_frontend_queue_length{%s}) > 0' % [$.jobMatcher($._config.job_names.query_frontend)],
          '{{user}}'
        ),
      )
    )
    .addRow(
      $.row('Query-scheduler')
      .addPanel(
        $.panel('Queue duration') +
        $.latencyPanel('cortex_query_scheduler_queue_duration_seconds', '{%s}' % $.jobMatcher($._config.job_names.query_scheduler)),
      )
      .addPanel(
        $.panel('Queue length (per %s)' % $._config.per_instance_label) +
        $.queryPanel(
          'sum by(%s) (cortex_query_scheduler_queue_length{%s})' % [$._config.per_instance_label, $.jobMatcher($._config.job_names.query_scheduler)],
          '{{%s}}' % $._config.per_instance_label
        ),
      )
      .addPanel(
        $.panel('Queue length (per user)') +
        $.queryPanel(
          'sum by(user) (cortex_query_scheduler_queue_length{%s}) > 0' % [$.jobMatcher($._config.job_names.query_scheduler)],
          '{{user}}'
        ),
      )
    )
    .addRow(
      $.row('Query-frontend - query splitting and results cache')
      .addPanel(
        $.panel('Intervals per Query') +
        $.queryPanel('sum(rate(cortex_frontend_split_queries_total{%s}[1m])) / sum(rate(cortex_frontend_query_range_duration_seconds_count{%s, method="split_by_interval"}[1m]))' % [$.jobMatcher($._config.job_names.query_frontend), $.jobMatcher($._config.job_names.query_frontend)], 'splitting rate') +
        $.panelDescription(
          'Intervals per query',
          |||
            The average number of split queries (partitioned by time) executed a single input query.
          |||
        ),
      )
      .addPanel(
        $.panel('Results cache hit %') +
        $.queryPanel(
          |||
            # Query metrics before and after migration to new memcached backend.
            sum (
              rate(cortex_cache_hits{name=~"frontend.+", %(frontend)s}[1m])
              or
              rate(thanos_cache_memcached_hits_total{name="frontend-cache", %(frontend)s}[1m])
            )
            /
            sum (
              rate(cortex_cache_fetched_keys{name=~"frontend.+", %(frontend)s}[1m])
              or
              rate(thanos_cache_memcached_requests_total{name=~"frontend-cache", %(frontend)s}[1m])
            )
          ||| % {
            frontend: $.jobMatcher($._config.job_names.query_frontend),
          },
          'Hit rate',
        ) +
        { yaxes: $.yaxes({ format: 'percentunit', max: 1 }) },
      )
      .addPanel(
        $.panel('Results cache misses') +
        $.queryPanel(
          |||
            # Query metrics before and after migration to new memcached backend.
            sum (
              rate(cortex_cache_fetched_keys{name=~"frontend.+", %(frontend)s}[1m])
              or
              rate(thanos_cache_memcached_requests_total{name="frontend-cache", %(frontend)s}[1m])
            )
            -
            sum (
              rate(cortex_cache_hits{name=~"frontend.+", %(frontend)s}[1m])
              or
              rate(thanos_cache_memcached_hits_total{name=~"frontend-cache", %(frontend)s}[1m])
            )
          ||| % {
            frontend: $.jobMatcher($._config.job_names.query_frontend),
          },
          'Miss rate'
        ),
      )
    )
    .addRow(
      $.row('Query-frontend - query sharding')
      .addPanel(
        $.panel('Sharded queries ratio') +
        $.queryPanel(|||
          sum(rate(cortex_frontend_query_sharding_rewrites_succeeded_total{%s}[$__rate_interval])) /
          sum(rate(cortex_frontend_query_sharding_rewrites_attempted_total{%s}[$__rate_interval]))
        ||| % [$.jobMatcher($._config.job_names.query_frontend), $.jobMatcher($._config.job_names.query_frontend)], 'sharded queries ratio') +
        { yaxes: $.yaxes({ format: 'percentunit', max: 1 }) } +
        $.panelDescription(
          'Sharded queries ratio',
          |||
            The % of queries that have been successfully rewritten and executed in a shardable way.
            This panel only takes into account the type of queries that are supported by query sharding (eg. range queries).
          |||
        ),
      )
      .addPanel(
        $.panel('Number of sharded queries per query') +
        $.latencyPanel('cortex_frontend_sharded_queries_per_query', '{%s}' % $.jobMatcher($._config.job_names.query_frontend), multiplier=1) +
        { yaxes: $.yaxes('short') } +
        $.panelDescription(
          'Number of sharded queries per query',
          |||
            The number of sharded queries that have been executed for a single input query. It only tracks queries that
            have been successfully rewritten in a shardable way.
          |||
        ),
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
    )
    .addRow(
      $.row('Ingester')
      .addPanel(
        $.panel('Series per query') +
        utils.latencyRecordingRulePanel('cortex_ingester_queried_series', $.jobSelector($._config.job_names.ingester), multiplier=1) +
        { yaxes: $.yaxes('short') },
      )
      .addPanel(
        $.panel('Chunks per query') +
        utils.latencyRecordingRulePanel('cortex_ingester_queried_chunks', $.jobSelector($._config.job_names.ingester), multiplier=1) +
        { yaxes: $.yaxes('short') },
      )
      .addPanel(
        $.panel('Samples per query') +
        utils.latencyRecordingRulePanel('cortex_ingester_queried_samples', $.jobSelector($._config.job_names.ingester), multiplier=1) +
        { yaxes: $.yaxes('short') },
      )
      .addPanel(
        $.panel('Exemplars per query') +
        utils.latencyRecordingRulePanel('cortex_ingester_queried_exemplars', $.jobSelector($._config.job_names.ingester), multiplier=1) +
        { yaxes: $.yaxes('short') },
      )
    )
    .addRow(
      $.row('Querier')
      .addPanel(
        $.panel('Number of store-gateways hit per query') +
        $.latencyPanel('cortex_querier_storegateway_instances_hit_per_query', '{%s}' % $.jobMatcher($._config.job_names.querier), multiplier=1) +
        { yaxes: $.yaxes('short') },
      )
      .addPanel(
        $.panel('Refetches of missing blocks per query') +
        $.latencyPanel('cortex_querier_storegateway_refetches_per_query', '{%s}' % $.jobMatcher($._config.job_names.querier), multiplier=1) +
        { yaxes: $.yaxes('short') },
      )
      .addPanel(
        $.panel('Consistency checks failed') +
        $.queryPanel('sum(rate(cortex_querier_blocks_consistency_checks_failed_total{%s}[1m])) / sum(rate(cortex_querier_blocks_consistency_checks_total{%s}[1m]))' % [$.jobMatcher($._config.job_names.querier), $.jobMatcher($._config.job_names.querier)], 'Failure Rate') +
        { yaxes: $.yaxes({ format: 'percentunit', max: 1 }) },
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.panel('Bucket indexes loaded (per querier)') +
        $.queryPanel([
          'max(cortex_bucket_index_loaded{%s})' % $.jobMatcher($._config.job_names.querier),
          'min(cortex_bucket_index_loaded{%s})' % $.jobMatcher($._config.job_names.querier),
          'avg(cortex_bucket_index_loaded{%s})' % $.jobMatcher($._config.job_names.querier),
        ], ['Max', 'Min', 'Average']) +
        { yaxes: $.yaxes('short') },
      )
      .addPanel(
        $.successFailurePanel(
          'Bucket indexes load / sec',
          'sum(rate(cortex_bucket_index_loads_total{%s}[$__rate_interval])) - sum(rate(cortex_bucket_index_load_failures_total{%s}[$__rate_interval]))' % [$.jobMatcher($._config.job_names.querier), $.jobMatcher($._config.job_names.querier)],
          'sum(rate(cortex_bucket_index_load_failures_total{%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.querier),
        )
      )
      .addPanel(
        $.panel('Bucket indexes load latency') +
        $.latencyPanel('cortex_bucket_index_load_duration_seconds', '{%s}' % $.jobMatcher($._config.job_names.querier)),
      )
    )
    .addRow(
      $.row('Store-gateway')
      .addPanel(
        $.panel('Blocks queried / sec') +
        $.queryPanel('sum(rate(cortex_bucket_store_series_blocks_queried_sum{component="store-gateway",%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.store_gateway), 'blocks') +
        { yaxes: $.yaxes('ops') },
      )
      .addPanel(
        $.panel('Data fetched / sec') +
        $.queryPanel('sum by(data_type) (rate(cortex_bucket_store_series_data_fetched_sum{component="store-gateway",%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.store_gateway), '{{data_type}}') +
        $.stack +
        { yaxes: $.yaxes('ops') },
      )
      .addPanel(
        $.panel('Data touched / sec') +
        $.queryPanel('sum by(data_type) (rate(cortex_bucket_store_series_data_touched_sum{component="store-gateway",%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.store_gateway), '{{data_type}}') +
        $.stack +
        { yaxes: $.yaxes('ops') },
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.panel('Series fetch duration (per request)') +
        $.latencyPanel('cortex_bucket_store_series_get_all_duration_seconds', '{component="store-gateway",%s}' % $.jobMatcher($._config.job_names.store_gateway)),
      )
      .addPanel(
        $.panel('Series merge duration (per request)') +
        $.latencyPanel('cortex_bucket_store_series_merge_duration_seconds', '{component="store-gateway",%s}' % $.jobMatcher($._config.job_names.store_gateway)),
      )
      .addPanel(
        $.panel('Series returned (per request)') +
        $.queryPanel('sum(rate(cortex_bucket_store_series_result_series_sum{component="store-gateway",%s}[$__rate_interval])) / sum(rate(cortex_bucket_store_series_result_series_count{component="store-gateway",%s}[$__rate_interval]))' % [$.jobMatcher($._config.job_names.store_gateway), $.jobMatcher($._config.job_names.store_gateway)], 'avg series returned'),
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.panel('Blocks currently loaded') +
        $.queryPanel('cortex_bucket_store_blocks_loaded{component="store-gateway",%s}' % $.jobMatcher($._config.job_names.store_gateway), '{{%s}}' % $._config.per_instance_label)
      )
      .addPanel(
        $.successFailurePanel(
          'Blocks loaded / sec',
          'sum(rate(cortex_bucket_store_block_loads_total{component="store-gateway",%s}[$__rate_interval])) - sum(rate(cortex_bucket_store_block_load_failures_total{component="store-gateway",%s}[$__rate_interval]))' % [$.jobMatcher($._config.job_names.store_gateway), $.jobMatcher($._config.job_names.store_gateway)],
          'sum(rate(cortex_bucket_store_block_load_failures_total{component="store-gateway",%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.store_gateway),
        )
      )
      .addPanel(
        $.successFailurePanel(
          'Blocks dropped / sec',
          'sum(rate(cortex_bucket_store_block_drops_total{component="store-gateway",%s}[$__rate_interval])) - sum(rate(cortex_bucket_store_block_drop_failures_total{component="store-gateway",%s}[$__rate_interval]))' % [$.jobMatcher($._config.job_names.store_gateway), $.jobMatcher($._config.job_names.store_gateway)],
          'sum(rate(cortex_bucket_store_block_drop_failures_total{component="store-gateway",%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.store_gateway),
        )
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.panel('Lazy loaded index-headers') +
        $.queryPanel('cortex_bucket_store_indexheader_lazy_load_total{%s} - cortex_bucket_store_indexheader_lazy_unload_total{%s}' % [$.jobMatcher($._config.job_names.store_gateway), $.jobMatcher($._config.job_names.store_gateway)], '{{%s}}' % $._config.per_instance_label)
      )
      .addPanel(
        $.panel('Index-header lazy load duration') +
        $.latencyPanel('cortex_bucket_store_indexheader_lazy_load_duration_seconds', '{%s}' % $.jobMatcher($._config.job_names.store_gateway)),
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.panel('Series hash cache hit ratio') +
        $.queryPanel(|||
          sum(rate(cortex_bucket_store_series_hash_cache_hits_total{%s}[$__rate_interval]))
          /
          sum(rate(cortex_bucket_store_series_hash_cache_requests_total{%s}[$__rate_interval]))
        ||| % [$.jobMatcher($._config.job_names.store_gateway), $.jobMatcher($._config.job_names.store_gateway)], 'hit ratio') +
        { yaxes: $.yaxes({ format: 'percentunit', max: 1 }) },
      )
      .addPanel(
        $.panel('ExpandedPostings cache hit ratio') +
        $.queryPanel(|||
          sum(rate(thanos_store_index_cache_hits_total{item_type="ExpandedPostings",%s}[$__rate_interval]))
          /
          sum(rate(thanos_store_index_cache_requests_total{item_type="ExpandedPostings",%s}[$__rate_interval]))
        ||| % [$.jobMatcher($._config.job_names.store_gateway), $.jobMatcher($._config.job_names.store_gateway)], 'hit ratio') +
        { yaxes: $.yaxes({ format: 'percentunit', max: 1 }) },
      )
      .addPanel(
        $.panel('Chunks attributes in-memory cache hit ratio') +
        $.queryPanel(|||
          sum(rate(cortex_cache_memory_hits_total{name="chunks-attributes-cache",%s}[$__rate_interval]))
          /
          sum(rate(cortex_cache_memory_requests_total{name="chunks-attributes-cache",%s}[$__rate_interval]))
        ||| % [$.jobMatcher($._config.job_names.store_gateway), $.jobMatcher($._config.job_names.store_gateway)], 'hit ratio') +
        { yaxes: $.yaxes({ format: 'percentunit', max: 1 }) },
      )
    ),
}
