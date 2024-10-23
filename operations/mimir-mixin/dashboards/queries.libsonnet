local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-queries.json';

(import 'dashboard-utils.libsonnet') {
  [filename]:
    assert std.md5(filename) == 'b3abe8d5c040395cc36615cb4334c92d' : 'UID of the dashboard has changed, please update references to dashboard.';
    ($.dashboard('Queries') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates()
    .addShowNativeLatencyVariable()
    // This selector allows to switch the read path components queried in this dashboard.
    // Since the labels matcher used by this selector is very wide (includes multiple components)
    // whenever you want to show panels for a specific component (e.g. query-frontend) you can safely
    // use this selector only on metrics that are exposed by the specific component itself
    // (e.g. cortex_query_frontend_* metrics are only exposed by query-frontend) or on metrics
    // that have other labels that allow to distinguish the component (e.g. component="query-frontend").
    .addCustomTemplate('Read path', 'read_path_matcher', [
      { label: 'All', value: $.jobMatcher(std.uniq(std.sort($._config.job_names.main_read_path + $._config.job_names.remote_ruler_read_path))) },
      { label: 'Main', value: $.jobMatcher($._config.job_names.main_read_path) },
      { label: 'Remote ruler', value: $.jobMatcher($._config.job_names.remote_ruler_read_path) },
    ])
    .addRow(
      $.row('Query-frontend')
      .addPanel(
        $.timeseriesPanel('Queue duration') +
        $.latencyPanel('cortex_query_frontend_queue_duration_seconds', '{$read_path_matcher}'),
      )
      .addPanel(
        $.timeseriesPanel('Retries') +
        $.latencyPanel('cortex_query_frontend_retries', '{$read_path_matcher}', multiplier=1) +
        { yaxes: $.yaxes('short') },
      )
      .addPanel(
        $.timeseriesPanel('Queue length (per %s)' % $._config.per_instance_label) +
        $.queryPanel(
          'sum by(%s) (cortex_query_frontend_queue_length{$read_path_matcher})' % [$._config.per_instance_label],
          '{{%s}}' % $._config.per_instance_label
        ),
      )
      .addPanel(
        $.timeseriesPanel('Queue length (per user)') +
        $.queryPanel(
          'sum by(user) (cortex_query_frontend_queue_length{$read_path_matcher}) > 0',
          '{{user}}'
        ) +
        { fieldConfig+: { defaults+: { noValue: '0' } } }
      )
    )
    .addRow(
      $.row('Query-scheduler')
      .addPanel(
        $.timeseriesPanel('Queue duration') +
        $.latencyPanel('cortex_query_scheduler_queue_duration_seconds', '{$read_path_matcher}'),
      )
      .addPanel(
        $.timeseriesPanel('Queue length (per %s)' % $._config.per_instance_label) +
        $.queryPanel(
          'sum by(%s) (cortex_query_scheduler_queue_length{$read_path_matcher})' % [$._config.per_instance_label],
          '{{%s}}' % $._config.per_instance_label
        ),
      )
      .addPanel(
        $.timeseriesPanel('Queue length (per user)') +
        $.queryPanel(
          'sum by(user) (cortex_query_scheduler_queue_length{$read_path_matcher}) > 0',
          '{{user}}'
        ) +
        { fieldConfig+: { defaults+: { noValue: '0' } } }
      )
    )
    .addRow(
      $.row('Query-frontend – query splitting and results cache')
      .addPanel(
        $.timeseriesPanel('Intervals per query') +
        $.queryPanel('sum(rate(cortex_frontend_split_queries_total{$read_path_matcher}[$__rate_interval])) / sum(rate(cortex_frontend_query_range_duration_seconds_count{$read_path_matcher, method="split_by_interval_and_results_cache"}[$__rate_interval]))', 'splitting rate') +
        $.panelDescription(
          'Intervals per query',
          |||
            The average number of split queries (partitioned by time) executed a single input query.
          |||
        ),
      )
      .addPanel(
        $.timeseriesPanel('Query results cache hit ratio') +
        $.queryPanel(
          |||
            # Query the new metric introduced in Mimir 2.10.
            (
              sum by(request_type) (rate(cortex_frontend_query_result_cache_hits_total{$read_path_matcher}[$__rate_interval]))
              /
              sum by(request_type) (rate(cortex_frontend_query_result_cache_requests_total{$read_path_matcher}[$__rate_interval]))
            )
            # Otherwise fallback to the previous general-purpose metrics.
            or
            (
              label_replace(
                # Query metrics before and after dskit cache refactor.
                sum (
                  rate(thanos_cache_memcached_hits_total{name="frontend-cache", $read_path_matcher}[$__rate_interval])
                  or ignoring(backend)
                  rate(thanos_cache_hits_total{name="frontend-cache", $read_path_matcher}[$__rate_interval])
                )
                /
                sum (
                  rate(thanos_cache_memcached_requests_total{name=~"frontend-cache", $read_path_matcher}[$__rate_interval])
                  or ignoring(backend)
                  rate(thanos_cache_requests_total{name=~"frontend-cache", $read_path_matcher}[$__rate_interval])
                ),
                "request_type", "query_range", "", "")
            )
          |||,
          '{{request_type}}',
        ) +
        { fieldConfig+: { defaults+: { unit: 'percentunit', min: 0, max: 1 } } }
      )
      .addPanel(
        $.timeseriesPanel('Query results cache skipped') +
        $.queryPanel(|||
          sum(rate(cortex_frontend_query_result_cache_skipped_total{$read_path_matcher}[$__rate_interval])) by (reason) /
          ignoring (reason) group_left sum(rate(cortex_frontend_query_result_cache_attempted_total{$read_path_matcher}[$__rate_interval]))
        |||, '{{reason}}') +
        $.stack +
        { fieldConfig+: { defaults+: { unit: 'percentunit', min: 0, max: 1 } } } +
        $.panelDescription(
          'Query results cache skipped',
          |||
            The % of queries whose results could not be cached.
            It is tracked for each split query when the splitting by interval is enabled.
          |||
        ),
      )
    )
    .addRow(
      $.row('Query-frontend – query sharding')
      .addPanel(
        $.timeseriesPanel('Sharded queries ratio') +
        $.queryPanel(|||
          sum(rate(cortex_frontend_query_sharding_rewrites_succeeded_total{$read_path_matcher}[$__rate_interval])) /
          sum(rate(cortex_frontend_query_sharding_rewrites_attempted_total{$read_path_matcher}[$__rate_interval]))
        |||, 'sharded queries ratio') +
        { fieldConfig+: { defaults+: { unit: 'percentunit', min: 0, max: 1 } } } +
        $.panelDescription(
          'Sharded queries ratio',
          |||
            The % of queries that have been successfully rewritten and executed in a shardable way.
            This panel only takes into account the type of queries that are supported by query sharding (eg. range queries).
          |||
        ),
      )
      .addPanel(
        $.timeseriesPanel('Number of sharded queries per query') +
        $.latencyPanel('cortex_frontend_sharded_queries_per_query', '{$read_path_matcher}', multiplier=1) +
        { fieldConfig+: { defaults+: { unit: 'short' } } } +
        $.panelDescription(
          'Number of sharded queries per query',
          |||
            The number of sharded queries that have been executed for a single input query. It only tracks queries that
            have been successfully rewritten in a shardable way.
          |||
        ),
      )
    )
    .addRowIf(
      $._config.show_ingest_storage_panels,
      $.row('Query-frontend – strong consistency (ingest storage)')
      .addPanel(
        $.ingestStorageStrongConsistencyRequestsPanel('query-frontend', '$read_path_matcher')
      )
      .addPanel(
        $.timeseriesPanel('Queries with strong read consistency ratio') +
        $.panelDescription(
          'Queries with strong read consistency ratio',
          |||
            Ratio between queries with strong read consistency and all other queries on query-frontends.
          |||
        ) +
        $.queryPanel(
          [
            |||
              sum by(container) (rate(cortex_query_frontend_queries_consistency_total{$read_path_matcher,consistency="strong"}[$__rate_interval]))
              /
              sum by(container) (rate(cortex_query_frontend_queries_total{$read_path_matcher}[$__rate_interval]))
            |||,
          ],
          ['{{container}}'],
        )
        + { fieldConfig+: { defaults+: { unit: 'percentunit', min: 0, max: 1 } } }
        + $.stack
      )
      .addPanel(
        $.ingestStorageStrongConsistencyWaitLatencyPanel('query-frontend', '$read_path_matcher')
      )
    )
    .addRowIf(
      $._config.show_ingest_storage_panels,
      $.row('')
      .addPanel(
        $.ingestStorageFetchLastProducedOffsetRequestsPanel('$read_path_matcher')
      )
      .addPanel(
        $.ingestStorageFetchLastProducedOffsetLatencyPanel('$read_path_matcher')
      )
    )
    .addRow(
      $.row('Ingester')
      .addPanel(
        $.timeseriesPanel('Series per query') +
        $.latencyRecordingRulePanel('cortex_ingester_queried_series', $.jobSelector($._config.job_names.ingester), multiplier=1) +
        { fieldConfig+: { defaults+: { unit: 'short' } } },
      )
      .addPanel(
        $.timeseriesPanel('Samples per query') +
        $.latencyRecordingRulePanel('cortex_ingester_queried_samples', $.jobSelector($._config.job_names.ingester), multiplier=1) +
        { fieldConfig+: { defaults+: { unit: 'short' } } },
      )
      .addPanel(
        $.timeseriesPanel('Exemplars per query') +
        $.latencyRecordingRulePanel('cortex_ingester_queried_exemplars', $.jobSelector($._config.job_names.ingester), multiplier=1) +
        { fieldConfig+: { defaults+: { unit: 'short' } } },
      )
    )
    .addRowIf(
      $._config.show_ingest_storage_panels,
      ($.row('Ingester – strong consistency (ingest storage)'))
      .addPanel(
        $.ingestStorageStrongConsistencyRequestsPanel('partition-reader', $.jobMatcher($._config.job_names.ingester))
      )
      .addPanel(
        $.timeseriesPanel('Requests with strong read consistency ratio') +
        $.panelDescription(
          'Requests with strong read consistency ratio',
          |||
            Ratio between requests with strong read consistency and all read requests on ingesters.
          |||
        ) +
        $.queryPanel(
          local ncSumRate = utils.ncHistogramSumBy(utils.ncHistogramCountRate($.queries.ingester.requestsPerSecondMetric, $.queries.ingester.readRequestsPerSecondSelector));
          local scSuccessful =
            |||
              (
                sum(rate(cortex_ingest_storage_strong_consistency_requests_total{%s}[$__rate_interval]))
                -
                sum(rate(cortex_ingest_storage_strong_consistency_failures_total{%s}[$__rate_interval]))
              )
            ||| % [$.jobMatcher($._config.job_names.ingester), $.jobMatcher($._config.job_names.ingester)];
          local scFailed =
            |||
              sum(rate(cortex_ingest_storage_strong_consistency_failures_total{%s}[$__rate_interval]))
            ||| % [$.jobMatcher($._config.job_names.ingester)];
          local scRate(sc, rate) = std.join(' / ', [sc, rate]);
          [
            scRate(scSuccessful, utils.showClassicHistogramQuery(ncSumRate)),
            scRate(scSuccessful, utils.showNativeHistogramQuery(ncSumRate)),
            scRate(scFailed, utils.showClassicHistogramQuery(ncSumRate)),
            scRate(scFailed, utils.showNativeHistogramQuery(ncSumRate)),
          ],
          ['successful', 'successful', 'failed', 'failed'],
        )
        + $.aliasColors({ failed: $._colors.failed, successful: $._colors.success })
        + { fieldConfig+: { defaults+: { unit: 'percentunit', min: 0, max: 1 } } }
        + $.stack
      )
      .addPanel(
        $.ingestStorageStrongConsistencyWaitLatencyPanel('partition-reader', $.jobMatcher($._config.job_names.ingester)),
      )
    )
    .addRowIf(
      $._config.show_ingest_storage_panels,
      $.row('')
      .addPanel(
        $.ingestStorageFetchLastProducedOffsetRequestsPanel($.jobMatcher($._config.job_names.ingester)),
      )
      .addPanel(
        $.ingestStorageFetchLastProducedOffsetLatencyPanel($.jobMatcher($._config.job_names.ingester)),
      )
      .addPanel(
        $.ingestStorageIngesterEndToEndLatencyWhenRunningPanel(),
      )
    )
    .addRow(
      $.row('Querier')
      .addPanel(
        $.timeseriesPanel('Number of store-gateways hit per query') +
        $.latencyPanel('cortex_querier_storegateway_instances_hit_per_query', '{$read_path_matcher}', multiplier=1) +
        { fieldConfig+: { defaults+: { unit: 'short' } } },
      )
      .addPanel(
        $.timeseriesPanel('Refetches of missing blocks per query') +
        $.latencyPanel('cortex_querier_storegateway_refetches_per_query', '{$read_path_matcher}', multiplier=1) +
        { fieldConfig+: { defaults+: { unit: 'short' } } },
      )
      .addPanel(
        $.timeseriesPanel('Consistency checks failed') +
        $.failurePanel('sum(rate(cortex_querier_blocks_consistency_checks_failed_total{$read_path_matcher}[$__rate_interval])) / sum(rate(cortex_querier_blocks_consistency_checks_total{$read_path_matcher}[$__rate_interval]))', 'Failure Rate') +
        { fieldConfig+: { defaults+: { unit: 'percentunit', min: 0, max: 1 } } } +
        $.panelDescription(
          'Consistency checks failed',
          |||
            Rate of queries that had to run with consistency checks and those checks failed. A failed consistency check means that some of at least one block which had to be queried wasn't present in any of the store-gateways.
          |||
        ),
      )
      .addPanel(
        $.timeseriesPanel('Rejected queries') +
        $.queryPanel('sum by (reason) (rate(cortex_querier_queries_rejected_total{$read_path_matcher}[$__rate_interval])) / ignoring (reason) group_left sum(rate(cortex_querier_request_duration_seconds_count{$read_path_matcher, route=~"%(routes_regex)s"}[$__rate_interval]))' % { routes_regex: $.queries.query_http_routes_regex }, '{{reason}}') +
        { fieldConfig+: { defaults+: { unit: 'percentunit', min: 0, max: 1 } } } +
        $.panelDescription(
          'Rejected queries',
          |||
            The proportion of all queries received by queriers that were rejected for some reason.
          |||
        ),
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.timeseriesPanel('Bucket indexes loaded (per querier)') +
        $.queryPanel([
          'max(cortex_bucket_index_loaded{$read_path_matcher})',
          'min(cortex_bucket_index_loaded{$read_path_matcher})',
          'avg(cortex_bucket_index_loaded{$read_path_matcher})',
        ], ['Max', 'Min', 'Average']) +
        { fieldConfig+: { defaults+: { unit: 'short' } } },
      )
      .addPanel(
        $.timeseriesPanel('Bucket indexes load / sec') +
        $.successFailurePanel(
          'sum(rate(cortex_bucket_index_loads_total{$read_path_matcher}[$__rate_interval])) - sum(rate(cortex_bucket_index_load_failures_total{$read_path_matcher}[$__rate_interval]))',
          'sum(rate(cortex_bucket_index_load_failures_total{$read_path_matcher}[$__rate_interval]))',
        ) +
        $.stack
      )
      .addPanel(
        $.timeseriesPanel('Bucket indexes load latency') +
        $.latencyPanel('cortex_bucket_index_load_duration_seconds', '{$read_path_matcher}'),
      )
    )
    .addRow(
      $.row('Store-gateway')
      .addPanel(
        $.timeseriesPanel('Blocks queried / sec by compaction level') +
        $.panelDescription(
          'Blocks queried / sec by compaction level',
          |||
            Increased volume of lower levels (for example levels 1 and 2) can indicate that the compactor is not keeping up.
            In that case the store-gateway will start serving more blocks which aren't that well compacted.
          |||
        ) +
        $.queryPanel([
            'sum(rate(cortex_bucket_store_series_blocks_queried_sum{component="store-gateway",level!~"[0-4]",%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.store_gateway),
            'sum by (level) (rate(cortex_bucket_store_series_blocks_queried_sum{component="store-gateway",level=~"[0-4]",%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.store_gateway),
          ],
          ['5+', '{{level}}'],
        ) +
        { fieldConfig+: { defaults+: { unit: 'ops' } } } +
        $.stack,
      )
      .addPanel(
        $.timeseriesPanel('Data fetched / sec') +
        $.queryPanel(|||
          sum by(data_type) (
            # Exclude "chunks refetched".
            rate(cortex_bucket_store_series_data_size_fetched_bytes_sum{component="store-gateway", stage!="refetched", %(jobMatcher)s}[$__rate_interval])
          )
        ||| % { jobMatcher: $.jobMatcher($._config.job_names.store_gateway) }, '{{data_type}}') +
        $.stack +
        { fieldConfig+: { defaults+: { unit: 'binBps' } } },
      )
      .addPanel(
        $.timeseriesPanel('Data touched / sec') +
        $.queryPanel(|||
          sum by(data_type) (
            # Exclude "chunks processed" to only count "chunks returned", other than postings and series.
            rate(cortex_bucket_store_series_data_size_touched_bytes_sum{component="store-gateway", stage!="processed",%(jobMatcher)s}[$__rate_interval])
          )
        ||| % { jobMatcher: $.jobMatcher($._config.job_names.store_gateway) }, '{{data_type}}') +
        $.stack +
        { fieldConfig+: { defaults+: { unit: 'binBps' } } },
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.timeseriesPanel('Series request average latency') +
        $.queryPanel(
          |||
            sum by(stage) (rate(cortex_bucket_store_series_request_stage_duration_seconds_sum{%s}[$__rate_interval]))
            /
            sum by(stage) (rate(cortex_bucket_store_series_request_stage_duration_seconds_count{%s}[$__rate_interval]))
          ||| % [$.jobMatcher($._config.job_names.store_gateway), $.jobMatcher($._config.job_names.store_gateway)],
          '{{stage}}'
        ) +
        $.stack +
        $.showAllTooltip +
        { fieldConfig+: { defaults+: { unit: 's' } } },
      )
      .addPanel(
        $.timeseriesPanel('Series request 99th percentile latency') +
        $.queryPanel(
          |||
            histogram_quantile(0.99, sum by(stage, le) (rate(cortex_bucket_store_series_request_stage_duration_seconds_bucket{%s}[$__rate_interval])))
          ||| % [$.jobMatcher($._config.job_names.store_gateway)],
          '{{stage}}'
        ) +
        $.stack +
        $.showAllTooltip +
        { fieldConfig+: { defaults+: { unit: 's' } } },
      )
      .addPanel(
        $.timeseriesPanel('Series batch preloading efficiency') +
        $.queryPanel(
          |||
            # Clamping min to 0 because if preloading not useful at all, then the actual value we get is
            # slightly negative because of the small overhead introduced by preloading.
            clamp_min(1 - (
                sum(rate(cortex_bucket_store_series_batch_preloading_wait_duration_seconds_sum{%s}[$__rate_interval]))
                /
                sum(rate(cortex_bucket_store_series_batch_preloading_load_duration_seconds_sum{%s}[$__rate_interval]))
            ), 0)
          ||| % [$.jobMatcher($._config.job_names.store_gateway), $.jobMatcher($._config.job_names.store_gateway)],
          '% of time reduced by preloading'
        ) +
        { fieldConfig+: { defaults+: { unit: 'percentunit', min: 0, max: 1 } } } +
        $.panelDescription(
          'Series batch preloading efficiency',
          |||
            This panel shows the % of time reduced by preloading, for Series() requests which have been
            split to 2+ batches. If a Series() request is served within a single batch, then preloading
            is not triggered, and thus not counted in this measurement.
          |||
        ),
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.timeseriesPanel('Blocks currently owned') +
        $.queryPanel('cortex_bucket_store_blocks_loaded{component="store-gateway",%s}' % $.jobMatcher($._config.job_names.store_gateway), '{{%s}}' % $._config.per_instance_label) +
        { fieldConfig+: { defaults+: { custom+: { fillOpacity: 0 } } } } +
        $.panelDescription(
          'Blocks currently owned',
          |||
            This panel shows the number of blocks owned by each store-gateway replica.
            For each owned block, the store-gateway keeps its index-header on disk, and
            eventually loaded in memory (if index-header lazy loading is disabled, or lazy loading
            is enabled and the index-header was loaded).
          |||
        ),
      )
      .addPanel(
        $.timeseriesPanel('Blocks loaded / sec') +
        $.successFailurePanel(
          'sum(rate(cortex_bucket_store_block_loads_total{component="store-gateway",%s}[$__rate_interval])) - sum(rate(cortex_bucket_store_block_load_failures_total{component="store-gateway",%s}[$__rate_interval]))' % [$.jobMatcher($._config.job_names.store_gateway), $.jobMatcher($._config.job_names.store_gateway)],
          'sum(rate(cortex_bucket_store_block_load_failures_total{component="store-gateway",%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.store_gateway),
        ) +
        $.stack
      )
      .addPanel(
        $.timeseriesPanel('Blocks dropped / sec') +
        $.successFailurePanel(
          'sum(rate(cortex_bucket_store_block_drops_total{component="store-gateway",%s}[$__rate_interval])) - sum(rate(cortex_bucket_store_block_drop_failures_total{component="store-gateway",%s}[$__rate_interval]))' % [$.jobMatcher($._config.job_names.store_gateway), $.jobMatcher($._config.job_names.store_gateway)],
          'sum(rate(cortex_bucket_store_block_drop_failures_total{component="store-gateway",%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.store_gateway),
        ) +
        $.stack
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.timeseriesPanel('Lazy loaded index-headers') +
        $.queryPanel('cortex_bucket_store_indexheader_lazy_load_total{%s} - cortex_bucket_store_indexheader_lazy_unload_total{%s}' % [$.jobMatcher($._config.job_names.store_gateway), $.jobMatcher($._config.job_names.store_gateway)], '{{%s}}' % $._config.per_instance_label) +
        { fieldConfig+: { defaults+: { custom+: { fillOpacity: 0 } } } }
      )
      .addPanel(
        $.timeseriesPanel('Index-header lazy load duration') +
        $.latencyPanel('cortex_bucket_store_indexheader_lazy_load_duration_seconds', '{%s}' % $.jobMatcher($._config.job_names.store_gateway)),
      )
      .addPanel(
        $.timeseriesPanel('Index-header lazy load gate latency') +
        $.latencyPanel('cortex_bucket_stores_gate_duration_seconds', '{%s,gate="index_header"}' % $.jobMatcher($._config.job_names.store_gateway)) +
        $.panelDescription(
          'Index-header lazy load gate latency',
          |||
            Time spent waiting for a turn to load an index header. This time is not included in "Index-header lazy load duration."
          |||
        )
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.timeseriesPanel('Series hash cache hit ratio') +
        $.queryPanel(|||
          sum(rate(cortex_bucket_store_series_hash_cache_hits_total{%s}[$__rate_interval]))
          /
          sum(rate(cortex_bucket_store_series_hash_cache_requests_total{%s}[$__rate_interval]))
        ||| % [$.jobMatcher($._config.job_names.store_gateway), $.jobMatcher($._config.job_names.store_gateway)], 'hit ratio') +
        { fieldConfig+: { defaults+: { unit: 'percentunit', min: 0, max: 1 } } },
      )
      .addPanel(
        $.timeseriesPanel('ExpandedPostings cache hit ratio') +
        $.queryPanel(|||
          sum(rate(thanos_store_index_cache_hits_total{item_type="ExpandedPostings",%s}[$__rate_interval]))
          /
          sum(rate(thanos_store_index_cache_requests_total{item_type="ExpandedPostings",%s}[$__rate_interval]))
        ||| % [$.jobMatcher($._config.job_names.store_gateway), $.jobMatcher($._config.job_names.store_gateway)], 'hit ratio') +
        { fieldConfig+: { defaults+: { unit: 'percentunit', min: 0, max: 1 } } },
      )
      .addPanel(
        $.timeseriesPanel('Chunks attributes in-memory cache hit ratio') +
        $.queryPanel(|||
          sum(rate(cortex_cache_memory_hits_total{name="chunks-attributes-cache",%s}[$__rate_interval]))
          /
          sum(rate(cortex_cache_memory_requests_total{name="chunks-attributes-cache",%s}[$__rate_interval]))
        ||| % [$.jobMatcher($._config.job_names.store_gateway), $.jobMatcher($._config.job_names.store_gateway)], 'hit ratio') +
        { fieldConfig+: { defaults+: { unit: 'percentunit', min: 0, max: 1 } } },
      )
    ),
}
