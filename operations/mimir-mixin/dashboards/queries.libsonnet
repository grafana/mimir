local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-queries.json';

(import 'dashboard-utils.libsonnet') {
  local queryMemoryConsumptionPanel(display_name, job_name) =
    $.heatmapPanel('Estimated per-query memory consumption') +
    $.queryPanel('sum(rate(cortex_mimir_query_engine_estimated_query_peak_memory_consumption{$read_path_matcher, %s}[$__rate_interval]))' % $.jobMatcher(job_name), 'Estimated memory consumption') +
    {
      options+: {
        legend+: {
          show: false,
        },
        tooltip+: {
          yHistogram: true,
        },
        cellValues+: {
          unit: 'reqps',
        },
        yAxis+: {
          min: 0,
          unit: 'bytes',
        },
        cellGap: 0,
        calculation+: {
          xBuckets: {
            mode: 'count',
            value: 60,
          },
          yBuckets: {
            mode: 'count',
            value: 40,
          },
        },
      },
    } +
    $.panelDescription(
      'Estimated per-query memory consumption',
      |||
        The esimated memory consumption of all queries evaluated by %ss. Only applicable if the Mimir query engine (MQE) is enabled and the query was evaluated with MQE.
      ||| % display_name
    ),

  local mqeFallbackPanel(display_name, job_name) =
    $.timeseriesPanel("Fallback to Prometheus' query engine") +
    $.queryPanel('sum(rate(cortex_mimir_query_engine_unsupported_queries_total{$read_path_matcher, %s}[$__rate_interval])) or vector(0)' % $.jobMatcher(job_name), 'Queries') +
    { fieldConfig+: { defaults+: { unit: 'reqps' } } } +
    $.panelDescription(
      "Fallback to Prometheus' query engine",
      |||
        The rate of queries that fell back to Prometheus' query engine in %ss. Only applicable if the Mimir query engine (MQE) is enabled.
      ||| % display_name
    ),

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
    .addRowIf(
      $._config.gateway_enabled,
      $.row('Gateway')
      .addPanel(
        $.timeseriesPanel('Queries / sec by read path') +
        $.panelDescription(
          'Queries / sec by read path',
          |||
            Queries coming from Grafana Managed Alerting to the gateway will be routed to the remote ruler query path.
          |||
        ) +
        local selector = $.jobMatcher($._config.job_names.gateway);
        local query = utils.ncHistogramApplyTemplate(
          template='label_replace(label_replace(%s, "proxy", "Remote ruler read path", "proxy", "^alternate_query_proxy$"),"proxy", "Main read path", "proxy", "^query_proxy$")',
          query=utils.ncHistogramSumBy(
            query=utils.ncHistogramCountRate('cortex_conditional_handler_request_duration_seconds', selector),
            sum_by=['proxy'],
          )
        );
        $.queryPanel(
          [utils.showClassicHistogramQuery(query), utils.showNativeHistogramQuery(query)],
          ['{{proxy}}', '{{proxy}}'],
        ) +
        { fieldConfig+: { defaults+: { unit: 'reqps' } } }
      )
    )
    .addRow(
      $.row('Query-frontend')
      .addPanel(
        $.timeseriesPanel('Queue duration') +
        $.onlyRelevantIfQuerySchedulerDisabled('Queue duration') +
        $.ncLatencyPanel('cortex_query_frontend_queue_duration_seconds', '$read_path_matcher'),
      )
      .addPanel(
        $.timeseriesPanel('Retries') +
        $.ncLatencyPanel('cortex_query_frontend_retries', '$read_path_matcher', multiplier=1) +
        { fieldConfig+: { defaults+: { unit: 'short' } } },
      )
      .addPanel(
        $.timeseriesPanel('Queue length (per %s)' % $._config.per_instance_label) +
        $.onlyRelevantIfQuerySchedulerDisabled('Queue length') +
        $.queryPanel(
          'sum by(%s) (cortex_query_frontend_queue_length{$read_path_matcher})' % [$._config.per_instance_label],
          '{{%s}}' % $._config.per_instance_label
        ) +
        $.showAllTooltip,
      )
      .addPanel(
        $.timeseriesPanel('Queue length (per user)') +
        $.onlyRelevantIfQuerySchedulerDisabled('Queue length') +
        $.queryPanel(
          'sum by(user) (cortex_query_frontend_queue_length{$read_path_matcher}) > 0',
          '{{user}}'
        ) +
        $.showAllTooltip +
        { fieldConfig+: { defaults+: { noValue: '0' } } }
      )
    )
    .addRow(
      $.row('Query-scheduler')
      .addPanel(
        $.timeseriesPanel('Queue duration') +
        $.onlyRelevantIfQuerySchedulerEnabled('Queue duration') +
        $.ncLatencyPanel('cortex_query_scheduler_queue_duration_seconds', '$read_path_matcher'),
      )
      .addPanel(
        $.timeseriesPanel('Queue length (per %s)' % $._config.per_instance_label) +
        $.onlyRelevantIfQuerySchedulerEnabled('Queue length') +
        $.queryPanel(
          'sum by(%s) (cortex_query_scheduler_queue_length{$read_path_matcher})' % [$._config.per_instance_label],
          '{{%s}}' % $._config.per_instance_label
        ) +
        $.showAllTooltip,
      )
      .addPanel(
        $.timeseriesPanel('Queue length (per user)') +
        $.onlyRelevantIfQuerySchedulerEnabled('Queue length') +
        $.queryPanel(
          'sum by(user) (cortex_query_scheduler_queue_length{$read_path_matcher}) > 0',
          '{{user}}'
        ) +
        $.showAllTooltip +
        { fieldConfig+: { defaults+: { noValue: '0' } } }
      )
    )
    .addRow(
      $.row('Query-frontend – query splitting and results cache')
      .addPanel(
        local query = utils.ncHistogramApplyTemplate(
          template='sum(rate(cortex_frontend_split_queries_total{$read_path_matcher}[$__rate_interval])) / %s',
          query=utils.ncHistogramSumBy(
            query=utils.ncHistogramCountRate('cortex_frontend_query_range_duration_seconds', '$read_path_matcher, method="split_by_interval_and_results_cache"'),
          )
        );
        $.timeseriesPanel('Intervals per query') +
        $.queryPanel(
          [utils.showClassicHistogramQuery(query), utils.showNativeHistogramQuery(query)],
          ['splitting rate', 'splitting rate'],
        ) +
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
                sum (
                  rate(thanos_cache_hits_total{name="frontend-cache", $read_path_matcher}[$__rate_interval])
                )
                /
                sum (
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
        $.ncLatencyPanel('cortex_frontend_sharded_queries_per_query', '$read_path_matcher', multiplier=1) +
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
    .addRow(
      $.row('Query-frontend - Query Lengths')
      .addPanel(
        $.heatmapPanel('Query Expression Length') +
        $.queryPanel(
          'sum(rate(cortex_query_frontend_queries_expression_bytes{namespace="$namespace"}[$__rate_interval]))',
          'Bytes'
        ) +
        { fieldConfig+: { defaults+: { unit: 'bytes' } } }
      )
      .addPanel(
        $.timeseriesPanel('Query Expression Percentiles') +
        $.queryPanel(
          'histogram_quantile(0.99, sum(rate(cortex_query_frontend_queries_expression_bytes{namespace="$namespace"}[$__rate_interval])))',
          '99th Percentile'
        ) +
        $.queryPanel(
          'histogram_quantile(0.90, sum(rate(cortex_query_frontend_queries_expression_bytes{namespace="$namespace"}[$__rate_interval])))',
          '90th Percentile'
        ) +
        $.queryPanel(
          'histogram_avg(sum(rate(cortex_query_frontend_queries_expression_bytes{namespace="$namespace"}[$__rate_interval])))',
          'Average'
        ) +
        { fieldConfig+: { defaults+: { unit: 'bytes' } } }
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
      $.row('Query-frontend - query engine')
      .addPanel(queryMemoryConsumptionPanel('query-frontend', $._config.job_names.query_frontend))
      .addPanel(mqeFallbackPanel('query-frontend', $._config.job_names.query_frontend))
    )
    .addRow(
      $.row('Ingester')
      .addPanel(
        $.timeseriesPanel('Series per query') +
        $.latencyRecordingRulePanelNativeHistogram('cortex_ingester_queried_series', $.jobSelector($._config.job_names.ingester) + [utils.selector.eq('stage', 'merged_blocks')], multiplier=1) +
        { fieldConfig+: { defaults+: { unit: 'short' } } },
      )
      .addPanel(
        $.timeseriesPanel('Samples per query') +
        $.latencyRecordingRulePanelNativeHistogram('cortex_ingester_queried_samples', $.jobSelector($._config.job_names.ingester), multiplier=1) +
        { fieldConfig+: { defaults+: { unit: 'short' } } },
      )
      .addPanel(
        $.timeseriesPanel('Exemplars per query') +
        $.latencyRecordingRulePanelNativeHistogram('cortex_ingester_queried_exemplars', $.jobSelector($._config.job_names.ingester), multiplier=1) +
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
        $.ncLatencyPanel('cortex_querier_storegateway_instances_hit_per_query', '$read_path_matcher', multiplier=1) +
        { fieldConfig+: { defaults+: { unit: 'short' } } },
      )
      .addPanel(
        $.timeseriesPanel('Refetches of missing blocks per query') +
        $.ncLatencyPanel('cortex_querier_storegateway_refetches_per_query', '$read_path_matcher', multiplier=1) +
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
    )
    .addRow(
      $.row('')
      .addPanel(
        local selector = '$read_path_matcher, route=~"%s"' % $.queries.query_http_routes_regex;
        local query = utils.ncHistogramApplyTemplate(
          template='sum by (reason) (rate(cortex_querier_queries_rejected_total{$read_path_matcher}[$__rate_interval])) / ignoring (reason) group_left %s',
          query=utils.ncHistogramSumBy(utils.ncHistogramCountRate('cortex_querier_request_duration_seconds', selector)),
        );
        $.timeseriesPanel('Rejected queries') +
        $.queryPanel(
          [utils.showClassicHistogramQuery(query), utils.showNativeHistogramQuery(query)],
          ['{{reason}}', '{{reason}}'],
        ) +
        { fieldConfig+: { defaults+: { unit: 'percentunit', min: 0, max: 1 } } } +
        $.panelDescription(
          'Rejected queries',
          |||
            The proportion of all queries received by queriers that were rejected for some reason.
          |||
        ),
      )
      .addPanel(queryMemoryConsumptionPanel('querier', $._config.job_names.querier))
      .addPanel(mqeFallbackPanel('querier', $._config.job_names.querier))
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
        $.ncLatencyPanel('cortex_bucket_index_load_duration_seconds', '$read_path_matcher'),
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
        $.queryPanel(
          [
            'sum by (level) (rate(cortex_bucket_store_series_blocks_queried_sum{component="store-gateway",level=~"[0-4]",%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.store_gateway),
            'sum(rate(cortex_bucket_store_series_blocks_queried_sum{component="store-gateway",level!~"[0-4]",%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.store_gateway),
          ],
          ['{{level}}', '5+'],
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
        local selector = $.jobMatcher($._config.job_names.store_gateway);
        local query = utils.ncHistogramAverageRate('cortex_bucket_store_series_request_stage_duration_seconds', selector, sum_by=['stage']);
        $.timeseriesPanel('Series request average latency') +
        $.queryPanel(
          [utils.showClassicHistogramQuery(query), utils.showNativeHistogramQuery(query)],
          ['{{stage}}', '{{stage}}'],
        ) +
        $.stack +
        $.showAllTooltip +
        { fieldConfig+: { defaults+: { unit: 's' } } },
      )
      .addPanel(
        local selector = $.jobMatcher($._config.job_names.store_gateway);
        local query = utils.ncHistogramQuantile('0.99', 'cortex_bucket_store_series_request_stage_duration_seconds', selector, sum_by=['stage']);
        $.timeseriesPanel('Series request 99th percentile latency') +
        $.queryPanel(
          [utils.showClassicHistogramQuery(query), utils.showNativeHistogramQuery(query)],
          ['{{stage}}', '{{stage}}'],
        ) +
        $.stack +
        $.showAllTooltip +
        { fieldConfig+: { defaults+: { unit: 's' } } },
      )
      .addPanel(
        local selector = $.jobMatcher($._config.job_names.store_gateway);
        local query = {
          classic:
            |||
              # Clamping min to 0 because if preloading not useful at all, then the actual value we get is
              # slightly negative because of the small overhead introduced by preloading.
              clamp_min(1 - (
                  sum(rate(cortex_bucket_store_series_batch_preloading_wait_duration_seconds_sum{%(selector)s}[$__rate_interval]))
                  /
                  sum(rate(cortex_bucket_store_series_batch_preloading_load_duration_seconds_sum{%(selector)s}[$__rate_interval]))
              ), 0)
            ||| % { selector: selector },
          native:
            |||
              # Clamping min to 0 because if preloading not useful at all, then the actual value we get is
              # slightly negative because of the small overhead introduced by preloading.
              clamp_min(1 - (
                  sum(histogram_sum(rate(cortex_bucket_store_series_batch_preloading_wait_duration_seconds{%(selector)s}[$__rate_interval])))
                  /
                  sum(histogram_sum(rate(cortex_bucket_store_series_batch_preloading_load_duration_seconds{%(selector)s}[$__rate_interval])))
              ), 0)
            ||| % { selector: selector },
        };
        $.timeseriesPanel('Series batch preloading efficiency') +
        $.queryPanel(
          [utils.showClassicHistogramQuery(query), utils.showNativeHistogramQuery(query)],
          ['% of time reduced by preloading', '% of time reduced by preloading'],
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
        $.queryPanel('sum (cortex_bucket_store_blocks_loaded{component="store-gateway",%s})' % $.jobMatcher($._config.job_names.store_gateway), '{{%s}}' % $._config.per_instance_label) +
        { fieldConfig+: { defaults+: { custom+: { fillOpacity: 0 } } } } +
        $.panelDescription(
          'Blocks currently owned',
          |||
            This panel shows the number of blocks owned by each store-gateway replica.
            For each owned block, the store-gateway keeps its index-header on disk, and
            eventually loaded in memory (if index-header lazy loading is disabled, or lazy loading
            is enabled and the index-header was loaded).
          |||
        ) +
        $.showAllTooltip,
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
        $.showAllTooltip +
        { fieldConfig+: { defaults+: { custom+: { fillOpacity: 0 } } } }
      )
      .addPanel(
        $.timeseriesPanel('Index-header lazy load duration') +
        $.ncLatencyPanel('cortex_bucket_store_indexheader_lazy_load_duration_seconds', $.jobMatcher($._config.job_names.store_gateway)),
      )
      .addPanel(
        $.timeseriesPanel('Index-header lazy load gate latency') +
        $.ncLatencyPanel('cortex_bucket_stores_gate_duration_seconds', '%s,gate="index_header"' % $.jobMatcher($._config.job_names.store_gateway)) +
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
