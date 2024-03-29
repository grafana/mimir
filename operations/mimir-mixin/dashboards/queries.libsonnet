local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-queries.json';

(import 'dashboard-utils.libsonnet') {
  [filename]:
    assert std.md5(filename) == 'b3abe8d5c040395cc36615cb4334c92d' : 'UID of the dashboard has changed, please update references to dashboard.';
    ($.dashboard('Queries') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates()
    .addRow(
      $.row('Query-frontend')
      .addPanel(
        $.timeseriesPanel('Queue duration') +
        $.latencyPanel('cortex_query_frontend_queue_duration_seconds', '{%s}' % $.jobMatcher($._config.job_names.query_frontend)),
      )
      .addPanel(
        $.timeseriesPanel('Retries') +
        $.latencyPanel('cortex_query_frontend_retries', '{%s}' % $.jobMatcher($._config.job_names.query_frontend), multiplier=1) +
        { yaxes: $.yaxes('short') },
      )
      .addPanel(
        $.timeseriesPanel('Queue length (per %s)' % $._config.per_instance_label) +
        $.queryPanel(
          'sum by(%s) (cortex_query_frontend_queue_length{%s})' % [$._config.per_instance_label, $.jobMatcher($._config.job_names.query_frontend)],
          '{{%s}}' % $._config.per_instance_label
        ),
      )
      .addPanel(
        $.timeseriesPanel('Queue length (per user)') +
        $.queryPanel(
          'sum by(user) (cortex_query_frontend_queue_length{%s}) > 0' % [$.jobMatcher($._config.job_names.query_frontend)],
          '{{user}}'
        ) +
        { fieldConfig+: { defaults+: { noValue: '0' } } }
      )
    )
    .addRow(
      $.row('Query-scheduler')
      .addPanel(
        $.timeseriesPanel('Queue duration') +
        $.latencyPanel('cortex_query_scheduler_queue_duration_seconds', '{%s}' % $.jobMatcher($._config.job_names.query_scheduler)),
      )
      .addPanel(
        $.timeseriesPanel('Queue length (per %s)' % $._config.per_instance_label) +
        $.queryPanel(
          'sum by(%s) (cortex_query_scheduler_queue_length{%s})' % [$._config.per_instance_label, $.jobMatcher($._config.job_names.query_scheduler)],
          '{{%s}}' % $._config.per_instance_label
        ),
      )
      .addPanel(
        $.timeseriesPanel('Queue length (per user)') +
        $.queryPanel(
          'sum by(user) (cortex_query_scheduler_queue_length{%s}) > 0' % [$.jobMatcher($._config.job_names.query_scheduler)],
          '{{user}}'
        ) +
        { fieldConfig+: { defaults+: { noValue: '0' } } }
      )
    )
    .addRow(
      $.row('Query-frontend - query splitting and results cache')
      .addPanel(
        $.timeseriesPanel('Intervals per query') +
        $.queryPanel('sum(rate(cortex_frontend_split_queries_total{%s}[$__rate_interval])) / sum(rate(cortex_frontend_query_range_duration_seconds_count{%s, method="split_by_interval_and_results_cache"}[$__rate_interval]))' % [$.jobMatcher($._config.job_names.query_frontend), $.jobMatcher($._config.job_names.query_frontend)], 'splitting rate') +
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
              sum by(request_type) (rate(cortex_frontend_query_result_cache_hits_total{%(frontend)s}[$__rate_interval]))
              /
              sum by(request_type) (rate(cortex_frontend_query_result_cache_requests_total{%(frontend)s}[$__rate_interval]))
            )
            # Otherwise fallback to the previous general-purpose metrics.
            or
            (
              label_replace(
                # Query metrics before and after dskit cache refactor.
                sum (
                  rate(thanos_cache_memcached_hits_total{name="frontend-cache", %(frontend)s}[$__rate_interval])
                  or ignoring(backend)
                  rate(thanos_cache_hits_total{name="frontend-cache", %(frontend)s}[$__rate_interval])
                )
                /
                sum (
                  rate(thanos_cache_memcached_requests_total{name=~"frontend-cache", %(frontend)s}[$__rate_interval])
                  or ignoring(backend)
                  rate(thanos_cache_requests_total{name=~"frontend-cache", %(frontend)s}[$__rate_interval])
                ),
                "request_type", "query_range", "", "")
            )
          ||| % {
            frontend: $.jobMatcher($._config.job_names.query_frontend),
          },
          '{{request_type}}',
        ) +
        { fieldConfig+: { defaults+: { unit: 'percentunit', min: 0, max: 1 } } }
      )
      .addPanel(
        $.timeseriesPanel('Query results cache skipped') +
        $.queryPanel(|||
          sum(rate(cortex_frontend_query_result_cache_skipped_total{%s}[$__rate_interval])) by (reason) /
          ignoring (reason) group_left sum(rate(cortex_frontend_query_result_cache_attempted_total{%s}[$__rate_interval]))
        ||| % [$.jobMatcher($._config.job_names.query_frontend), $.jobMatcher($._config.job_names.query_frontend)], '{{reason}}') +
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
      $.row('Query-frontend - query sharding')
      .addPanel(
        $.timeseriesPanel('Sharded queries ratio') +
        $.queryPanel(|||
          sum(rate(cortex_frontend_query_sharding_rewrites_succeeded_total{%s}[$__rate_interval])) /
          sum(rate(cortex_frontend_query_sharding_rewrites_attempted_total{%s}[$__rate_interval]))
        ||| % [$.jobMatcher($._config.job_names.query_frontend), $.jobMatcher($._config.job_names.query_frontend)], 'sharded queries ratio') +
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
        $.latencyPanel('cortex_frontend_sharded_queries_per_query', '{%s}' % $.jobMatcher($._config.job_names.query_frontend), multiplier=1) +
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
      ($.row('Ingester (ingest storage: strong consistency)'))
      .addPanel(
        $.timeseriesPanel('Requests with strong read consistency / sec') +
        $.panelDescription(
          'Requests with strong read consistency / sec',
          |||
            Shows rate of requests with strong read consistency, and rate of failed requests with strong read consistency.
          |||
        ) +
        $.queryPanel(
          [
            |||
              sum(rate(cortex_ingest_storage_strong_consistency_requests_total{%s}[$__rate_interval]))
              -
              sum(rate(cortex_ingest_storage_strong_consistency_failures_total{%s}[$__rate_interval]))
            ||| % [$.jobMatcher($._config.job_names.ingester), $.jobMatcher($._config.job_names.ingester)],
            |||
              sum(rate(cortex_ingest_storage_strong_consistency_failures_total{%s}[$__rate_interval]))
            ||| % [$.jobMatcher($._config.job_names.ingester)],
          ],
          [
            'successful',
            'failed',
          ],
        ) + {
          fieldConfig+: {
            defaults+: { unit: 'reqps' },
          },
        } + $.aliasColors({ successful: $._colors.success, failed: $._colors.failed }) + $.stack,
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
          [
            |||
              (
                sum(rate(cortex_ingest_storage_strong_consistency_requests_total{%s}[$__rate_interval]))
                -
                sum(rate(cortex_ingest_storage_strong_consistency_failures_total{%s}[$__rate_interval]))
              )
              /
              sum(rate(cortex_request_duration_seconds_count{%s,route=~"%s"}[$__rate_interval]))
            ||| % [$.jobMatcher($._config.job_names.ingester), $.jobMatcher($._config.job_names.ingester), $.jobMatcher($._config.job_names.ingester), $._config.ingester_read_path_routes_regex],
            |||
              sum(rate(cortex_ingest_storage_strong_consistency_failures_total{%s}[$__rate_interval]))
              /
              sum(rate(cortex_request_duration_seconds_count{%s,route=~"%s"}[$__rate_interval]))
            ||| % [$.jobMatcher($._config.job_names.ingester), $.jobMatcher($._config.job_names.ingester), $._config.ingester_read_path_routes_regex],
          ],
          ['successful', 'failed'],
        )
        + $.aliasColors({ failed: $._colors.failed, successful: $._colors.success })
        + { fieldConfig+: { defaults+: { unit: 'percentunit', min: 0, max: 1 } } }
        + $.stack
      )
      .addPanel(
        $.timeseriesPanel('Strong read consistency queries — wait latency') +
        $.panelDescription(
          'Strong read consistency queries — wait latency',
          |||
            How long does the request wait to guarantee strong read consistency.
          |||
        ) +
        $.queryPanel(
          [
            'histogram_quantile(0.5, sum(rate(cortex_ingest_storage_strong_consistency_wait_duration_seconds{%s}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.ingester)],
            'histogram_quantile(0.99, sum(rate(cortex_ingest_storage_strong_consistency_wait_duration_seconds{%s}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.ingester)],
            'histogram_quantile(0.999, sum(rate(cortex_ingest_storage_strong_consistency_wait_duration_seconds{%s}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.ingester)],
            'histogram_quantile(1.0, sum(rate(cortex_ingest_storage_strong_consistency_wait_duration_seconds{%s}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.ingester)],
          ],
          [
            '50th percentile',
            '99th percentile',
            '99.9th percentile',
            '100th percentile',
          ],
        ) + {
          fieldConfig+: {
            defaults+: { unit: 's' },
          },
        },
      )
    )
    .addRowIf(
      $._config.show_ingest_storage_panels,
      ($.row('Ingester (ingest storage: last produced offset)'))
      .addPanel(
        $.timeseriesPanel('Last produced offset requests / sec') +
        $.panelDescription(
          'Rate of requests to fetch last produced offset for partition',
          |||
            Shows rate of requests to fetch last produced offset for partition, and rate of failed requests.
          |||
        ) +
        $.queryPanel(
          [
            |||
              sum(rate(cortex_ingest_storage_reader_last_produced_offset_requests_total{%s}[$__rate_interval]))
              -
              sum(rate(cortex_ingest_storage_reader_last_produced_offset_failures_total{%s}[$__rate_interval]))
            ||| % [$.jobMatcher($._config.job_names.ingester), $.jobMatcher($._config.job_names.ingester)],
            |||
              sum(rate(cortex_ingest_storage_reader_last_produced_offset_failures_total{%s}[$__rate_interval]))
            ||| % [$.jobMatcher($._config.job_names.ingester)],
          ],
          [
            'successful',
            'failed',
          ],
        ) + {
          fieldConfig+: {
            defaults+: { unit: 'reqps' },
          },
        } + $.aliasColors({ successful: $._colors.success, failed: $._colors.failed }) + $.stack,
      )
      .addPanel(
        $.timeseriesPanel('Last produced offset latency') +
        $.panelDescription(
          'Latency',
          |||
            How long does it take to fetch "last produced offset" of partition.
          |||
        ) +
        $.queryPanel(
          [
            'histogram_quantile(0.5, sum(rate(cortex_ingest_storage_reader_last_produced_offset_request_duration_seconds{%s}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.ingester)],
            'histogram_quantile(0.99, sum(rate(cortex_ingest_storage_reader_last_produced_offset_request_duration_seconds{%s}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.ingester)],
            'histogram_quantile(0.999, sum(rate(cortex_ingest_storage_reader_last_produced_offset_request_duration_seconds{%s}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.ingester)],
            'histogram_quantile(1.0, sum(rate(cortex_ingest_storage_reader_last_produced_offset_request_duration_seconds{%s}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.ingester)],
          ],
          [
            '50th percentile',
            '99th percentile',
            '99.9th percentile',
            '100th percentile',
          ],
        ) + {
          fieldConfig+: {
            defaults+: { unit: 's' },
          },
        },
      )
    )
    .addRow(
      $.row('Querier')
      .addPanel(
        $.timeseriesPanel('Number of store-gateways hit per query') +
        $.latencyPanel('cortex_querier_storegateway_instances_hit_per_query', '{%s}' % $.jobMatcher($._config.job_names.querier), multiplier=1) +
        { fieldConfig+: { defaults+: { unit: 'short' } } },
      )
      .addPanel(
        $.timeseriesPanel('Refetches of missing blocks per query') +
        $.latencyPanel('cortex_querier_storegateway_refetches_per_query', '{%s}' % $.jobMatcher($._config.job_names.querier), multiplier=1) +
        { fieldConfig+: { defaults+: { unit: 'short' } } },
      )
      .addPanel(
        $.timeseriesPanel('Consistency checks failed') +
        $.failurePanel('sum(rate(cortex_querier_blocks_consistency_checks_failed_total{%s}[$__rate_interval])) / sum(rate(cortex_querier_blocks_consistency_checks_total{%s}[$__rate_interval]))' % [$.jobMatcher($._config.job_names.querier), $.jobMatcher($._config.job_names.querier)], 'Failure Rate') +
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
        $.queryPanel('sum by (reason) (rate(cortex_querier_queries_rejected_total{%(job_matcher)s}[$__rate_interval])) / ignoring (reason) group_left sum(rate(cortex_querier_request_duration_seconds_count{%(job_matcher)s, route=~"%(routes_regex)s"}[$__rate_interval]))' % { job_matcher: $.jobMatcher($._config.job_names.querier), routes_regex: $.queries.query_http_routes_regex }, '{{reason}}') +
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
          'max(cortex_bucket_index_loaded{%s})' % $.jobMatcher($._config.job_names.querier),
          'min(cortex_bucket_index_loaded{%s})' % $.jobMatcher($._config.job_names.querier),
          'avg(cortex_bucket_index_loaded{%s})' % $.jobMatcher($._config.job_names.querier),
        ], ['Max', 'Min', 'Average']) +
        { fieldConfig+: { defaults+: { unit: 'short' } } },
      )
      .addPanel(
        $.timeseriesPanel('Bucket indexes load / sec') +
        $.successFailurePanel(
          'sum(rate(cortex_bucket_index_loads_total{%s}[$__rate_interval])) - sum(rate(cortex_bucket_index_load_failures_total{%s}[$__rate_interval]))' % [$.jobMatcher($._config.job_names.querier), $.jobMatcher($._config.job_names.querier)],
          'sum(rate(cortex_bucket_index_load_failures_total{%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.querier),
        ) +
        $.stack
      )
      .addPanel(
        $.timeseriesPanel('Bucket indexes load latency') +
        $.latencyPanel('cortex_bucket_index_load_duration_seconds', '{%s}' % $.jobMatcher($._config.job_names.querier)),
      )
    )
    .addRow(
      $.row('Store-gateway')
      .addPanel(
        $.timeseriesPanel('Blocks queried / sec') +
        $.queryPanel('sum(rate(cortex_bucket_store_series_blocks_queried_sum{component="store-gateway",%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.store_gateway), 'blocks') +
        { fieldConfig+: { defaults+: { unit: 'ops' } } },
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
