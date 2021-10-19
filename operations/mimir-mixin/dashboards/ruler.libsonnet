local utils = import 'mixin-utils/utils.libsonnet';

(import 'dashboard-utils.libsonnet') {
  local ruler_config_api_routes_re = 'api_prom_rules.*|api_prom_api_v1_(rules|alerts)',

  rulerQueries+:: {
    ruleEvaluations: {
      success:
        |||
          sum(rate(cortex_prometheus_rule_evaluations_total{%s}[$__rate_interval]))
          -
          sum(rate(cortex_prometheus_rule_evaluation_failures_total{%s}[$__rate_interval]))
        |||,
      failure: 'sum(rate(cortex_prometheus_rule_evaluation_failures_total{%s}[$__rate_interval]))',
      latency:
        |||
          sum (rate(cortex_prometheus_rule_evaluation_duration_seconds_sum{%s}[$__rate_interval]))
            /
          sum (rate(cortex_prometheus_rule_evaluation_duration_seconds_count{%s}[$__rate_interval]))
        |||,
    },
    perUserPerGroupEvaluations: {
      failure: 'sum by(rule_group) (rate(cortex_prometheus_rule_evaluation_failures_total{%s}[$__rate_interval])) > 0',
      latency:
        |||
          sum by(user) (rate(cortex_prometheus_rule_evaluation_duration_seconds_sum{%s}[$__rate_interval]))
            /
          sum by(user) (rate(cortex_prometheus_rule_evaluation_duration_seconds_count{%s}[$__rate_interval]))
        |||,
    },
    groupEvaluations: {
      missedIterations: 'sum by(user) (rate(cortex_prometheus_rule_group_iterations_missed_total{%s}[$__rate_interval])) > 0',
      latency:
        |||
          rate(cortex_prometheus_rule_group_duration_seconds_sum{%s}[$__rate_interval])
            /
          rate(cortex_prometheus_rule_group_duration_seconds_count{%s}[$__rate_interval])
        |||,
    },
    notifications: {
      failure:
        |||
          sum by(user) (rate(cortex_prometheus_notifications_errors_total{%s}[$__rate_interval]))
            /
          sum by(user) (rate(cortex_prometheus_notifications_sent_total{%s}[$__rate_interval]))
          > 0
        |||,
      queue:
        |||
          sum by(user) (rate(cortex_prometheus_notifications_queue_length{%s}[$__rate_interval]))
            /
          sum by(user) (rate(cortex_prometheus_notifications_queue_capacity{%s}[$__rate_interval])) > 0
        |||,
      dropped:
        |||
          sum by (user) (increase(cortex_prometheus_notifications_dropped_total{%s}[$__rate_interval])) > 0
        |||,
    },
  },

  'ruler.json':
    ($.dashboard('Cortex / Ruler') + { uid: '44d12bcb1f95661c6ab6bc946dfc3473' })
    .addClusterSelectorTemplates()
    .addRow(
      ($.row('Headlines') + {
         height: '100px',
         showTitle: false,
       })
      .addPanel(
        $.panel('Active Configurations') +
        $.statPanel('sum(cortex_ruler_managers_total{%s})' % $.jobMatcher('ruler'), format='short')
      )
      .addPanel(
        $.panel('Total Rules') +
        $.statPanel('sum(cortex_prometheus_rule_group_rules{%s})' % $.jobMatcher('ruler'), format='short')
      )
      .addPanel(
        $.panel('Read from Ingesters - QPS') +
        $.statPanel('sum(rate(cortex_ingester_client_request_duration_seconds_count{%s, operation="/cortex.Ingester/QueryStream"}[5m]))' % $.jobMatcher('ruler'), format='reqps')
      )
      .addPanel(
        $.panel('Write to Ingesters - QPS') +
        $.statPanel('sum(rate(cortex_ingester_client_request_duration_seconds_count{%s, operation="/cortex.Ingester/Push"}[5m]))' % $.jobMatcher('ruler'), format='reqps')
      )
    )
    .addRow(
      $.row('Rule Evaluations Global')
      .addPanel(
        $.panel('EPS') +
        $.queryPanel(
          [
            $.rulerQueries.ruleEvaluations.success % [$.jobMatcher('ruler'), $.jobMatcher('ruler')],
            $.rulerQueries.ruleEvaluations.failure % $.jobMatcher('ruler'),
          ],
          ['success', 'failed'],
        ),
      )
      .addPanel(
        $.panel('Latency') +
        $.queryPanel(
          $.rulerQueries.ruleEvaluations.latency % [$.jobMatcher('ruler'), $.jobMatcher('ruler')],
          'average'
        ),
      )
    )
    .addRow(
      $.row('Configuration API (gateway)')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_request_duration_seconds_count{%s, route=~"%s"}' % [$.jobMatcher($._config.job_names.gateway), ruler_config_api_routes_re])
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector($._config.job_names.gateway) + [utils.selector.re('route', ruler_config_api_routes_re)])
      )
      .addPanel(
        $.panel('Per route p99 Latency') +
        $.queryPanel(
          'histogram_quantile(0.99, sum by (route, le) (cluster_job_route:cortex_request_duration_seconds_bucket:sum_rate{%s, route=~"%s"}))' % [$.jobMatcher($._config.job_names.gateway), ruler_config_api_routes_re],
          '{{ route }}'
        ) +
        { yaxes: $.yaxes('s') }
      )
    )
    .addRow(
      $.row('Writes (Ingesters)')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_ingester_client_request_duration_seconds_count{%s, operation="/cortex.Ingester/Push"}' % $.jobMatcher('ruler'))
      )
      .addPanel(
        $.panel('Latency') +
        $.latencyPanel('cortex_ingester_client_request_duration_seconds', '{%s, operation="/cortex.Ingester/Push"}' % $.jobMatcher('ruler'))
      )
    )
    .addRow(
      $.row('Reads (Ingesters)')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_ingester_client_request_duration_seconds_count{%s, operation="/cortex.Ingester/QueryStream"}' % $.jobMatcher('ruler'))
      )
      .addPanel(
        $.panel('Latency') +
        $.latencyPanel('cortex_ingester_client_request_duration_seconds', '{%s, operation="/cortex.Ingester/QueryStream"}' % $.jobMatcher('ruler'))
      )
    )
    .addRowIf(
      std.member($._config.storage_engine, 'chunks'),
      $.row('Ruler - Chunks storage - Index Cache')
      .addPanel(
        $.panel('Total entries') +
        $.queryPanel('sum(querier_cache_added_new_total{cache="store.index-cache-read.fifocache",%s}) - sum(querier_cache_evicted_total{cache="store.index-cache-read.fifocache",%s})' % [$.jobMatcher($._config.job_names.ruler), $.jobMatcher($._config.job_names.ruler)], 'Entries'),
      )
      .addPanel(
        $.panel('Cache Hit %') +
        $.queryPanel('(sum(rate(querier_cache_gets_total{cache="store.index-cache-read.fifocache",%s}[1m])) - sum(rate(querier_cache_misses_total{cache="store.index-cache-read.fifocache",%s}[1m]))) / sum(rate(querier_cache_gets_total{cache="store.index-cache-read.fifocache",%s}[1m]))' % [$.jobMatcher($._config.job_names.ruler), $.jobMatcher($._config.job_names.ruler), $.jobMatcher($._config.job_names.ruler)], 'hit rate')
        { yaxes: $.yaxes({ format: 'percentunit', max: 1 }) },
      )
      .addPanel(
        $.panel('Churn Rate') +
        $.queryPanel('sum(rate(querier_cache_evicted_total{cache="store.index-cache-read.fifocache",%s}[1m]))' % $.jobMatcher($._config.job_names.ruler), 'churn rate'),
      )
    )
    .addRowIf(
      std.member($._config.storage_engine, 'chunks'),
      $.row('Ruler - Chunks storage - Store')
      .addPanel(
        $.panel('Index Lookups per Query') +
        utils.latencyRecordingRulePanel('cortex_chunk_store_index_lookups_per_query', $.jobSelector($._config.job_names.ruler), multiplier=1) +
        { yaxes: $.yaxes('short') },
      )
      .addPanel(
        $.panel('Series (pre-intersection) per Query') +
        utils.latencyRecordingRulePanel('cortex_chunk_store_series_pre_intersection_per_query', $.jobSelector($._config.job_names.ruler), multiplier=1) +
        { yaxes: $.yaxes('short') },
      )
      .addPanel(
        $.panel('Series (post-intersection) per Query') +
        utils.latencyRecordingRulePanel('cortex_chunk_store_series_post_intersection_per_query', $.jobSelector($._config.job_names.ruler), multiplier=1) +
        { yaxes: $.yaxes('short') },
      )
      .addPanel(
        $.panel('Chunks per Query') +
        utils.latencyRecordingRulePanel('cortex_chunk_store_chunks_per_query', $.jobSelector($._config.job_names.ruler), multiplier=1) +
        { yaxes: $.yaxes('short') },
      )
    )
    .addRowIf(
      std.member($._config.storage_engine, 'blocks'),
      $.row('Ruler - Blocks storage')
      .addPanel(
        $.panel('Number of store-gateways hit per Query') +
        $.latencyPanel('cortex_querier_storegateway_instances_hit_per_query', '{%s}' % $.jobMatcher($._config.job_names.ruler), multiplier=1) +
        { yaxes: $.yaxes('short') },
      )
      .addPanel(
        $.panel('Refetches of missing blocks per Query') +
        $.latencyPanel('cortex_querier_storegateway_refetches_per_query', '{%s}' % $.jobMatcher($._config.job_names.ruler), multiplier=1) +
        { yaxes: $.yaxes('short') },
      )
      .addPanel(
        $.panel('Consistency checks failed') +
        $.queryPanel('sum(rate(cortex_querier_blocks_consistency_checks_failed_total{%s}[1m])) / sum(rate(cortex_querier_blocks_consistency_checks_total{%s}[1m]))' % [$.jobMatcher($._config.job_names.ruler), $.jobMatcher($._config.job_names.ruler)], 'Failure Rate') +
        { yaxes: $.yaxes({ format: 'percentunit', max: 1 }) },
      )
    )
    .addRow(
      $.row('Notifications')
      .addPanel(
        $.panel('Delivery Errors') +
        $.queryPanel($.rulerQueries.notifications.failure % [$.jobMatcher('ruler'), $.jobMatcher('ruler')], '{{ user }}')
      )
      .addPanel(
        $.panel('Queue Length') +
        $.queryPanel($.rulerQueries.notifications.queue % [$.jobMatcher('ruler'), $.jobMatcher('ruler')], '{{ user }}')
      )
      .addPanel(
        $.panel('Dropped') +
        $.queryPanel($.rulerQueries.notifications.dropped % $.jobMatcher('ruler'), '{{ user }}')
      )
    )
    .addRow(
      ($.row('Group Evaluations') + { collapse: true })
      .addPanel(
        $.panel('Missed Iterations') +
        $.queryPanel($.rulerQueries.groupEvaluations.missedIterations % $.jobMatcher('ruler'), '{{ user }}'),
      )
      .addPanel(
        $.panel('Latency') +
        $.queryPanel(
          $.rulerQueries.groupEvaluations.latency % [$.jobMatcher('ruler'), $.jobMatcher('ruler')],
          '{{ user }}'
        ),
      )
      .addPanel(
        $.panel('Failures') +
        $.queryPanel(
          $.rulerQueries.perUserPerGroupEvaluations.failure % [$.jobMatcher('ruler')], '{{ rule_group }}'
        )
      )
    )
    .addRow(
      ($.row('Rule Evaluation per User') + { collapse: true })
      .addPanel(
        $.panel('Latency') +
        $.queryPanel(
          $.rulerQueries.perUserPerGroupEvaluations.latency % [$.jobMatcher('ruler'), $.jobMatcher('ruler')],
          '{{ user }}'
        )
      )
    )
    .addRows(
      $.getObjectStoreRows('Ruler Configuration Object Store (Ruler accesses)', 'ruler-storage')
    ),
}
