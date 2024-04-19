local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-ruler.json';

(import 'dashboard-utils.libsonnet') +
(import 'dashboard-queries.libsonnet') {
  local ruler_config_api_routes_re = '(%s)|(%s)' % [
    // Prometheus API routes which are also exposed by Mimir.
    '(api_prom_api_v1|prometheus_api_v1)_(rules|alerts|status_buildinfo)',
    // Mimir-only API routes used for rule configuration.
    '(api_prom|api_v1|prometheus|prometheus_config_v1)_rules.*',
  ],

  [filename]:
    assert std.md5(filename) == '631e15d5d85afb2ca8e35d62984eeaa0' : 'UID of the dashboard has changed, please update references to dashboard.';
    ($.dashboard('Ruler') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates()
    .addRow(
      ($.row('Headlines') + {
         height: '100px',
         showTitle: false,
       })
      .addPanel(
        $.panel('Active configurations') +
        $.statPanel('sum(cortex_ruler_managers_total{%s})' % $.jobMatcher($._config.job_names.ruler), format='short')
      )
      .addPanel(
        $.panel('Total rules') +
        $.statPanel('sum(cortex_prometheus_rule_group_rules{%s})' % $.jobMatcher($._config.job_names.ruler), format='short')
      )
      .addPanel(
        $.panel('Read from ingesters - QPS') +
        $.statPanel('sum(rate(cortex_ingester_client_request_duration_seconds_count{%s, operation="/cortex.Ingester/QueryStream"}[$__rate_interval]))' % $.jobMatcher($._config.job_names.ruler + $._config.job_names.ruler_querier), format='reqps') +
        $.panelDescription(
          'Read from ingesters - QPS',
          |||
            Note: Even while operating in Remote ruler mode you will still see values for this panel.

            This is because the metrics are inclusive of intermediate services and are showing the requests that ultimately reach the ingesters.

            For a more detailed view of the read path when using remote ruler mode, see the Remote ruler reads dashboard.
          |||
        ),
      )
      .addPanel(
        $.panel('Write to ingesters - QPS') +
        $.statPanel('sum(rate(cortex_ingester_client_request_duration_seconds_count{%s, operation="/cortex.Ingester/Push"}[$__rate_interval]))' % $.jobMatcher($._config.job_names.ruler), format='reqps')
      )
    )
    .addRow(
      $.row('Rule evaluations global')
      .addPanel(
        $.timeseriesPanel('Evaluations per second') +
        $.successFailureCustomPanel(
          [
            $.queries.ruler.evaluations.successPerSecond,
            $.queries.ruler.evaluations.failurePerSecond,
            $.queries.ruler.evaluations.missedIterationsPerSecond,
          ],
          ['success', 'failed', 'missed'],
        ),
      )
      .addPanel(
        $.timeseriesPanel('Latency') +
        $.queryPanel(
          $.queries.ruler.evaluations.latency,
          'average'
        ) +
        { fieldConfig+: { defaults+: { unit: 's' } } },
      )
    )
    .addRowIf(
      $._config.gateway_enabled,
      $.row('Configuration API (gateway)')
      .addPanel(
        $.timeseriesPanel('QPS') +
        $.qpsPanel('cortex_request_duration_seconds_count{%s, route=~"%s"}' % [$.jobMatcher($._config.job_names.gateway), ruler_config_api_routes_re])
      )
      .addPanel(
        $.timeseriesPanel('Latency') +
        $.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector($._config.job_names.gateway) + [utils.selector.re('route', ruler_config_api_routes_re)])
      )
      .addPanel(
        local selectors = $.jobSelector($._config.job_names.gateway) + [utils.selector.re('route', ruler_config_api_routes_re)];
        $.timeseriesPanel('Per route p99 latency') +
        $.queryPanel(
          'histogram_quantile(0.99, sum by (route, le) (%s:cortex_request_duration_seconds_bucket:sum_rate%s))' %
          [$.recordingRulePrefix(selectors), utils.toPrometheusSelector(selectors)],
          '{{ route }}'
        ) +
        { fieldConfig+: { defaults+: { unit: 's' } } },
      )
    )
    .addRow(
      $.row('Writes (ingesters)')
      .addPanel(
        $.timeseriesPanel('QPS') +
        $.qpsPanel('cortex_ingester_client_request_duration_seconds_count{%s, operation="/cortex.Ingester/Push"}' % $.jobMatcher($._config.job_names.ruler))
      )
      .addPanel(
        $.timeseriesPanel('Latency') +
        $.latencyPanel('cortex_ingester_client_request_duration_seconds', '{%s, operation="/cortex.Ingester/Push"}' % $.jobMatcher($._config.job_names.ruler))
      )
    )
    .addRow(
      $.row('Reads (ingesters)')
      .addPanel(
        $.timeseriesPanel('QPS') +
        $.qpsPanel('cortex_ingester_client_request_duration_seconds_count{%s, operation="/cortex.Ingester/QueryStream"}' % $.jobMatcher($._config.job_names.ruler + $._config.job_names.ruler_querier))
      )
      .addPanel(
        $.timeseriesPanel('Latency') +
        $.latencyPanel('cortex_ingester_client_request_duration_seconds', '{%s, operation="/cortex.Ingester/QueryStream"}' % $.jobMatcher($._config.job_names.ruler + $._config.job_names.ruler_querier))
      )
    )
    .addRowIf(
      $._config.autoscaling.ruler.enabled,
      $.cpuAndMemoryBasedAutoScalingRow('Ruler'),
    )
    .addRowIf(
      $._config.autoscaling.ruler_query_frontend.enabled,
      $.cpuAndMemoryBasedAutoScalingRow('Ruler-query-frontend'),
    )
    .addRow(
      $.kvStoreRow('Ruler - key-value store for rulers ring', 'ruler', 'ruler')
    )
    .addRow(
      $.row('Ruler - blocks storage')
      .addPanel(
        $.timeseriesPanel('Number of store-gateways hit per query') +
        $.latencyPanel('cortex_querier_storegateway_instances_hit_per_query', '{%s}' % $.jobMatcher($._config.job_names.ruler), multiplier=1) +
        { fieldConfig+: { defaults+: { unit: 'short' } } },
      )
      .addPanel(
        $.timeseriesPanel('Refetches of missing blocks per query') +
        $.latencyPanel('cortex_querier_storegateway_refetches_per_query', '{%s}' % $.jobMatcher($._config.job_names.ruler), multiplier=1) +
        { fieldConfig+: { defaults+: { unit: 'short' } } },
      )
      .addPanel(
        $.timeseriesPanel('Consistency checks failed') +
        $.failurePanel('sum(rate(cortex_querier_blocks_consistency_checks_failed_total{%s}[$__rate_interval])) / sum(rate(cortex_querier_blocks_consistency_checks_total{%s}[$__rate_interval]))' % [$.jobMatcher($._config.job_names.ruler), $.jobMatcher($._config.job_names.ruler)], 'Failures / sec') +
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
      $.row('Notifications')
      .addPanel(
        $.timeseriesPanel('Delivery errors') +
        $.queryPanel(|||
          sum by(user) (rate(cortex_prometheus_notifications_errors_total{%s}[$__rate_interval]))
            /
          sum by(user) (rate(cortex_prometheus_notifications_sent_total{%s}[$__rate_interval]) > 0)
          > 0
        ||| % [$.jobMatcher($._config.job_names.ruler), $.jobMatcher($._config.job_names.ruler)], '{{ user }}') +
        { fieldConfig+: { defaults+: { unit: 'short', noValue: 0 } } }
      )
      .addPanel(
        $.timeseriesPanel('Queue length') +
        $.queryPanel(|||
          sum by(user) (rate(cortex_prometheus_notifications_queue_length{%s}[$__rate_interval]))
            /
          sum by(user) (rate(cortex_prometheus_notifications_queue_capacity{%s}[$__rate_interval])) > 0
        ||| % [$.jobMatcher($._config.job_names.ruler), $.jobMatcher($._config.job_names.ruler)], '{{ user }}')
        { fieldConfig+: { defaults+: { unit: 'percentunit', min: 0, max: 1 } } },
      )
      .addPanel(
        $.timeseriesPanel('Dropped') +
        $.queryPanel(|||
          sum by (user) (increase(cortex_prometheus_notifications_dropped_total{%s}[$__rate_interval])) > 0
        ||| % $.jobMatcher($._config.job_names.ruler), '{{ user }}') +
        { fieldConfig+: { defaults+: { unit: 'short', noValue: 0 } } }
      )
    )
    .addRow(
      ($.row('Group evaluations') + { collapse: true })
      .addPanel(
        $.timeseriesPanel('Missed iterations') +
        $.queryPanel('sum by(user) (rate(cortex_prometheus_rule_group_iterations_missed_total{%s}[$__rate_interval])) > 0' % $.jobMatcher($._config.job_names.ruler), '{{ user }}'),
      )
      .addPanel(
        $.timeseriesPanel('Latency') +
        $.queryPanel(
          |||
            rate(cortex_prometheus_rule_group_duration_seconds_sum{%s}[$__rate_interval])
              /
            rate(cortex_prometheus_rule_group_duration_seconds_count{%s}[$__rate_interval])
          ||| % [$.jobMatcher($._config.job_names.ruler), $.jobMatcher($._config.job_names.ruler)],
          '{{ user }}'
        ) +
        { fieldConfig+: { defaults+: { unit: 's' } } },
      )
      .addPanel(
        $.timeseriesPanel('Failures') +
        $.queryPanel(
          'sum by(rule_group) (rate(cortex_prometheus_rule_evaluation_failures_total{%s}[$__rate_interval])) > 0' % [$.jobMatcher($._config.job_names.ruler)], '{{ rule_group }}'
        )
      )
    )
    .addRow(
      ($.row('Rule evaluation per user') + { collapse: true })
      .addPanel(
        $.timeseriesPanel('Latency') +
        $.queryPanel(
          |||
            sum by(user) (rate(cortex_prometheus_rule_evaluation_duration_seconds_sum{%s}[$__rate_interval]))
              /
            sum by(user) (rate(cortex_prometheus_rule_evaluation_duration_seconds_count{%s}[$__rate_interval]))
          ||| % [$.jobMatcher($._config.job_names.ruler), $.jobMatcher($._config.job_names.ruler)],
          '{{ user }}'
        ) +
        { fieldConfig+: { defaults+: { unit: 's' } } },
      )
    )
    .addRows(
      $.getObjectStoreRows('Ruler configuration object store (ruler accesses)', 'ruler-storage')
    ),
}
