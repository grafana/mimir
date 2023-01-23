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
        $.statPanel('sum(rate(cortex_ingester_client_request_duration_seconds_count{%s, operation="/cortex.Ingester/QueryStream"}[$__rate_interval]))' % $.jobMatcher($._config.job_names.ruler), format='reqps')
      )
      .addPanel(
        $.panel('Write to ingesters - QPS') +
        $.statPanel('sum(rate(cortex_ingester_client_request_duration_seconds_count{%s, operation="/cortex.Ingester/Push"}[$__rate_interval]))' % $.jobMatcher($._config.job_names.ruler), format='reqps')
      )
    )
    .addRow(
      $.row('Rule evaluations global')
      .addPanel(
        $.panel('Evaluations per second') +
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
        $.panel('Latency') +
        $.queryPanel(
          $.queries.ruler.evaluations.latency,
          'average'
        ) +
        { yaxes: $.yaxes('s') },
      )
    )
    .addRowIf(
      $._config.gateway_enabled,
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
        local selectors = $.jobSelector($._config.job_names.gateway) + [utils.selector.re('route', ruler_config_api_routes_re)];
        $.panel('Per route p99 latency') +
        $.queryPanel(
          'histogram_quantile(0.99, sum by (route, le) (%s:cortex_request_duration_seconds_bucket:sum_rate%s))' %
          [$.recordingRulePrefix(selectors), utils.toPrometheusSelector(selectors)],
          '{{ route }}'
        ) +
        { yaxes: $.yaxes('s') }
      )
    )
    .addRow(
      $.row('Writes (ingesters)')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_ingester_client_request_duration_seconds_count{%s, operation="/cortex.Ingester/Push"}' % $.jobMatcher($._config.job_names.ruler))
      )
      .addPanel(
        $.panel('Latency') +
        $.latencyPanel('cortex_ingester_client_request_duration_seconds', '{%s, operation="/cortex.Ingester/Push"}' % $.jobMatcher($._config.job_names.ruler))
      )
    )
    .addRow(
      $.row('Reads (ingesters)')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_ingester_client_request_duration_seconds_count{%s, operation="/cortex.Ingester/QueryStream"}' % $.jobMatcher($._config.job_names.ruler))
      )
      .addPanel(
        $.panel('Latency') +
        $.latencyPanel('cortex_ingester_client_request_duration_seconds', '{%s, operation="/cortex.Ingester/QueryStream"}' % $.jobMatcher($._config.job_names.ruler))
      )
    )
    .addRowIf(
      $._config.autoscaling.ruler.enabled,
      $.row('Ruler - autoscaling')
      .addPanel(
        local title = 'Replicas';
        $.panel(title) +
        $.queryPanel(
          [
            'kube_horizontalpodautoscaler_spec_min_replicas{%s, horizontalpodautoscaler="%s"}' % [$.namespaceMatcher(), $._config.autoscaling.ruler.hpa_name],
            'kube_horizontalpodautoscaler_spec_max_replicas{%s, horizontalpodautoscaler="%s"}' % [$.namespaceMatcher(), $._config.autoscaling.ruler.hpa_name],
            'kube_horizontalpodautoscaler_status_current_replicas{%s, horizontalpodautoscaler="%s"}' % [$.namespaceMatcher(), $._config.autoscaling.ruler.hpa_name],
          ],
          [
            'Min',
            'Max',
            'Current',
          ],
        ) +
        $.panelDescription(
          title,
          |||
            The minimum, maximum, and current number of ruler replicas.
          |||
        ),
      )
      .addPanel(
        local title = 'Scaling metric (CPU)';
        $.panel(title) +
        $.queryPanel(
          [
            $.filterKedaMetricByHPA('(keda_metrics_adapter_scaler_metrics_value{metric=~".*cpu.*"} / 1000)', $._config.autoscaling.ruler.hpa_name),
            'kube_horizontalpodautoscaler_spec_target_metric{%s, horizontalpodautoscaler="%s", metric_name=~".*cpu.*"} / 1000' % [$.namespaceMatcher(), $._config.autoscaling.ruler.hpa_name],
          ], [
            'Scaling metric',
            'Target per replica',
          ]
        ) +
        $.panelDescription(
          title,
          |||
            This panel shows the result of the query used as scaling metric and target/threshold used.
            The desired number of replicas is computed by HPA as: <scaling metric> / <target per replica>.
          |||
        ) +
        $.panelAxisPlacement('Target per replica', 'right'),
      )
      .addPanel(
        local title = 'Scaling metric (Memory)';
        $.panel(title) +
        $.queryPanel(
          [
            $.filterKedaMetricByHPA('keda_metrics_adapter_scaler_metrics_value{metric=~".*memory.*"}', $._config.autoscaling.ruler.hpa_name),
            'kube_horizontalpodautoscaler_spec_target_metric{%s, horizontalpodautoscaler="%s", metric_name=~".*memory.*"}' % [$.namespaceMatcher(), $._config.autoscaling.ruler.hpa_name],
          ], [
            'Scaling metric',
            'Target per replica',
          ]
        ) +
        $.panelDescription(
          title,
          |||
            This panel shows the result of the query used as scaling metric and target/threshold used.
            The desired number of replicas is computed by HPA as: <scaling metric> / <target per replica>.
          |||
        ) +
        $.panelAxisPlacement('Target per replica', 'right') +
        {
          yaxes: std.mapWithIndex(
            // Set the "bytes" unit to the right axis.
            function(idx, axis) axis + (if idx == 1 then { format: 'bytes' } else {}),
            $.yaxes('bytes')
          ),
        },
      )
      .addPanel(
        local title = 'Autoscaler failures rate';
        $.panel(title) +
        $.queryPanel(
          $.filterKedaMetricByHPA('sum by(metric) (rate(keda_metrics_adapter_scaler_errors[$__rate_interval]))', $._config.autoscaling.ruler.hpa_name),
          '{{metric}} failures'
        ) +
        $.panelDescription(
          title,
          |||
            The rate of failures in the KEDA custom metrics API server. Whenever an error occurs, the KEDA custom
            metrics server is unable to query the scaling metric from Prometheus so the autoscaler woudln't work properly.
          |||
        ),
      )
    )
    .addRow(
      $.kvStoreRow('Ruler - key-value store for rulers ring', 'ruler', 'ruler')
    )
    .addRow(
      $.row('Ruler - blocks storage')
      .addPanel(
        $.panel('Number of store-gateways hit per query') +
        $.latencyPanel('cortex_querier_storegateway_instances_hit_per_query', '{%s}' % $.jobMatcher($._config.job_names.ruler), multiplier=1) +
        { yaxes: $.yaxes('short') },
      )
      .addPanel(
        $.panel('Refetches of missing blocks per query') +
        $.latencyPanel('cortex_querier_storegateway_refetches_per_query', '{%s}' % $.jobMatcher($._config.job_names.ruler), multiplier=1) +
        { yaxes: $.yaxes('short') },
      )
      .addPanel(
        $.panel('Consistency checks failed') +
        $.failurePanel('sum(rate(cortex_querier_blocks_consistency_checks_failed_total{%s}[$__rate_interval])) / sum(rate(cortex_querier_blocks_consistency_checks_total{%s}[$__rate_interval]))' % [$.jobMatcher($._config.job_names.ruler), $.jobMatcher($._config.job_names.ruler)], 'Failures / sec') +
        { yaxes: $.yaxes({ format: 'percentunit', max: 1 }) },
      )
    )
    .addRow(
      $.row('Notifications')
      .addPanel(
        $.panel('Delivery errors') +
        $.queryPanel(|||
          sum by(user) (rate(cortex_prometheus_notifications_errors_total{%s}[$__rate_interval]))
            /
          sum by(user) (rate(cortex_prometheus_notifications_sent_total{%s}[$__rate_interval]))
          > 0
        ||| % [$.jobMatcher($._config.job_names.ruler), $.jobMatcher($._config.job_names.ruler)], '{{ user }}')
      )
      .addPanel(
        $.panel('Queue length') +
        $.queryPanel(|||
          sum by(user) (rate(cortex_prometheus_notifications_queue_length{%s}[$__rate_interval]))
            /
          sum by(user) (rate(cortex_prometheus_notifications_queue_capacity{%s}[$__rate_interval])) > 0
        ||| % [$.jobMatcher($._config.job_names.ruler), $.jobMatcher($._config.job_names.ruler)], '{{ user }}')
      )
      .addPanel(
        $.panel('Dropped') +
        $.queryPanel(|||
          sum by (user) (increase(cortex_prometheus_notifications_dropped_total{%s}[$__rate_interval])) > 0
        ||| % $.jobMatcher($._config.job_names.ruler), '{{ user }}')
      )
    )
    .addRow(
      ($.row('Group evaluations') + { collapse: true })
      .addPanel(
        $.panel('Missed iterations') +
        $.queryPanel('sum by(user) (rate(cortex_prometheus_rule_group_iterations_missed_total{%s}[$__rate_interval])) > 0' % $.jobMatcher($._config.job_names.ruler), '{{ user }}'),
      )
      .addPanel(
        $.panel('Latency') +
        $.queryPanel(
          |||
            rate(cortex_prometheus_rule_group_duration_seconds_sum{%s}[$__rate_interval])
              /
            rate(cortex_prometheus_rule_group_duration_seconds_count{%s}[$__rate_interval])
          ||| % [$.jobMatcher($._config.job_names.ruler), $.jobMatcher($._config.job_names.ruler)],
          '{{ user }}'
        ) +
        { yaxes: $.yaxes('s') },
      )
      .addPanel(
        $.panel('Failures') +
        $.queryPanel(
          'sum by(rule_group) (rate(cortex_prometheus_rule_evaluation_failures_total{%s}[$__rate_interval])) > 0' % [$.jobMatcher($._config.job_names.ruler)], '{{ rule_group }}'
        )
      )
    )
    .addRow(
      ($.row('Rule evaluation per user') + { collapse: true })
      .addPanel(
        $.panel('Latency') +
        $.queryPanel(
          |||
            sum by(user) (rate(cortex_prometheus_rule_evaluation_duration_seconds_sum{%s}[$__rate_interval]))
              /
            sum by(user) (rate(cortex_prometheus_rule_evaluation_duration_seconds_count{%s}[$__rate_interval]))
          ||| % [$.jobMatcher($._config.job_names.ruler), $.jobMatcher($._config.job_names.ruler)],
          '{{ user }}'
        ) +
        { yaxes: $.yaxes('s') }
      )
    )
    .addRows(
      $.getObjectStoreRows('Ruler configuration object store (ruler accesses)', 'ruler-storage')
    ),
}
