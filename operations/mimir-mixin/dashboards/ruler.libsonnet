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
            |||
              kube_horizontalpodautoscaler_spec_max_replicas{%(namespace_matcher)s, horizontalpodautoscaler=~"%(hpa_name)s"}
              # Add the scaletargetref_name label which is more readable than "kube-hpa-..."
              + on (%(cluster_labels)s, horizontalpodautoscaler) group_left (scaletargetref_name)
                0*kube_horizontalpodautoscaler_info{%(namespace_matcher)s, horizontalpodautoscaler=~"%(hpa_name)s"}
            ||| % {
              namespace_matcher: $.namespaceMatcher(),
              cluster_labels: std.join(', ', $._config.cluster_labels),
              hpa_name: $._config.autoscaling.ruler.hpa_name,
            },
            |||
              kube_horizontalpodautoscaler_status_current_replicas{%(namespace_matcher)s, horizontalpodautoscaler=~"%(hpa_name)s"}
              # HPA doesn't go to 0 replicas, so we multiply by 0 if the HPA is not active.
              * on (%(cluster_labels)s, horizontalpodautoscaler)
                kube_horizontalpodautoscaler_status_condition{%(namespace_matcher)s, horizontalpodautoscaler=~"%(hpa_name)s", condition="ScalingActive", status="true"}
              # Add the scaletargetref_name label which is more readable than "kube-hpa-..."
              + on (%(cluster_labels)s, horizontalpodautoscaler) group_left (scaletargetref_name)
                0*kube_horizontalpodautoscaler_info{%(namespace_matcher)s, horizontalpodautoscaler=~"%(hpa_name)s"}
            ||| % {
              namespace_matcher: $.namespaceMatcher(),
              cluster_labels: std.join(', ', $._config.cluster_labels),
              hpa_name: $._config.autoscaling.ruler.hpa_name,
            },
          ],
          [
            'Max {{ scaletargetref_name }}',
            'Current {{ scaletargetref_name }}',
          ],
        ) +
        $.panelDescription(
          title,
          |||
            The maximum, and current number of ruler replicas.
            Please note that the current number of replicas can still show 1 replica even when scaled to 0.
            Since HPA never reports 0 replicas, the query will report 0 only if the HPA is not active.
          |||
        ) +
        {
          seriesOverrides+: [
            {
              alias: '/Max .+/',
              dashes: true,
              fill: 0,
            },
            {
              alias: '/Current .+/',
              fill: 0,
            },
          ],
        }
      )
      .addPanel(
        local title = 'Scaling metric (desired replicas)';
        $.panel(title) +
        $.queryPanel(
          [
            |||
              keda_metrics_adapter_scaler_metrics_value
              /
              on(metric) group_left
              label_replace(
                  kube_horizontalpodautoscaler_spec_target_metric{%s, horizontalpodautoscaler=~"%s"},
                  "metric", "$1", "metric_name", "(.+)"
              )
            ||| % [$.namespaceMatcher(), $._config.autoscaling.ruler.hpa_name],
          ], [
            '{{ scaledObject }}',
          ]
        ) +
        $.panelDescription(
          title,
          |||
            This panel shows the result scaling metric exposed by KEDA divided by the target/threshold used.
            It should represent the desired number of replicas, ignoring the min/max constraints which are applied later.
          |||
        ) +
        $.panelAxisPlacement('Target per replica', 'right'),
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
