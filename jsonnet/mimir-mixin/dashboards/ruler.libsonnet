local utils = import 'mixin-utils/utils.libsonnet';

(import 'dashboard-utils.libsonnet') {

  rulerQueries+:: {
    ruleEvaluations: {
      success:
        |||
          sum(rate(cortex_prometheus_rule_evaluations_total{%s}[$__interval]))
          -
          sum(rate(cortex_prometheus_rule_evaluation_failures_total{%s}[$__interval]))
        |||,
      failure: 'sum(rate(cortex_prometheus_rule_evaluation_failures_total{%s}[$__interval]))',
      latency:
        |||
          sum (rate(cortex_prometheus_rule_evaluation_duration_seconds_sum{%s}[$__interval]))
            /
          sum (rate(cortex_prometheus_rule_evaluation_duration_seconds_count{%s}[$__interval]))
        |||,
    },
    perUserPerGroupEvaluations: {
      failure: 'sum by(rule_group) (rate(cortex_prometheus_rule_evaluation_failures_total{%s}[$__interval])) > 0',
      latency:
        |||
          sum by(user) (rate(cortex_prometheus_rule_evaluation_duration_seconds_sum{%s}[$__interval]))
            /
          sum by(user) (rate(cortex_prometheus_rule_evaluation_duration_seconds_count{%s}[$__interval]))
        |||,
    },
    groupEvaluations: {
      missedIterations: 'sum by(user) (rate(cortex_prometheus_rule_group_iterations_missed_total{%s}[$__interval])) > 0',
      latency:
        |||
          rate(cortex_prometheus_rule_group_duration_seconds_sum{%s}[$__interval])
            /
          rate(cortex_prometheus_rule_group_duration_seconds_count{%s}[$__interval])
        |||,
    },
    notifications: {
      failure:
        |||
          sum by(user) (rate(cortex_prometheus_notifications_errors_total{%s}[$__interval]))
            /
          sum by(user) (rate(cortex_prometheus_notifications_sent_total{%s}[$__interval]))
          > 0
        |||,
      queue:
        |||
          sum by(user) (rate(cortex_prometheus_notifications_queue_length{%s}[$__interval]))
            /
          sum by(user) (rate(cortex_prometheus_notifications_queue_capacity{%s}[$__interval]))  
          > 0
        |||,
      dropped:
        |||
          sum by (user) (increase(cortex_prometheus_notifications_dropped_total{%s}[$__interval])) > 0
        |||,
    },
  },

  'ruler.json':
    $.dashboard('Cortex / Ruler')
    .addClusterSelectorTemplates()
    .addRow(
      $.row('Rule Evaluations Global')
      .addPanel(
        $.panel('EPS') +
        $.queryPanel(
          [
            $.rulerQueries.ruleEvaluations.success % [$.jobMatcher('ruler'), $.jobMatcher('ruler')],
            $.rulerQueries.ruleEvaluations.failure % $.jobMatcher('ruler'),
          ],
          ['sucess', 'failed'],
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
      $.row('Gateway Latency')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_request_duration_seconds_count{%s, route=~"api_prom_rules.*|api_prom_api_v1_(rules|alerts)"}' % $.jobMatcher($._config.job_names.gateway))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector($._config.job_names.gateway) + [utils.selector.re('route', 'api_prom_rules.*|api_prom_api_v1_(rules|alerts)')])
      )
    )
    .addRow(
      $.row('Group Evaluations')
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
      $.row('Rule Evaluation per User')
      .addPanel(
        $.panel('Latency') +
        $.queryPanel(
          $.rulerQueries.perUserPerGroupEvaluations.latency % [$.jobMatcher('ruler'), $.jobMatcher('ruler')],
          '{{ user }}'
        )
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
    ),
}
