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
    groupEvaluations: {
      missedIterations: 'sum(rate(cortex_prometheus_rule_group_iterations_missed_total{%s}[$__interval]))',
      latency:
        |||
          sum (rate(cortex_prometheus_rule_group_duration_seconds_sum{%s}[$__interval]))
            /
          sum (rate(cortex_prometheus_rule_group_duration_seconds_count{%s}[$__interval]))
        |||,
    },
  },

  'ruler.json':
    $.dashboard('Cortex / Ruler')
    .addClusterSelectorTemplates()
    .addRow(
      $.row('Rule Evaluations')
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
      $.row('Group Evaluations')
      .addPanel(
        $.panel('Missed Iterations') +
        $.queryPanel($.rulerQueries.groupEvaluations.missedIterations % $.jobMatcher('ruler'), 'iterations missed'),
      )
      .addPanel(
        $.panel('Latency') +
        $.queryPanel(
          $.rulerQueries.groupEvaluations.latency % [$.jobMatcher('ruler'), $.jobMatcher('ruler')],
          'average'
        ),
      )
    ),
}
