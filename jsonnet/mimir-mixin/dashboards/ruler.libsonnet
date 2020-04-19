local utils = import 'mixin-utils/utils.libsonnet';

(import 'dashboard-utils.libsonnet') {

  'ruler.json':
    $.dashboard('Cortex / Ruler')
    .addClusterSelectorTemplates()
    .addRow(
      $.row('Rule Evaluations')
      .addPanel(
        $.panel('EPS') +
        $.queryPanel('sum(rate(cortex_prometheus_rule_evaluations_total{cluster=~"$cluster", job=~"($namespace)/ruler"}[$__interval]))', 'rules processed'),
      )
      .addPanel(
        $.panel('Latency') +
        $.queryPanel(
          |||
            sum (rate(cortex_prometheus_rule_evaluation_duration_seconds_sum{cluster=~"$cluster", job=~"($namespace)/ruler"}[$__interval]))
              /
            sum (rate(cortex_prometheus_rule_evaluation_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/ruler"}[$__interval]))
          |||, 'average'
        ),
      )
    )
    .addRow(
      $.row('Group Evaluations')
      .addPanel(
        $.panel('Missed Iterations') +
        $.queryPanel('sum(rate(cortex_prometheus_rule_group_iterations_missed_total{cluster=~"$cluster", job=~"($namespace)/ruler"}[$__interval]))', 'iterations missed'),
      )
      .addPanel(
        $.panel('Latency') +
        $.queryPanel(
          |||
            sum (rate(cortex_prometheus_rule_group_duration_seconds_sum{cluster=~"$cluster", job=~"($namespace)/ruler"}[$__interval]))
              /
            sum (rate(cortex_prometheus_rule_group_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/ruler"}[$__interval]))
          |||, 'average'
        ),
      )
    ),
}
