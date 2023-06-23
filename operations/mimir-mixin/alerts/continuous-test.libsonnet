(import 'alerts-utils.libsonnet') {
  local alertGroups = [
    {
      name: 'mimir_continuous_test',
      rules: [
        {
          // Alert if Mimir continuous test is not effectively running because writes are failing.
          // This alert tolerates short failures, due to temporarily outages in the Mimir cluster.
          alert: $.alertName('ContinuousTestNotRunningOnWrites'),
          'for': '1h',
          expr: |||
            sum by(%(alert_aggregation_labels)s, test) (rate(mimir_continuous_test_writes_failed_total[5m])) > 0
          ||| % $._config,
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: '%(product)s continuous test {{ $labels.test }} in %(alert_aggregation_variables)s is not effectively running because writes are failing.' % $._config,
          },
        },
        {
          // Alert if Mimir continuous test is not effectively running because queries are failing.
          // This alert tolerates short failures, due to temporarily outages in the Mimir cluster.
          alert: $.alertName('ContinuousTestNotRunningOnReads'),
          'for': '1h',
          expr: |||
            sum by(%(alert_aggregation_labels)s, test) (rate(mimir_continuous_test_queries_failed_total[5m])) > 0
          ||| % $._config,
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: '%(product)s continuous test {{ $labels.test }} in %(alert_aggregation_variables)s is not effectively running because queries are failing.' % $._config,
          },
        },
        {
          // Alert if Mimir continuous test failed when asserting results. This alert has no "for" duration because it
          // should have no "grace period" and alert as soon as the test fails.
          alert: $.alertName('ContinuousTestFailed'),
          expr: |||
            sum by(%(alert_aggregation_labels)s, test) (rate(mimir_continuous_test_query_result_checks_failed_total[10m])) > 0
          ||| % $._config,
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: '%(product)s continuous test {{ $labels.test }} in %(alert_aggregation_variables)s failed when asserting query results.' % $._config,
          },
        },
      ],
    },
  ],

  groups+: $.withRunbookURL('https://grafana.com/docs/mimir/latest/operators-guide/mimir-runbooks/#%s', alertGroups),
}
