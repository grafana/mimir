(import 'alerts-utils.libsonnet') {
  local alertGroups = [
    {
      name: 'mimir_distributor_alerts',
      rules: [
        {
          // Alert if distributor GC CPU utilization is too high.
          alert: $.alertName('DistributorGcUsesTooMuchCpu'),
          'for': '10m',
          expr: |||
            (quantile(0.9, sum(rate(go_cpu_classes_gc_total_cpu_seconds_total{container="distributor"}[%(range_interval)s]))
              /
              (
                sum(rate(go_cpu_classes_total_cpu_seconds_total{container="distributor"}[%(range_interval)s]))
                -
                sum(rate(go_cpu_classes_idle_cpu_seconds_total{container="distributor"}[%(range_interval)s]))
              )
            ) * 100) > 10
          ||| % {
            range_interval: $.alertRangeInterval(5),
          },
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: '%(product)s Distributor %(alert_instance_variable)s in %(alert_aggregation_variables)s GC CPU utilization is too high.' % $._config,
          },
        },
      ],
    },
  ],

  groups+: $.withRunbookURL('https://grafana.com/docs/mimir/latest/operators-guide/mimir-runbooks/#%s', $.withExtraLabelsAnnotations(alertGroups)),
}
