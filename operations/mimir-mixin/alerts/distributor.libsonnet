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
            (quantile by (%(alert_aggregation_labels)s) (0.9, sum by (%(alert_aggregation_labels)s, %(per_instance_label)s) (rate(go_cpu_classes_gc_total_cpu_seconds_total{container="distributor"}[%(range_interval)s]))
              /
              (
                sum by (%(alert_aggregation_labels)s, %(per_instance_label)s) (rate(go_cpu_classes_total_cpu_seconds_total{container="distributor"}[%(range_interval)s]))
                -
                sum by (%(alert_aggregation_labels)s, %(per_instance_label)s) (rate(go_cpu_classes_idle_cpu_seconds_total{container="distributor"}[%(range_interval)s]))
              )
            ) * 100) > %(distributor_gc_cpu_threshold)s

            # Alert only for namespaces with Mimir clusters.
            and (count by (%(alert_aggregation_labels)s) (mimir_build_info) > 0)
          ||| % $._config {
            range_interval: $.alertRangeInterval(5),
          },
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: '%(product)s distributors in %(alert_aggregation_variables)s GC CPU utilization is too high.' % $._config,
          },
        },
        {
          // Alert if HA tracker elected replica changes too often.
          alert: $.alertName('HATrackerElectedReplicaChangesTooOften'),
          'for': '10m',
          expr: |||
            sum by (%(alert_aggregation_labels)s, user) (rate(cortex_ha_tracker_elected_replica_changes_total[1m])) 
            >
            5 * max_over_time(sum by (%(alert_aggregation_labels)s, user) (rate(cortex_ha_tracker_elected_replica_changes_total[1m] offset 24h))[30m:])
          ||| % $._config,
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: '%(product)s HA Tracker for tenant {{ $labels.user }} in %(alert_aggregation_variables)s is changing its elected replica too often.' % $._config,
          },
        },
      ],
    },
  ],

  groups+: $.withRunbookURL('https://grafana.com/docs/mimir/latest/operators-guide/mimir-runbooks/#%s', $.withExtraLabelsAnnotations(alertGroups)),
}
