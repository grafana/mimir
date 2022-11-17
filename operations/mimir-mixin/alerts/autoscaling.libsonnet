{
  local alertGroups = [
    {
      name: 'mimir_autoscaling',
      rules: [
        {
          alert: $.alertName('AutoscalerNotActive'),
          'for': '1h',
          expr: |||
            kube_horizontalpodautoscaler_status_condition{condition="ScalingActive",status="false"}
            * on(%(aggregation_labels)s) group_left max by(%(aggregation_labels)s) (cortex_build_info)
            > 0
          ||| % {
            aggregation_labels: $._config.alert_aggregation_labels,
          },
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: 'The Horizontal Pod Autoscaler (HPA) {{ $labels.horizontalpodautoscaler }} in {{ $labels.namespace }} is not active.' % $._config,
          },
        },
      ],
    },
  ],

  groups+: $.withRunbookURL('https://grafana.com/docs/mimir/latest/operators-guide/mimir-runbooks/#%s', alertGroups),
}
