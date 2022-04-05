(import 'alerts-utils.libsonnet') {
  groups+: if !$._config.autoscaling.querier_enabled then [] else [
    {
      name: 'mimir_autoscaling_querier',
      rules: [
        {
          alert: $.alertName('QuerierAutoscalerNotActive'),
          'for': '15m',
          expr: |||
            kube_horizontalpodautoscaler_status_condition{horizontalpodautoscaler="%(hpa_name)s",condition="ScalingActive",status="false"}
            * on(%(aggregation_labels)s) group_left max by(%(aggregation_labels)s) (cortex_build_info)
            > 0
          ||| % {
            aggregation_labels: $._config.alert_aggregation_labels,
            hpa_name: $._config.autoscaling.querier_hpa_name,
          },
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: 'The Horizontal Pod Autoscaler (HPA) in {{ $labels.namespace }} is not active.' % $._config,
          },
        },
      ],
    },
  ],
}
