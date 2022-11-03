(import 'alerts-utils.libsonnet') {
  local anyEnabled(components) = (
    // NOTE(jhesketh): std.any is only available in jsonnet > 0.19. Instead, use .count for now
    if std.count([(component['enabled']) for component in components], true) > 0 then (
      true
    ) else (
      false
    )
  ),
  groups+: if !anyEnabled($._config.autoscaling) then [] else [
    {
      name: 'mimir_autoscaling',
      rules: [
        {
          alert: $.alertName('MimirAutoscalerNotActive'),
          'for': '1h',
          expr: |||
            kube_horizontalpodautoscaler_status_condition{horizontalpodautoscaler=~"%(hpa_name)s",condition="ScalingActive",status="false"}
            * on(%(aggregation_labels)s) group_left max by(%(aggregation_labels)s) (cortex_build_info)
            > 0
          ||| % {
            aggregation_labels: $._config.alert_aggregation_labels,
            hpa_name: component['hpa_name'],
          },
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: 'The Horizontal Pod Autoscaler (HPA) {{ $labels.horizontalpodautoscaler }} in {{ $labels.namespace }} is not active.' % $._config,
          },
        } for component in $._config.autoscaling if component['enabled'] == true
      ],
    },
  ],
}
