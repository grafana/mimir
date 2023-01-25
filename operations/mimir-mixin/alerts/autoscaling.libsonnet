{
  local alertGroups = [
    {
      name: 'mimir_autoscaling',
      rules: [
        {
          alert: $.alertName('AutoscalerNotActive'),
          'for': '1h',
          expr: |||
            (
                kube_horizontalpodautoscaler_status_condition{condition="ScalingActive",status="false"}
                # Match only Mimir namespaces.
                * on(%(aggregation_labels)s) group_left max by(%(aggregation_labels)s) (cortex_build_info)
                # Add "metric" label.
                + on(%(aggregation_labels)s, horizontalpodautoscaler) group_right label_replace(kube_horizontalpodautoscaler_spec_target_metric*0, "metric", "$1", "metric_name", "(.+)")
                > 0
            )
            # Alert only if the scaling metric exists and is > 0. If the KEDA ScaledObject is configured to scale down 0,
            # then HPA ScalingActive may be false when expected to run 0 replicas. In this case, the scaling metric exported
            # by KEDA could not exist at all or being exposed with a value of 0.
            and on (%(aggregation_labels)s, metric)
            (label_replace(keda_metrics_adapter_scaler_metrics_value, "namespace", "$0", "exported_namespace", ".+") > 0)
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
        {
          alert: $.alertName('AutoscalerKedaFailing'),
          'for': '1h',
          expr: |||
            (
                # Find KEDA scalers reporting errors.
                label_replace(rate(keda_metrics_adapter_scaler_errors[5m]), "namespace", "$1", "exported_namespace", "(.*)")
                # Match only Mimir namespaces.
                * on(%(aggregation_labels)s) group_left max by(%(aggregation_labels)s) (cortex_build_info)
            )
            > 0
          ||| % {
            aggregation_labels: $._config.alert_aggregation_labels,
          },
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: 'The Keda ScaledObject {{ $labels.scaledObject }} in {{ $labels.namespace }} is experiencing errors.',
          },
        },
      ],
    },
  ],

  groups+: $.withRunbookURL('https://grafana.com/docs/mimir/latest/operators-guide/mimir-runbooks/#%s', alertGroups),
}
