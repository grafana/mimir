{
  _config+:: {
    alert_cluster_variable: '{{ $labels.%s }}' % $._config.per_cluster_label,
    alert_instance_variable: '{{ $labels.%s }}' % $._config.per_instance_label,
    alert_node_variable: '{{ $labels.%s }}' % $._config.per_cluster_label,
  },

  // The alert name is prefixed with the product name (eg. AlertName -> MimirAlertName).
  alertName(name)::
    $._config.product + name,
}
