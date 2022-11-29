{
  _config+:: {
    alert_cluster_variable: '{{ $labels.%s }}' % $._config.per_cluster_label,
    alert_instance_variable: '{{ $labels.%s }}' % $._config.per_instance_label,
    alert_node_variable: '{{ $labels.%s }}' % $._config.per_cluster_label,
  },

  // The alert name is prefixed with the product name (eg. AlertName -> MimirAlertName).
  alertName(name)::
    $._config.product + name,

  jobMatcher(job)::
    'job=~".*/%s"' % job,

  withRunbookURL(url_format, groups)::
    local update_rule(rule) =
      if std.objectHas(rule, 'alert')
      then rule {
        annotations+: {
          runbook_url: url_format % std.asciiLower(rule.alert),
        },
      }
      else rule;
    [
      group {
        rules: [
          update_rule(alert)
          for alert in group.rules
        ],
      }
      for group in groups
    ],
}
