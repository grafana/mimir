{
  local makePrefix(groups) = std.join('_', groups),
  local makeGroupBy(groups) = std.join(', ', groups),

  local group_by_cluster = makeGroupBy($._config.cluster_labels),

  _group_config+:: {
    // Each group prefix is composed of `_`-separated labels
    group_prefix_jobs: makePrefix($._config.job_labels),
    group_prefix_clusters: makePrefix($._config.cluster_labels),

    // Each group-by label list is `, `-separated and unique identifies
    group_by_job: makeGroupBy($._config.job_labels),
    group_by_cluster: group_by_cluster,
  },

  // The following works around the deprecation of `$._config.alert_aggregation_labels`
  // - If an override of that value is detected, a warning will be printed
  // - If no override was detected, it will be set to the `group_by_cluster` value,
  //   which will replace it altogether in the future.
  local alert_aggregation_labels_override = (
    {
      alert_aggregation_labels: null,
    } + super._config
  ).alert_aggregation_labels,

  _config+:: {
    alert_aggregation_labels:
      if alert_aggregation_labels_override != null
      then std.trace(
        |||
          Deprecated: _config.alert_aggregation_labels
            This field has been explicitly overridden to "%s".
            Instead, express the override in terms of _config.cluster_labels.
              E.g., cluster_labels: %s will automatically convert to "%s".
        ||| % [
          alert_aggregation_labels_override,
          $._config.cluster_labels,
          group_by_cluster,
        ],
        alert_aggregation_labels_override
      )
      else group_by_cluster,

    // This field contains contains the Prometheus template variables that should
    // be used to display values of the configured "group_by_cluster" (or the
    // deprecated "alert_aggregation_labels").
    alert_aggregation_variables:
      std.join(
        '/',
        // Generate the variable replacement for each label.
        std.map(
          function(l) '{{ $labels.%s }}' % l,
          // Split the configured labels by comma and remove whitespaces.
          std.map(
            function(l) std.strReplace(l, ' ', ''),
            std.split($._config.alert_aggregation_labels, ',')
          ),
        ),
      ),
    alert_aggregation_rule_prefix:
      std.join(
        '_',
        // Split the configured labels by comma and remove whitespaces.
        std.map(
          function(l) std.strReplace(l, ' ', ''),
          std.split($._config.alert_aggregation_labels, ',')
        ),
      ),
  },
}
