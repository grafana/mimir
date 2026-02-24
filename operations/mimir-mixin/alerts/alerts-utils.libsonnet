{
  // The alert name is prefixed with the product name (eg. AlertName -> MimirAlertName).
  alertName(name)::
    $._config.alert_product + name,

  jobMatcher(job)::
    '%s=~"%s%s"' % [$._config.per_job_label, $._config.alert_job_prefix, formatJobForQuery(job)],

  jobNotMatcher(job)::
    '%s!~"%s%s"' % [$._config.per_job_label, $._config.alert_job_prefix, formatJobForQuery(job)],

  local formatJobForQuery(job) =
    if std.isArray(job) then '(%s)' % std.join('|', job)
    else if std.isString(job) then job
    else error 'expected job "%s" to be a string or an array, but it is type "%s"' % [job, std.type(job)],

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

  withExtraLabelsAnnotations(groups)::
    local update_rule(rule) =
      if std.objectHas(rule, 'alert')
      then rule {
        annotations+: $._config.alert_extra_annotations,
        labels+: $._config.alert_extra_labels,
      }
      else rule;
    [
      group {
        rules: [
          update_rule(rule)
          for rule in group.rules
        ],
      }
      for group in groups
    ],

  alertRangeInterval(multiple)::
    ($._config.base_alerts_range_interval_minutes * multiple) + 'm',

  histogramLabels(labels, histogram_type, nhcb=false)::
    assert histogram_type == 'native' || histogram_type == 'classic';
    labels { histogram: histogram_type } +
    (if histogram_type == 'native' && nhcb then {
       buckets: 'custom',
     } else {}),

  // Returns the absolute URL of a dashboard for use in alert annotations.
  // Includes query parameters to preserve time range and map alert labels to dashboard variables.
  // The Dashboard UID is dynamically computed from the filename (same as dashboard definitions).
  // Returns null if externalGrafanaURLPrefix is empty (disables dashboard links).
  // Usage: externalDashboardURL('mimir-writes.json')
  externalDashboardURL(filename)::
    if $._config.externalGrafanaURLPrefix != '' then
      local base_url = '%s/d/%s/%s' % [
        $._config.externalGrafanaURLPrefix,
        std.md5(filename),
        std.strReplace(filename, '.json', ''),
      ];
      // Generate var-<label>={{ $labels.<label> }} for each cluster label
      local var_params = std.join(
        '&',
        std.map(
          function(label) 'var-%s={{ $labels.%s }}' % [label, label],
          $._config.cluster_labels
        )
      );
      '%s?%s' % [base_url, var_params]
    else
      null,

  // Returns an object with dashboard_url field if externalGrafanaURLPrefix is configured, empty object otherwise.
  // Use this to conditionally add dashboard_url to alert annotations.
  // Usage: annotations: { message: '...' } + $.dashboardURLAnnotation('mimir-writes.json')
  dashboardURLAnnotation(filename)::
    local url = $.externalDashboardURL(filename);
    if url != null then { dashboard_url: url } else {},

  // Adds a single dashboard URL to all alerts in the groups that don't already have one.
  // This is useful for component-specific alert files where all alerts should link to the same dashboard.
  // Skips adding dashboard_url if externalGrafanaURLPrefix is empty.
  // Usage: withDashboardURL('mimir-alertmanager.json', alertGroups)
  withDashboardURL(dashboard_filename, groups)::
    local dashboard_url = $.externalDashboardURL(dashboard_filename);
    local update_rule(rule) =
      if std.objectHas(rule, 'alert') && !std.objectHas(rule.annotations, 'dashboard_url') && dashboard_url != null
      then rule {
        annotations+: {
          dashboard_url: dashboard_url,
        },
      }
      else rule;

    [
      group {
        rules: [
          update_rule(rule)
          for rule in group.rules
        ],
      }
      for group in groups
    ],
}
