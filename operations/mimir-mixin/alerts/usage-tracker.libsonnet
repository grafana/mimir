(import 'alerts-utils.libsonnet') {
  local alertGroups = [
    {
      name: 'mimir_usage_tracker_alerts',
      rules: [
        {
          alert: $.alertName('UsageTrackerSnapshotUploadFailing'),
          'for': '15m',
          expr: |||
            sum by(%(alert_aggregation_labels)s, %(per_instance_label)s) (rate(cortex_usage_tracker_snapshot_events_publish_failures_total[%(rate_interval)s])) > 0
          ||| % $._config {
            rate_interval: $.rateInterval('5m'),
          },
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: '%(product)s usage-tracker %(alert_instance_variable)s in %(alert_aggregation_variables)s is failing to upload snapshots.' % $._config,
          },
        },
        {
          alert: $.alertName('UsageTrackerSnapshotDownloadFailing'),
          'for': '15m',
          expr: |||
            sum by(%(alert_aggregation_labels)s, %(per_instance_label)s) (rate(cortex_usage_tracker_snapshot_events_errors_total{error="download"}[%(rate_interval)s])) > 0
          ||| % $._config {
            rate_interval: $.rateInterval('5m'),
          },
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: '%(product)s usage-tracker %(alert_instance_variable)s in %(alert_aggregation_variables)s is failing to download snapshots from object storage.' % $._config,
          },
        },
        {
          alert: $.alertName('UsageTrackerSnapshotLoadFailedAtStartup'),
          // The metric is set to 1 on failure and reset to 0 after 10 minutes, so keep 'for' well within that window.
          'for': '5m',
          expr: |||
            max by(%(alert_aggregation_labels)s, %(per_instance_label)s) (cortex_usage_tracker_snapshot_load_failed_at_startup) > 0
          ||| % $._config,
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: '%(product)s usage-tracker %(alert_instance_variable)s in %(alert_aggregation_variables)s failed to load the snapshot and events within the snapshot interval at startup and started with partial state.' % $._config,
          },
        },
      ],
    },
  ],

  groups+:
    $.withRunbookURL(
      'https://grafana.com/docs/mimir/latest/operators-guide/mimir-runbooks/#%s',
      $.withExtraLabelsAnnotations(alertGroups)
    ),
}
