(import 'alerts-utils.libsonnet') {
  local alertGroups = [
    {
      name: 'mimir_compactor_alerts',
      rules: [
        {
          // Alert if the compactor has not successfully cleaned up blocks in the last 6h.
          alert: $.alertName('CompactorNotCleaningUpBlocks'),
          'for': '1h',
          expr: |||
            # The "last successful run" metric is updated even if the compactor owns no tenants,
            # so this alert correctly doesn't fire if compactor has nothing to do.
            (time() - cortex_compactor_block_cleanup_last_successful_run_timestamp_seconds > 60 * 60 * 6)
          |||,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: '%(product)s Compactor %(alert_instance_variable)s in %(alert_aggregation_variables)s has not successfully cleaned up blocks in the last 6 hours.' % $._config,
          },
        },
      ] + [
        // Alert if the compactor has not successfully run compaction in the last X hours.
        {
          alert: $.alertName('CompactorNotRunningCompaction'),
          'for': '15m',
          expr: |||
            # The "last successful run" metric is updated even if the compactor owns no tenants,
            # so this alert correctly doesn't fire if compactor has nothing to do.
            (time() - max by(%(alert_aggregation_labels)s, %(per_instance_label)s) (cortex_compactor_last_successful_run_timestamp_seconds) > 60 * 60 * %(threshold_hours)d)
            and
            (max by(%(alert_aggregation_labels)s, %(per_instance_label)s) (cortex_compactor_last_successful_run_timestamp_seconds) > 0)
          ||| % $._config { threshold_hours: alert.threshold_hours },
          labels: {
            severity: alert.severity,
            reason: 'in-last-%dh' % alert.threshold_hours,
          },
          annotations: {
            message: '%(product)s Compactor %(alert_instance_variable)s in %(alert_aggregation_variables)s has not run compaction in the last %(threshold_hours)d hours.' % $._config { threshold_hours: alert.threshold_hours },
          },
        }
        for alert in [
          { severity: 'warning', threshold_hours: 6 },
          { severity: 'critical', threshold_hours: 24 },
        ]
      ] + [
        // Alert if the compactor has not successfully run compaction since startup.
        {
          alert: $.alertName('CompactorNotRunningCompaction'),
          'for': alert.for_duration,
          expr: |||
            # The "last successful run" metric is updated even if the compactor owns no tenants,
            # so this alert correctly doesn't fire if compactor has nothing to do.
            max by(%(alert_aggregation_labels)s, %(per_instance_label)s) (cortex_compactor_last_successful_run_timestamp_seconds) == 0
          ||| % $._config,
          labels: {
            severity: alert.severity,
            reason: 'since-startup',
          },
          annotations: {
            message: '%(product)s Compactor %(alert_instance_variable)s in %(alert_aggregation_variables)s has not run compaction since startup.' % $._config,
          },
        }
        for alert in [
          { severity: 'warning', for_duration: '6h' },
          { severity: 'critical', for_duration: '12h' },
        ]
      ] + [
        {
          // Alert if compactor has sustained failing compactions (excluding shutdowns) in the last 10 minutes.
          alert: $.alertName('CompactorNotRunningCompaction'),
          'for': '10m',
          expr: |||
            sum by(%(alert_aggregation_labels)s, %(per_instance_label)s) (increase(cortex_compactor_runs_failed_total{reason!="shutdown"}[%(range_interval)s])) > 0
          ||| %  $._config {
            range_interval: $.alertRangeInterval(5),
          },
          labels: {
            severity: 'critical',
            reason: 'consecutive-failures',
          },
          annotations: {
            message: '%(product)s Compactor %(alert_instance_variable)s in %(alert_aggregation_variables)s failed to run compactions for the last 10m.' % $._config,
          },
        },
        {
          // Alert if compactor ran out of disk space in the last 24h.
          // This is a non-transient condition which requires an operator to look at it even if it happens only once.
          alert: $.alertName('CompactorHasRunOutOfDiskSpace'),
          expr: |||
            increase(cortex_compactor_disk_out_of_space_errors_total{}[24h]) >= 1
          |||,
          labels: {
            severity: 'critical',
            reason: 'non-transient',
          },
          annotations: {
            message: '%(product)s Compactor %(alert_instance_variable)s in %(alert_aggregation_variables)s has run out of disk space.' % $._config,
          },
        },
        {
          // Alert if the compactor has not uploaded anything in the last 24h.
          alert: $.alertName('CompactorHasNotUploadedBlocks'),
          'for': '15m',
          expr: |||
            (time() - (max by(%(alert_aggregation_labels)s, %(per_instance_label)s) (thanos_objstore_bucket_last_successful_upload_time{component="compactor"})) > 60 * 60 * 24)
            and
            (max by(%(alert_aggregation_labels)s, %(per_instance_label)s) (thanos_objstore_bucket_last_successful_upload_time{component="compactor"}) > 0)
            and
            # Only if some compactions have started. We don't want to fire this alert if the compactor has nothing to do.
            (sum by(%(alert_aggregation_labels)s, %(per_instance_label)s) (rate(cortex_compactor_group_compaction_runs_started_total[24h])) > 0)
          ||| % $._config,
          labels: {
            severity: 'critical',
            time_period: '24h',
          },
          annotations: {
            message: '%(product)s Compactor %(alert_instance_variable)s in %(alert_aggregation_variables)s has not uploaded any block in the last 24 hours.' % $._config,
          },
        },
        {
          // Alert if the compactor has not uploaded anything since its start.
          alert: $.alertName('CompactorHasNotUploadedBlocks'),
          'for': '24h',
          expr: |||
            (max by(%(alert_aggregation_labels)s, %(per_instance_label)s) (thanos_objstore_bucket_last_successful_upload_time{component="compactor"}) == 0)
            and
            # Only if some compactions have started. We don't want to fire this alert if the compactor has nothing to do.
            (sum by(%(alert_aggregation_labels)s, %(per_instance_label)s) (rate(cortex_compactor_group_compaction_runs_started_total[24h])) > 0)
          ||| % $._config,
          labels: {
            severity: 'critical',
            time_period: 'since-start',
          },
          annotations: {
            message: '%(product)s Compactor %(alert_instance_variable)s in %(alert_aggregation_variables)s has not uploaded any block since its start.' % $._config,
          },
        },
        {
          // Alert if compactor has marked blocks for no-compaction in the last 24 hours.
          alert: $.alertName('CompactorSkippedBlocks'),
          'for': '5m',
          expr: |||
            sum by (%(alert_aggregation_labels)s, reason) (
              increase(cortex_compactor_blocks_marked_for_no_compaction_total[24h]) > 0
            )
          ||| % $._config,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: '%(product)s Compactor in %(alert_aggregation_variables)s has marked {{ $value }} blocks for no-compaction (reason: {{ $labels.reason }}).' % $._config,
          },
        },
        // Alert if compactor has failed to build sparse-index headers.
        {
          alert: $.alertName('CompactorBuildingSparseIndexFailed'),
          'for': '30m',
          expr: |||
            (sum by(%(alert_aggregation_labels)s, %(per_instance_label)s) (increase(cortex_compactor_build_sparse_headers_failures_total[%(range_interval)s])) > 0)
          ||| % $._config {
            range_interval: $.alertRangeInterval(5),
          },
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: '%(product)s Compactor %(alert_instance_variable)s in %(alert_aggregation_variables)s is failing to build sparse index headers' % $._config,
          },
        },
      ] + [
        // Alert if compactor pods are being OOMKilled.
        {
          alert: $.alertName('CompactorOOMKilled'),
          'for': '15m',
          expr: |||
            (
              sum by(%(alert_aggregation_labels)s, %(per_instance_label)s) (
                increase(kube_pod_container_status_restarts_total{container=~"%(compactor)s"}[%(time_window)s])
              )
              > %(threshold)s
            )
            and on (%(alert_aggregation_labels)s, %(per_instance_label)s)
            (
              kube_pod_container_status_last_terminated_reason{container=~"%(compactor)s", reason="OOMKilled"} > 0
            )
          ||| % ($._config { compactor: $._config.container_names.compactor } + settings),
          labels: {
            severity: settings.severity,
          },
          annotations: {
            message: '%(product)s Compactor %(alert_instance_variable)s in %(alert_aggregation_variables)s has been OOMKilled {{ printf "%%.2f" $value }} times in the last %(time_window)s.' % ($._config + settings),
          },
        }
        for settings in [
          { severity: 'warning', threshold: 2, time_window: '4h' },
          { severity: 'critical', threshold: 5, time_window: '2h' },
        ]
      ],
    },
  ],

  groups+: $.withRunbookURL('https://grafana.com/docs/mimir/latest/operators-guide/mimir-runbooks/#%s', $.withExtraLabelsAnnotations(alertGroups)),
}
