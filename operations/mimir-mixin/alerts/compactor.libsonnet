(import 'alerts-utils.libsonnet') {
  local alertGroups = [
    {
      name: 'mimir_compactor_alerts',
      rules: [
        {
          // Alert if the compactor has not successfully cleaned up blocks in the last 6h.
          alert: $.alertName('CompactorHasNotSuccessfullyCleanedUpBlocks'),
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
        {
          // Alert if the compactor has not successfully run compaction in the last 24h.
          alert: $.alertName('CompactorHasNotSuccessfullyRunCompaction'),
          'for': '1h',
          expr: |||
            # The "last successful run" metric is updated even if the compactor owns no tenants,
            # so this alert correctly doesn't fire if compactor has nothing to do.
            (time() - cortex_compactor_last_successful_run_timestamp_seconds > 60 * 60 * 24)
            and
            (cortex_compactor_last_successful_run_timestamp_seconds > 0)
          |||,
          labels: {
            severity: 'critical',
            reason: 'in-last-24h',
          },
          annotations: {
            message: '%(product)s Compactor %(alert_instance_variable)s in %(alert_aggregation_variables)s has not run compaction in the last 24 hours.' % $._config,
          },
        },
        {
          // Alert if the compactor has not successfully run compaction in the last 24h since startup.
          alert: $.alertName('CompactorHasNotSuccessfullyRunCompaction'),
          'for': '24h',
          expr: |||
            # The "last successful run" metric is updated even if the compactor owns no tenants,
            # so this alert correctly doesn't fire if compactor has nothing to do.
            cortex_compactor_last_successful_run_timestamp_seconds == 0
          |||,
          labels: {
            severity: 'critical',
            reason: 'since-startup',
          },
          annotations: {
            message: '%(product)s Compactor %(alert_instance_variable)s in %(alert_aggregation_variables)s has not run compaction in the last 24 hours.' % $._config,
          },
        },
        {
          // Alert if compactor failed to run 2 consecutive compactions excluding shutdowns.
          alert: $.alertName('CompactorHasNotSuccessfullyRunCompaction'),
          expr: |||
            increase(cortex_compactor_runs_failed_total{reason!="shutdown"}[2h]) >= 2
          |||,
          labels: {
            severity: 'critical',
            reason: 'consecutive-failures',
          },
          annotations: {
            message: '%(product)s Compactor %(alert_instance_variable)s in %(alert_aggregation_variables)s failed to run 2 consecutive compactions.' % $._config,
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
            # Only if some compactions have started. We don't want to fire this alert if the compactor has nothing to do
            # (e.g. there are more replicas than required because running as part of mimir-backend).
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
            # Only if some compactions have started. We don't want to fire this alert if the compactor has nothing to do
            # (e.g. there are more replicas than required because running as part of mimir-backend).
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
          // Alert if compactor has tried to compact blocks with out-of-order chunks.
          alert: $.alertName('CompactorSkippedBlocksWithOutOfOrderChunks'),
          'for': '1m',
          expr: |||
            increase(cortex_compactor_blocks_marked_for_no_compaction_total{reason="block-index-out-of-order-chunk"}[5m]) > 0
          |||,
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: '%(product)s Compactor %(alert_instance_variable)s in %(alert_aggregation_variables)s has found and ignored blocks with out of order chunks.' % $._config,
          },
        },
        {
          // Alert if compactor has tried to compact blocks with out-of-order chunks.
          // Any number greater than 1 over the last 30 minutes should be investigated quickly as it could start to impact the read path.
          alert: $.alertName('CompactorSkippedBlocksWithOutOfOrderChunks'),
          'for': '30m',
          expr: |||
            increase(cortex_compactor_blocks_marked_for_no_compaction_total{reason="block-index-out-of-order-chunk"}[5m]) > 1
          |||,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: '%(product)s Compactor %(alert_instance_variable)s in %(alert_aggregation_variables)s has found and ignored blocks with out of order chunks.' % $._config,
          },
        },
      ],
    },
  ],

  groups+: $.withRunbookURL('https://grafana.com/docs/mimir/latest/operators-guide/mimir-runbooks/#%s', alertGroups),
}
