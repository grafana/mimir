(import 'alerts-utils.libsonnet') {
  groups+: [
    {
      name: 'mimir_compactor_alerts',
      rules: [
        {
          // Alert if the compactor has not successfully cleaned up blocks in the last 6h.
          alert: $.alertName('CompactorHasNotSuccessfullyCleanedUpBlocks'),
          'for': '1h',
          expr: |||
            (time() - cortex_compactor_block_cleanup_last_successful_run_timestamp_seconds > 60 * 60 * 6)
          |||,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: '%(product)s Compactor {{ $labels.instance }} in %(alert_aggregation_variables)s has not successfully cleaned up blocks in the last 6 hours.' % $._config,
          },
        },
        {
          // Alert if the compactor has not successfully run compaction in the last 24h.
          alert: $.alertName('CompactorHasNotSuccessfullyRunCompaction'),
          'for': '1h',
          expr: |||
            (time() - cortex_compactor_last_successful_run_timestamp_seconds > 60 * 60 * 24)
            and
            (cortex_compactor_last_successful_run_timestamp_seconds > 0)
          |||,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: '%(product)s Compactor {{ $labels.instance }} in %(alert_aggregation_variables)s has not run compaction in the last 24 hours.' % $._config,
          },
        },
        {
          // Alert if the compactor has not successfully run compaction in the last 24h since startup.
          alert: $.alertName('CompactorHasNotSuccessfullyRunCompaction'),
          'for': '24h',
          expr: |||
            cortex_compactor_last_successful_run_timestamp_seconds == 0
          |||,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: '%(product)s Compactor {{ $labels.instance }} in %(alert_aggregation_variables)s has not run compaction in the last 24 hours.' % $._config,
          },
        },
        {
          // Alert if compactor failed to run 2 consecutive compactions.
          alert: $.alertName('CompactorHasNotSuccessfullyRunCompaction'),
          expr: |||
            increase(cortex_compactor_runs_failed_total[2h]) >= 2
          |||,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: '%(product)s Compactor {{ $labels.instance }} in %(alert_aggregation_variables)s failed to run 2 consecutive compactions.' % $._config,
          },
        },
        {
          // Alert if the compactor has not uploaded anything in the last 24h.
          alert: $.alertName('CompactorHasNotUploadedBlocks'),
          'for': '15m',
          expr: |||
            (time() - thanos_objstore_bucket_last_successful_upload_time{job=~".+/(%(compactor)s)"} > 60 * 60 * 24)
            and
            (thanos_objstore_bucket_last_successful_upload_time{job=~".+/(%(compactor)s)"} > 0)
          ||| % $._config.job_names,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: '%(product)s Compactor {{ $labels.instance }} in %(alert_aggregation_variables)s has not uploaded any block in the last 24 hours.' % $._config,
          },
        },
        {
          // Alert if the compactor has not uploaded anything since its start.
          alert: $.alertName('CompactorHasNotUploadedBlocks'),
          'for': '24h',
          expr: |||
            thanos_objstore_bucket_last_successful_upload_time{job=~".+/(%(compactor)s)"} == 0
          ||| % $._config.job_names,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: '%(product)s Compactor {{ $labels.instance }} in %(alert_aggregation_variables)s has not uploaded any block in the last 24 hours.' % $._config,
          },
        },
        {
          // Alert if compactor has tried to compact blocks with out-of-order chunks.
          alert: $.alertName('CompactorSkippedBlocksWithOutOfOrderChunks'),
          'for': '1m',
          expr: |||
            increase(cortex_compactor_blocks_marked_for_no_compaction_total{job=~".+/(%(compactor)s)", reason="block-index-out-of-order-chunk"}[5m]) > 0
          ||| % $._config.job_names,
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: '%(product)s Compactor {{ $labels.instance }} in %(alert_aggregation_variables)s has found and ignored blocks with out of order chunks.' % $._config,
          },
        },
      ],
    },
  ],
}
