{
  groups+: [
    {
      name: 'cortex_compactor_alerts',
      rules: [
        {
          // Alert if the compactor has not successfully cleaned up blocks in the last 24h.
          alert: 'CortexCompactorHasNotSuccessfullyCleanedUpBlocks',
          'for': '15m',
          expr: |||
            (time() - cortex_compactor_block_cleanup_last_successful_run_timestamp_seconds > 60 * 60 * 24)
            and
            (cortex_compactor_block_cleanup_last_successful_run_timestamp_seconds > 0)
          |||,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: 'Cortex Compactor {{ $labels.namespace }}/{{ $labels.instance }} has not successfully cleaned up blocks in the last 24 hours.',
          },
        },
        {
          // Alert if the compactor has not successfully cleaned up blocks since its start.
          alert: 'CortexCompactorHasNotSuccessfullyCleanedUpBlocksSinceStart',
          'for': '24h',
          expr: |||
            cortex_compactor_block_cleanup_last_successful_run_timestamp_seconds == 0
          |||,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: 'Cortex Compactor {{ $labels.namespace }}/{{ $labels.instance }} has not successfully cleaned up blocks in the last 24 hours.',
          },
        },
        {
          // Alert if the compactor has not uploaded anything in the last 24h.
          alert: 'CortexCompactorHasNotUploadedBlocks',
          'for': '15m',
          expr: |||
            (time() - thanos_objstore_bucket_last_successful_upload_time{job=~".+/%(compactor)s"} > 60 * 60 * 24)
            and
            (thanos_objstore_bucket_last_successful_upload_time{job=~".+/%(compactor)s"} > 0)
          ||| % $._config.job_names,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: 'Cortex Compactor {{ $labels.namespace }}/{{ $labels.instance }} has not uploaded any block in the last 24 hours.',
          },
        },
        {
          // Alert if the compactor has not uploaded anything since its start.
          alert: 'CortexCompactorHasNotUploadedBlocksSinceStart',
          'for': '24h',
          expr: |||
            thanos_objstore_bucket_last_successful_upload_time{job=~".+/%(compactor)s"} == 0
          ||| % $._config.job_names,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: 'Cortex Compactor {{ $labels.namespace }}/{{ $labels.instance }} has not uploaded any block in the last 24 hours.',
          },
        },
        {
          // Alert if compactor fails.
          alert: 'CortexCompactorRunFailed',
          expr: |||
            increase(cortex_compactor_runs_failed_total[2h]) > 1
          |||,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: |||
              {{ $labels.job }}/{{ $labels.instance }} failed to run compaction.
            |||,
          },
        },
      ],
    },
  ],
}
