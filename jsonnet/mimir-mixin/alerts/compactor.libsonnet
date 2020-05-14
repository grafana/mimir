(import 'alert-utils.libsonnet') {
  groups+: [
    {
      name: 'cortex_compactor_alerts',
      rules: [
        {
          // Alert if the compactor has not successfully completed a run in the last 24h.
          alert: 'CortexCompactorHasNotSuccessfullyRun',
          'for': '15m',
          expr: |||
            (time() - cortex_compactor_last_successful_run_timestamp_seconds{%s} > 60 * 60 * 24)
            and
            (cortex_compactor_last_successful_run_timestamp_seconds{%s} > 0)
          ||| % [$.namespace_matcher(''), $.namespace_matcher('')],
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: 'Cortex Compactor {{ $labels.namespace }}/{{ $labels.instance }} has not successfully completed a run in the last 24 hours.',
          },
        },
        {
          // Alert if the compactor has not successfully completed a run since its start.
          alert: 'CortexCompactorHasNotSuccessfullyRunSinceStart',
          'for': '24h',
          expr: |||
            cortex_compactor_last_successful_run_timestamp_seconds{%s} == 0
          ||| % $.namespace_matcher(''),
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: 'Cortex Compactor {{ $labels.namespace }}/{{ $labels.instance }} has not successfully completed a run in the last 24 hours.',
          },
        },
        {
          // Alert if the compactor has not uploaded anything in the last 24h.
          alert: 'CortexCompactorHasNotUploadedBlocks',
          'for': '15m',
          expr: |||
            (time() - thanos_objstore_bucket_last_successful_upload_time{job=~".+/compactor"%s} > 60 * 60 * 24)
            and
            (thanos_objstore_bucket_last_successful_upload_time{job=~".+/compactor"%s} > 0)
          ||| % [$.namespace_matcher(','), $.namespace_matcher(',')],
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
            thanos_objstore_bucket_last_successful_upload_time{job=~".+/compactor"%s} == 0
          ||| % $.namespace_matcher(','),
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: 'Cortex Compactor {{ $labels.namespace }}/{{ $labels.instance }} has not uploaded any block in the last 24 hours.',
          },
        },
      ],
    },
  ],
}
