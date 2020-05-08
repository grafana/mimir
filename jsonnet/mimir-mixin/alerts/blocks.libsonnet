(import 'alert-utils.libsonnet') {
  groups+: [
    {
      name: 'cortex_blocks_alerts',
      rules: [
        {
          // Alert if the ingester has not shipped any block in the last 4h.
          alert: 'CortexIngesterHasNotShippedBlocks',
          'for': '15m',
          expr: |||
            (time() - thanos_objstore_bucket_last_successful_upload_time{job=~".+/ingester"%s} > 60 * 60 * 4)
            and
            (thanos_objstore_bucket_last_successful_upload_time{job=~".+/ingester"%s} > 0)
          ||| % [$.namespace_matcher(','), $.namespace_matcher(',')],
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: 'Cortex Ingester {{ $labels.namespace }}/{{ $labels.instance }} has not shipped any block in the last 4 hours.',
          },
        },
        {
          // Alert if the ingester has not shipped any block since start.
          alert: 'CortexIngesterHasNotShippedBlocks',
          'for': '4h',
          expr: |||
            thanos_objstore_bucket_last_successful_upload_time{job=~".+/ingester"%s} == 0
          ||| % $.namespace_matcher(','),
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: 'Cortex Ingester {{ $labels.namespace }}/{{ $labels.instance }} has not shipped any block in the last 4 hours.',
          },
        },
      ],
    },
  ],
}
