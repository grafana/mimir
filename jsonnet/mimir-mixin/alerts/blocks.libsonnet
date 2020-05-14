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
          alert: 'CortexIngesterHasNotShippedBlocksSinceStart',
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
        {
          // Alert if the querier is not successfully scanning the bucket.
          alert: 'CortexQuerierHasNotScanTheBucket',
          'for': '5m',
          expr: |||
            (time() - cortex_querier_blocks_last_successful_scan_timestamp_seconds{%s} > 60 * 30)
            and
            cortex_querier_blocks_last_successful_scan_timestamp_seconds{%s} > 0
          ||| % [$.namespace_matcher(''), $.namespace_matcher('')],
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: 'Cortex Querier {{ $labels.namespace }}/{{ $labels.instance }} has not successfully scanned the bucket since {{ $value | humanizeDuration }}.',
          },
        },
        {
          // Alert if the store-gateway is not successfully synching the bucket.
          alert: 'CortexStoreGatewayHasNotSyncTheBucket',
          'for': '5m',
          expr: |||
            (time() - cortex_storegateway_blocks_last_successful_sync_timestamp_seconds{%s} > 60 * 30)
            and
            cortex_storegateway_blocks_last_successful_sync_timestamp_seconds{%s} > 0
          ||| % [$.namespace_matcher(''), $.namespace_matcher('')],
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: 'Cortex Store Gateway {{ $labels.namespace }}/{{ $labels.instance }} has not successfully synched the bucket since {{ $value | humanizeDuration }}.',
          },
        },
      ],
    },
  ],
}
