{
  groups+: [
    {
      name: 'cortex_blocks_alerts',
      rules: [
        {
          // Alert if the ingester has not shipped any block in the last 4h. It also checks cortex_ingester_ingested_samples_total
          // to avoid false positives on ingesters not receiving any traffic yet (eg. a newly created cluster).
          alert: 'CortexIngesterHasNotShippedBlocks',
          'for': '15m',
          expr: |||
            (min by(namespace, instance) (time() - thanos_objstore_bucket_last_successful_upload_time{job=~".+/ingester"}) > 60 * 60 * 4)
            and
            (max by(namespace, instance) (thanos_objstore_bucket_last_successful_upload_time{job=~".+/ingester"}) > 0)
            and
            (max by(namespace, instance) (rate(cortex_ingester_ingested_samples_total[4h])) > 0)
          |||,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: 'Cortex Ingester {{ $labels.namespace }}/{{ $labels.instance }} has not shipped any block in the last 4 hours.',
          },
        },
        {
          // Alert if the ingester has not shipped any block since start. It also checks cortex_ingester_ingested_samples_total
          // to avoid false positives on ingesters not receiving any traffic yet (eg. a newly created cluster).
          alert: 'CortexIngesterHasNotShippedBlocksSinceStart',
          'for': '4h',
          expr: |||
            (max by(namespace, instance) (thanos_objstore_bucket_last_successful_upload_time{job=~".+/ingester"}) == 0)
            and
            (max by(namespace, instance) (rate(cortex_ingester_ingested_samples_total[4h])) > 0)
          |||,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: 'Cortex Ingester {{ $labels.namespace }}/{{ $labels.instance }} has not shipped any block in the last 4 hours.',
          },
        },
        {
          // Alert if the ingester is failing to compact TSDB head into a block, for any opened TSDB. This is a critical
          // condition that should never happen.
          alert: 'CortexIngesterTSDBHeadCompactionFailed',
          'for': '15m',
          expr: |||
            rate(cortex_ingester_tsdb_compactions_failed_total[5m]) > 0
          |||,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: 'Cortex Ingester {{ $labels.namespace }}/{{ $labels.instance }} is failing to compact TSDB head.',
          },
        },
        {
          // Alert if the querier is not successfully scanning the bucket.
          alert: 'CortexQuerierHasNotScanTheBucket',
          'for': '5m',
          expr: |||
            (time() - cortex_querier_blocks_last_successful_scan_timestamp_seconds > 60 * 30)
            and
            cortex_querier_blocks_last_successful_scan_timestamp_seconds > 0
          |||,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: 'Cortex Querier {{ $labels.namespace }}/{{ $labels.instance }} has not successfully scanned the bucket since {{ $value | humanizeDuration }}.',
          },
        },
        {
          // Alert if the number of queries for which we had to refetch series from different store-gateways
          // (because of missing blocks) is greater than a %.
          alert: 'CortexQuerierHighRefetchRate',
          'for': '10m',
          expr: |||
            100 * (
              (
                sum by(namespace) (rate(cortex_querier_storegateway_refetches_per_query_count[5m]))
                -
                sum by(namespace) (rate(cortex_querier_storegateway_refetches_per_query_bucket{le="0"}[5m]))
              )
              /
              sum by(namespace) (rate(cortex_querier_storegateway_refetches_per_query_count[5m]))
            )
            > 1
          |||,
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: 'Cortex Queries in {{ $labels.namespace }} are refetching series from different store-gateways (because of missing blocks) for the {{ printf "%.0f" $value }}% of queries.',
          },
        },
        {
          // Alert if the store-gateway is not successfully synching the bucket.
          alert: 'CortexStoreGatewayHasNotSyncTheBucket',
          'for': '5m',
          expr: |||
            (time() - cortex_bucket_stores_blocks_last_successful_sync_timestamp_seconds{component="store-gateway"} > 60 * 30)
            and
            cortex_bucket_stores_blocks_last_successful_sync_timestamp_seconds{component="store-gateway"} > 0
          |||,
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
