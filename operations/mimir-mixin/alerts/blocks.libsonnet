(import 'alerts-utils.libsonnet') {
  groups+: [
    {
      name: 'mimir_blocks_alerts',
      rules: [
        {
          // Alert if the ingester has not shipped any block in the last 4h. It also checks cortex_ingester_ingested_samples_total
          // to avoid false positives on ingesters not receiving any traffic yet (eg. a newly created cluster).
          alert: $.alertName('IngesterHasNotShippedBlocks'),
          'for': '15m',
          expr: |||
            (min by(%(alert_aggregation_labels)s, %(per_instance_label)s) (time() - thanos_objstore_bucket_last_successful_upload_time{job=~".+/ingester.*"}) > 60 * 60 * 4)
            and
            (max by(%(alert_aggregation_labels)s, %(per_instance_label)s) (thanos_objstore_bucket_last_successful_upload_time{job=~".+/ingester.*"}) > 0)
            and
            # Only if the ingester has ingested samples over the last 4h.
            (max by(%(alert_aggregation_labels)s, %(per_instance_label)s) (max_over_time(%(alert_aggregation_rule_prefix)s_%(per_instance_label)s:cortex_ingester_ingested_samples_total:rate1m[4h])) > 0)
            and
            # Only if the ingester was ingesting samples 4h ago. This protects from the case the ingester instance
            # had ingested samples in the past, then no traffic was received for a long period and then it starts
            # receiving samples again. Without this check, the alert would fire as soon as it gets back receiving
            # samples, while the a block shipping is expected within the next 4h.
            (max by(%(alert_aggregation_labels)s, %(per_instance_label)s) (max_over_time(%(alert_aggregation_rule_prefix)s_%(per_instance_label)s:cortex_ingester_ingested_samples_total:rate1m[1h] offset 4h)) > 0)
          ||| % $._config,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: '%(product)s Ingester {{ $labels.%(per_instance_label)s }} in %(alert_aggregation_variables)s has not shipped any block in the last 4 hours.' % $._config,
          },
        },
        {
          // Alert if the ingester has not shipped any block since start. It also checks cortex_ingester_ingested_samples_total
          // to avoid false positives on ingesters not receiving any traffic yet (eg. a newly created cluster).
          alert: $.alertName('IngesterHasNotShippedBlocksSinceStart'),
          'for': '4h',
          expr: |||
            (max by(%(alert_aggregation_labels)s, %(per_instance_label)s) (thanos_objstore_bucket_last_successful_upload_time{job=~".+/ingester.*"}) == 0)
            and
            (max by(%(alert_aggregation_labels)s, %(per_instance_label)s) (max_over_time(%(alert_aggregation_rule_prefix)s_%(per_instance_label)s:cortex_ingester_ingested_samples_total:rate1m[4h])) > 0)
          ||| % $._config,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: '%(product)s Ingester {{ $labels.%(per_instance_label)s }} in %(alert_aggregation_variables)s has not shipped any block in the last 4 hours.' % $._config,
          },
        },
        {
          // Alert if the ingester has compacted some blocks that haven't been successfully uploaded to the storage yet since
          // more than 1 hour. The metric tracks the time of the oldest unshipped block, measured as the time when the
          // TSDB head has been compacted to a block. The metric is 0 if all blocks have been shipped.
          alert: $.alertName('IngesterHasUnshippedBlocks'),
          'for': '15m',
          expr: |||
            (time() - cortex_ingester_oldest_unshipped_block_timestamp_seconds > 3600)
            and
            (cortex_ingester_oldest_unshipped_block_timestamp_seconds > 0)
          |||,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: "%(product)s Ingester {{ $labels.instance }} in %(alert_aggregation_variables)s has compacted a block {{ $value | humanizeDuration }} ago but it hasn't been successfully uploaded to the storage yet." % $._config,
          },
        },
        {
          // Alert if the ingester is failing to compact TSDB head into a block, for any opened TSDB. Once the TSDB head is
          // compactable, the ingester will try to compact it every 1 minute. Repeatedly failing it is a critical condition
          // that should never happen.
          alert: $.alertName('IngesterTSDBHeadCompactionFailed'),
          'for': '15m',
          expr: |||
            rate(cortex_ingester_tsdb_compactions_failed_total[5m]) > 0
          |||,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: '%(product)s Ingester {{ $labels.instance }} in %(alert_aggregation_variables)s is failing to compact TSDB head.' % $._config,
          },
        },
        {
          alert: $.alertName('IngesterTSDBHeadTruncationFailed'),
          expr: |||
            rate(cortex_ingester_tsdb_head_truncations_failed_total[5m]) > 0
          |||,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: '%(product)s Ingester {{ $labels.instance }} in %(alert_aggregation_variables)s is failing to truncate TSDB head.' % $._config,
          },
        },
        {
          alert: $.alertName('IngesterTSDBCheckpointCreationFailed'),
          expr: |||
            rate(cortex_ingester_tsdb_checkpoint_creations_failed_total[5m]) > 0
          |||,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: '%(product)s Ingester {{ $labels.instance }} in %(alert_aggregation_variables)s is failing to create TSDB checkpoint.' % $._config,
          },
        },
        {
          alert: $.alertName('IngesterTSDBCheckpointDeletionFailed'),
          expr: |||
            rate(cortex_ingester_tsdb_checkpoint_deletions_failed_total[5m]) > 0
          |||,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: '%(product)s Ingester {{ $labels.instance }} in %(alert_aggregation_variables)s is failing to delete TSDB checkpoint.' % $._config,
          },
        },
        {
          alert: $.alertName('IngesterTSDBWALTruncationFailed'),
          expr: |||
            rate(cortex_ingester_tsdb_wal_truncations_failed_total[5m]) > 0
          |||,
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: '%(product)s Ingester {{ $labels.instance }} in %(alert_aggregation_variables)s is failing to truncate TSDB WAL.' % $._config,
          },
        },
        {
          alert: $.alertName('IngesterTSDBWALCorrupted'),
          expr: |||
            rate(cortex_ingester_tsdb_wal_corruptions_total[5m]) > 0
          |||,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: '%(product)s Ingester {{ $labels.instance }} in %(alert_aggregation_variables)s got a corrupted TSDB WAL.' % $._config,
          },
        },
        {
          alert: $.alertName('IngesterTSDBWALWritesFailed'),
          'for': '3m',
          expr: |||
            rate(cortex_ingester_tsdb_wal_writes_failed_total[1m]) > 0
          |||,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: '%(product)s Ingester {{ $labels.instance }} in %(alert_aggregation_variables)s is failing to write to TSDB WAL.' % $._config,
          },
        },
        {
          // Alert if the querier is not successfully scanning the bucket.
          alert: $.alertName('QuerierHasNotScanTheBucket'),
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
            message: '%(product)s Querier {{ $labels.instance }} in %(alert_aggregation_variables)s has not successfully scanned the bucket since {{ $value | humanizeDuration }}.' % $._config,
          },
        },
        {
          // Alert if the number of queries for which we had to refetch series from different store-gateways
          // (because of missing blocks) is greater than a %.
          alert: $.alertName('QuerierHighRefetchRate'),
          'for': '10m',
          expr: |||
            100 * (
              (
                sum by(%(alert_aggregation_labels)s) (rate(cortex_querier_storegateway_refetches_per_query_count[5m]))
                -
                sum by(%(alert_aggregation_labels)s) (rate(cortex_querier_storegateway_refetches_per_query_bucket{le="0.0"}[5m]))
              )
              /
              sum by(%(alert_aggregation_labels)s) (rate(cortex_querier_storegateway_refetches_per_query_count[5m]))
            )
            > 1
          ||| % $._config,
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: '%(product)s Queries in %(alert_aggregation_variables)s are refetching series from different store-gateways (because of missing blocks) for the {{ printf "%%.0f" $value }}%% of queries.' % $._config,
          },
        },
        {
          // Alert if the store-gateway is not successfully synching the bucket.
          alert: $.alertName('StoreGatewayHasNotSyncTheBucket'),
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
            message: '%(product)s Store Gateway {{ $labels.instance }} in %(alert_aggregation_variables)s has not successfully synched the bucket since {{ $value | humanizeDuration }}.' % $._config,
          },
        },
        {
          // Alert if the bucket index has not been updated for a given user.
          alert: $.alertName('BucketIndexNotUpdated'),
          expr: |||
            min by(%(alert_aggregation_labels)s, user) (time() - cortex_bucket_index_last_successful_update_timestamp_seconds) > 7200
          ||| % $._config,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: '%(product)s bucket index for tenant {{ $labels.user }} in %(alert_aggregation_variables)s has not been updated since {{ $value | humanizeDuration }}.' % $._config,
          },
        },
        {
          // Alert if a we consistently find partial blocks for a given tenant over a relatively large time range.
          alert: $.alertName('TenantHasPartialBlocks'),
          'for': '6h',
          expr: |||
            max by(%(alert_aggregation_labels)s, user) (cortex_bucket_blocks_partials_count) > 0
          ||| % $._config,
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: '%(product)s tenant {{ $labels.user }} in %(alert_aggregation_variables)s has {{ $value }} partial blocks.' % $._config,
          },
        },
      ],
    },
  ],
}
