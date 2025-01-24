(import 'alerts-utils.libsonnet') {
  local alertGroups = [
    {
      name: 'mimir_ingest_storage_alerts',
      rules: [
        {
          alert: $.alertName('IngesterLastConsumedOffsetCommitFailed'),
          'for': '15m',
          expr: |||
            sum by(%(alert_aggregation_labels)s, %(per_instance_label)s) (rate(cortex_ingest_storage_reader_offset_commit_failures_total[5m]))
            /
            sum by(%(alert_aggregation_labels)s, %(per_instance_label)s) (rate(cortex_ingest_storage_reader_offset_commit_requests_total[5m]))
            > 0.2
          ||| % $._config,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: '%(product)s {{ $labels.%(per_instance_label)s }} in %(alert_aggregation_variables)s is failing to commit the last consumed offset.' % $._config,
          },
        },

        {
          alert: $.alertName('IngesterFailedToReadRecordsFromKafka'),
          'for': '5m',

          // Metric used by this alert is reported by Kafka client on read errors from connection to Kafka.
          // We use node_id to only alert if problems to the same Kafka node are repeating.
          // If problems are for different nodes (eg. during rollout), that is not a problem, and we don't need to trigger alert.
          expr: |||
            sum by(%(alert_aggregation_labels)s, %(per_instance_label)s, node_id) (rate(cortex_ingest_storage_reader_read_errors_total[1m]))
            > 0
          ||| % $._config,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: '%(product)s {{ $labels.%(per_instance_label)s }} in %(alert_aggregation_variables)s is failing to read records from Kafka.' % $._config,
          },
        },

        {
          alert: $.alertName('IngesterKafkaFetchErrorsRateTooHigh'),
          'for': '15m',
          // See https://github.com/grafana/mimir/blob/24591ae56cd7d6ef24a7cc1541a41405676773f4/vendor/github.com/twmb/franz-go/pkg/kgo/record_and_fetch.go#L332-L366 for errors that can be reported here.
          expr: |||
            sum by (%(alert_aggregation_labels)s, %(per_instance_label)s) (rate (cortex_ingest_storage_reader_fetch_errors_total[5m]))
            /
            sum by (%(alert_aggregation_labels)s, %(per_instance_label)s) (rate (cortex_ingest_storage_reader_fetches_total[5m]))
            > 0.1
          ||| % $._config,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: '%(product)s {{ $labels.%(per_instance_label)s }} in %(alert_aggregation_variables)s is receiving fetch errors when reading records from Kafka.' % $._config,
          },
        },

        // This is an experiment. We compute derivatition (ie. rate of consumption lag change) over 5 minutes. If derivation is above 0, it means consumption lag is increasing, instead of decreasing.
        {
          alert: $.alertName('StartingIngesterKafkaReceiveDelayIncreasing'),
          'for': '5m',
          expr: |||
            deriv((
                sum by (%(alert_aggregation_labels)s, %(per_instance_label)s) (rate(cortex_ingest_storage_reader_receive_delay_seconds_sum{phase="starting"}[1m]))
                /
                sum by (%(alert_aggregation_labels)s, %(per_instance_label)s) (rate(cortex_ingest_storage_reader_receive_delay_seconds_count{phase="starting"}[1m]))
            )[5m:1m]) > 0
          ||| % $._config,
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: '%(product)s {{ $labels.%(per_instance_label)s }} in %(alert_aggregation_variables)s in "starting" phase is not reducing consumption lag of write requests read from Kafka.' % $._config,
          },
        },

        // Alert firing if an ingester is ingesting data with a very high delay, even for a short period of time.
        // With a threshold of 2m, a for duration of 3m, an evaluation delay of 1m and an evaluation interval of 1m
        // when this alert fires the ingester should be up to 2+3+1+1=7 minutes behind.
        {
          alert: $.alertName('RunningIngesterReceiveDelayTooHigh'),
          'for': '3m',
          expr: |||
            (
              sum by (%(alert_aggregation_labels)s, %(per_instance_label)s) (rate(cortex_ingest_storage_reader_receive_delay_seconds_sum{phase="running"}[1m]))
              /
              sum by (%(alert_aggregation_labels)s, %(per_instance_label)s) (rate(cortex_ingest_storage_reader_receive_delay_seconds_count{phase="running"}[1m]))
            ) > (2 * 60)
          ||| % $._config,
          labels: {
            severity: 'critical',
            threshold: 'very_high_for_short_period',  // Add an extra label to distinguish between multiple alerts with the same name.
          },
          annotations: {
            message: '%(product)s {{ $labels.%(per_instance_label)s }} in %(alert_aggregation_variables)s in "running" phase is too far behind in its consumption of write requests from Kafka.' % $._config,
          },
        },

        // Alert firing if an ingester is ingesting data with a relatively high delay for a long period of time.
        {
          alert: $.alertName('RunningIngesterReceiveDelayTooHigh'),
          'for': '15m',
          expr: |||
            (
              sum by (%(alert_aggregation_labels)s, %(per_instance_label)s) (rate(cortex_ingest_storage_reader_receive_delay_seconds_sum{phase="running"}[1m]))
              /
              sum by (%(alert_aggregation_labels)s, %(per_instance_label)s) (rate(cortex_ingest_storage_reader_receive_delay_seconds_count{phase="running"}[1m]))
            ) > 30
          ||| % $._config,
          labels: {
            severity: 'critical',
            threshold: 'relatively_high_for_long_period',  // Add an extra label to distinguish between multiple alerts with the same name.
          },
          annotations: {
            message: '%(product)s {{ $labels.%(per_instance_label)s }} in %(alert_aggregation_variables)s in "running" phase is too far behind in its consumption of write requests from Kafka.' % $._config,
          },
        },

        // Alert firing if an ingester is failing to read from Kafka.
        {
          alert: $.alertName('IngesterFailsToProcessRecordsFromKafka'),
          'for': '5m',
          expr: |||
            sum by (%(alert_aggregation_labels)s, %(per_instance_label)s) (
                # This is the old metric name. We're keeping support for backward compatibility.
              rate(cortex_ingest_storage_reader_records_failed_total{cause="server"}[1m])
              or
              rate(cortex_ingest_storage_reader_requests_failed_total{cause="server"}[1m])
            ) > 0
          ||| % $._config,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: '%(product)s {{ $labels.%(per_instance_label)s }} in %(alert_aggregation_variables)s fails to consume write requests read from Kafka due to internal errors.' % $._config,
          },
        },

        // Alert firing is an ingester is reading from Kafka, there are buffered records to process, but processing is stuck.
        {
          alert: $.alertName('IngesterStuckProcessingRecordsFromKafka'),
          'for': '5m',
          expr: |||
            # Alert if the reader is not processing any records, but there buffered records to process in the Kafka client.
            (sum by (%(alert_aggregation_labels)s, %(per_instance_label)s) (
                # This is the old metric name. We're keeping support for backward compatibility.
              rate(cortex_ingest_storage_reader_records_total[5m])
              or
              rate(cortex_ingest_storage_reader_requests_total[5m])
            ) == 0)
            and
            (sum by (%(alert_aggregation_labels)s, %(per_instance_label)s) (cortex_ingest_storage_reader_buffered_fetched_records) > 0)
          ||| % $._config,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: '%(product)s {{ $labels.%(per_instance_label)s }} in %(alert_aggregation_variables)s is stuck processing write requests from Kafka.' % $._config,
          },
        },

        // Alert firing is an ingester is reading from Kafka, there are buffered records to process, but processing is stuck.
        {
          alert: $.alertName('IngesterMissedRecordsFromKafka'),
          expr: |||
            # Alert if the ingester missed some records from Kafka.
            increase(cortex_ingest_storage_reader_missed_records_total[%s]) > 0
          ||| % $.alertRangeInterval(10),
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: '%(product)s {{ $labels.%(per_instance_label)s }} in %(alert_aggregation_variables)s missed processing records from Kafka. There may be data loss.' % $._config,
          },
        },

        // Alert firing if Mimir is failing to enforce strong read consistency.
        {
          alert: $.alertName('StrongConsistencyEnforcementFailed'),
          'for': '5m',
          expr: |||
            sum by (%(alert_aggregation_labels)s, %(per_instance_label)s) (rate(cortex_ingest_storage_strong_consistency_failures_total[1m])) > 0
          ||| % $._config,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: '%(product)s {{ $labels.%(per_instance_label)s }} in %(alert_aggregation_variables)s fails to enforce strong-consistency on read-path.' % $._config,
          },
        },

        // Alert firing if ingesters are receiving an unexpected high number of strongly consistent requests without an offset specified.
        {
          alert: $.alertName('StrongConsistencyOffsetNotPropagatedToIngesters'),
          'for': '5m',
          expr: |||
            sum by (%(alert_aggregation_labels)s) (rate(cortex_ingest_storage_strong_consistency_requests_total{component="partition-reader", with_offset="false"}[1m]))
            /
            sum by (%(alert_aggregation_labels)s) (rate(cortex_ingest_storage_strong_consistency_requests_total{component="partition-reader"}[1m]))
            * 100 > 5
          ||| % $._config,
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: '%(product)s ingesters in %(alert_aggregation_variables)s are receiving an unexpected high number of strongly consistent requests without an offset specified.' % $._config,
          },
        },

        // Alert firing if the Kafka client produce buffer utilization is consistently high.
        {
          alert: $.alertName('KafkaClientBufferedProduceBytesTooHigh'),
          'for': '5m',
          expr: |||
            max by(%(alert_aggregation_labels)s, %(per_instance_label)s) (max_over_time(cortex_ingest_storage_writer_buffered_produce_bytes{quantile="1.0"}[1m]))
            /
            min by(%(alert_aggregation_labels)s, %(per_instance_label)s) (min_over_time(cortex_ingest_storage_writer_buffered_produce_bytes_limit[1m]))
            * 100 > 50
          ||| % $._config,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: '%(product)s {{ $labels.%(per_instance_label)s }} in %(alert_aggregation_variables)s Kafka client produce buffer utilization is {{ printf "%%.2f" $value }}%%.' % $._config,
          },
        },

        // Alert if block-builder didn't process cycles in the past hour.
        {
          alert: $.alertName('BlockBuilderNoCycleProcessing'),
          'for': '20m',
          expr: |||
            max by(%(alert_aggregation_labels)s, %(per_instance_label)s) (histogram_count(increase(cortex_blockbuilder_consume_cycle_duration_seconds[60m]))) == 0
          ||| % $._config,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: '%(product)s {{ $labels.%(per_instance_label)s }} in %(alert_aggregation_variables)s has not processed cycles in the past hour.' % $._config,
          },
        },

        // Alert if block-builder per partition lag is higher than the threshhold.
        // The value of the threshhold is arbitary large for now. We will reconsider this alert after we get the block-builder-scheduler.
        // Note on "for: 75m": we assume one cycle is 1hr; with 10m loopback we expect the warning to trigger only if the metric is above the threshold for more than one cycle.
        {
          alert: $.alertName('BlockBuilderLagging'),
          'for': '75m',
          expr: |||
            max by(%(alert_aggregation_labels)s, %(per_instance_label)s) (max_over_time(cortex_blockbuilder_consumer_lag_records[10m])) > 4e6
          ||| % $._config,
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: '%(product)s {{ $labels.%(per_instance_label)s }} in %(alert_aggregation_variables)s reports partition lag of {{ printf "%%.2f" $value }}.' % $._config,
          },
        },
        {
          alert: $.alertName('BlockBuilderLagging'),
          'for': '140m',  // 2h20m. Indicating the lag did not come down for ~2 consumption cycles.
          expr: |||
            max by(%(alert_aggregation_labels)s, %(per_instance_label)s) (max_over_time(cortex_blockbuilder_consumer_lag_records[10m])) > 4e6
          ||| % $._config,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: '%(product)s {{ $labels.%(per_instance_label)s }} in %(alert_aggregation_variables)s reports partition lag of {{ printf "%%.2f" $value }}.' % $._config,
          },
        },

        // Alert immediately if block-builder is failing to compact and upload any blocks.
        {
          alert: $.alertName('BlockBuilderCompactAndUploadFailed'),
          expr: |||
            sum by (%(alert_aggregation_labels)s, %(per_instance_label)s) (rate(cortex_blockbuilder_tsdb_compact_and_upload_failed_total[1m])) > 0
          ||| % $._config,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: '%(product)s {{ $labels.%(per_instance_label)s }} in %(alert_aggregation_variables)s fails to compact and upload blocks.' % $._config,
          },
        },
      ],
    },
  ],

  groups+: $.withRunbookURL('https://grafana.com/docs/mimir/latest/operators-guide/mimir-runbooks/#%s', $.withExtraLabelsAnnotations(alertGroups)),
}
