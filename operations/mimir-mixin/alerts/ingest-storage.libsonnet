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
            sum by (%(alert_aggregation_labels)s, %(per_instance_label)s) (rate(cortex_ingest_storage_reader_records_failed_total{cause="server"}[1m])) > 0
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
            (sum by (%(alert_aggregation_labels)s, %(per_instance_label)s) (rate(cortex_ingest_storage_reader_records_total[5m])) == 0)
            and
            # NOTE: the cortex_ingest_storage_reader_buffered_fetch_records_total metric is a gauge showing the current number of buffered records.
            (sum by (%(alert_aggregation_labels)s, %(per_instance_label)s) (cortex_ingest_storage_reader_buffered_fetch_records_total) > 0)
          ||| % $._config,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: '%(product)s {{ $labels.%(per_instance_label)s }} in %(alert_aggregation_variables)s is stuck processing write requests from Kafka.' % $._config,
          },
        },

        {
          alert: $.alertName('IngesterFailsEnforceStrongConsistencyOnReadPath'),
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
      ],
    },
  ],

  groups+: $.withRunbookURL('https://grafana.com/docs/mimir/latest/operators-guide/mimir-runbooks/#%s', $.withExtraLabelsAnnotations(alertGroups)),
}
