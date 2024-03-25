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
            deriv(histogram_quantile(0.99, sum by (%(alert_aggregation_labels)s, %(per_instance_label)s) (rate(cortex_ingest_storage_reader_receive_delay_seconds{phase="starting"}[1m])))[5m:1m]) > 0
          ||| % $._config,
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: '%(product)s {{ $labels.%(per_instance_label)s }} in %(alert_aggregation_variables)s in "starting" phase is not reducing consumption lag of write requests read from Kafka.' % $._config,
          },
        },

        {
          alert: $.alertName('RunningIngesterReceiveDelayTooHigh'),
          'for': '5m',
          expr: |||
            histogram_quantile(0.99, sum by (%(alert_aggregation_labels)s, %(per_instance_label)s) (rate(cortex_ingest_storage_reader_receive_delay_seconds{phase="running"}[1m]))) > (10*60)
          ||| % $._config,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: '%(product)s {{ $labels.%(per_instance_label)s }} in %(alert_aggregation_variables)s in "running" phase is too far behind in its consumption of write requests from Kafka.' % $._config,
          },
        },

        {
          alert: $.alertName('IngesterFailsToProcessRecordsFromKafka'),
          'for': '5m',
          expr: |||
            sum by (%(alert_aggregation_labels)s, %(per_instance_label)s) (rate(cortex_ingest_storage_reader_records_failed_total{cause="server"}[1m]) > 0
          ||| % $._config,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: '%(product)s {{ $labels.%(per_instance_label)s }} in %(alert_aggregation_variables)s fails to consume write requests read from Kafka due to internal errors.' % $._config,
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
