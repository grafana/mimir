local utils = import 'mixin-utils/utils.libsonnet';

(import 'alerts-utils.libsonnet') {
  local startingIngesterKafkaDelayGrowing(histogram_type) = {
    // This is an experiment. We compute derivation (ie. rate of consumption lag change) over 5 minutes. If derivation is above 0, it means consumption lag is increasing, instead of decreasing.
    alert: $.alertName('StartingIngesterKafkaDelayGrowing'),
    local sum_by = [$._config.alert_aggregation_labels, $._config.per_instance_label],
    local range_interval = $.alertRangeInterval(1),
    local numerator = utils.ncHistogramSumBy(utils.ncHistogramSumRate('cortex_ingest_storage_reader_receive_delay_seconds', 'phase="starting"', rate_interval=range_interval, from_recording=false), sum_by),
    local denominator = utils.ncHistogramSumBy(utils.ncHistogramCountRate('cortex_ingest_storage_reader_receive_delay_seconds', 'phase="starting"', rate_interval=range_interval, from_recording=false), sum_by),
    expr: |||
      deriv((
          %(numerator)s
          /
          %(denominator)s
      )[5m:1m]) > 0
    ||| % {
      numerator: numerator[histogram_type],
      denominator: denominator[histogram_type],
    },
    'for': '5m',
    labels: $.histogramLabels({ severity: 'warning' }, histogram_type, nhcb=false),
    annotations: {
      message: '%(product)s {{ $labels.%(per_instance_label)s }} in %(alert_aggregation_variables)s in "starting" phase is not reducing consumption lag of write requests read from Kafka.' % $._config,
    },
  },

  local runningIngesterReceiveDelayTooHigh(histogram_type, threshold_value, for_duration, threshold_label) = {
    alert: $.alertName('RunningIngesterReceiveDelayTooHigh'),
    local sum_by = [$._config.alert_aggregation_labels, $._config.per_instance_label],
    local range_interval = $.alertRangeInterval(1),
    local numerator = utils.ncHistogramSumBy(utils.ncHistogramSumRate('cortex_ingest_storage_reader_receive_delay_seconds', 'phase="running"', rate_interval=range_interval, from_recording=false), sum_by),
    local denominator = utils.ncHistogramSumBy(utils.ncHistogramCountRate('cortex_ingest_storage_reader_receive_delay_seconds', 'phase="running"', rate_interval=range_interval, from_recording=false), sum_by),
    expr: |||
      (
        %(numerator)s
        /
        %(denominator)s
      ) > %(threshold_value)s
    ||| % {
      numerator: numerator[histogram_type],
      denominator: denominator[histogram_type],
      threshold_value: threshold_value,
    },
    'for': for_duration,
    labels: $.histogramLabels({
      severity: 'critical',
      threshold: threshold_label,  // Add an extra label to distinguish between multiple alerts with the same name.
    }, histogram_type, nhcb=false),
    annotations: {
      message: '%(product)s {{ $labels.%(per_instance_label)s }} in %(alert_aggregation_variables)s in "running" phase is too far behind in its consumption of write requests from Kafka.' % $._config,
    },
  },

  // Alert firing if an ingester is ingesting data with a very high delay, even for a short period of time.
  // With a threshold of 2m, a for duration of 3m, an evaluation delay of 1m and an evaluation interval of 1m
  // when this alert fires the ingester should be up to 2+3+1+1=7 minutes behind.
  local runningIngesterReceiveDelayVeryHigh(histogram_type) = runningIngesterReceiveDelayTooHigh(histogram_type, '(2 * 60)', '3m', 'very_high_for_short_period'),

  // Alert firing if an ingester is ingesting data with a relatively high delay for a long period of time.
  local runningIngesterReceiveDelayRelativelyHigh(histogram_type) = runningIngesterReceiveDelayTooHigh(histogram_type, '30', '15m', 'relatively_high_for_long_period'),

  local alertGroups = [
    {
      name: 'mimir_ingest_storage_alerts',
      rules: [
        {
          alert: $.alertName('IngesterOffsetCommitFailed'),
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
          alert: $.alertName('IngesterKafkaReadFailed'),
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

        startingIngesterKafkaDelayGrowing('classic'),
        startingIngesterKafkaDelayGrowing('native'),
        runningIngesterReceiveDelayVeryHigh('classic'),
        runningIngesterReceiveDelayVeryHigh('native'),
        runningIngesterReceiveDelayRelativelyHigh('classic'),
        runningIngesterReceiveDelayRelativelyHigh('native'),

        // Alert firing if an ingester is failing to read from Kafka.
        {
          alert: $.alertName('IngesterKafkaProcessingFailed'),
          'for': '5m',
          expr: |||
            (
              sum by (%(alert_aggregation_labels)s, %(per_instance_label)s) (
                  # This is the old metric name. We're keeping support for backward compatibility.
                rate(cortex_ingest_storage_reader_records_failed_total{cause="server"}[1m])
                or
                rate(cortex_ingest_storage_reader_requests_failed_total{cause="server"}[1m])
              ) > 0
            )

            # Tolerate failures during the forced TSDB head compaction, because samples older than the
            # new "head min time" will fail to be appended while the forced compaction is running.
            unless (max by (%(alert_aggregation_labels)s, %(per_instance_label)s) (max_over_time(cortex_ingester_tsdb_forced_compactions_in_progress[1m])) > 0)
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
          alert: $.alertName('IngesterKafkaProcessingStuck'),
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
          alert: $.alertName('StrongConsistencyOffsetMissing'),
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
          alert: $.alertName('KafkaClientProduceBufferHigh'),
          'for': '5m',
          expr: |||
            max by(%(alert_aggregation_labels)s, %(per_instance_label)s) (
                # New metric.
                max_over_time(cortex_ingest_storage_writer_buffered_produce_bytes_distribution{quantile="1.0"}[1m])
                or
                # Old metric.
                max_over_time(cortex_ingest_storage_writer_buffered_produce_bytes{quantile="1.0"}[1m])
            )
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

        // Alert if block-builder scheduler reports pending jobs for extended period of time.
        {
          alert: $.alertName('BlockBuilderSchedulerPendingJobs'),
          'for': '40m',
          expr: |||
            sum by (%(alert_aggregation_labels)s, %(per_instance_label)s) (cortex_blockbuilder_scheduler_pending_jobs) > 0
          ||| % $._config,
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: '%(product)s {{ $labels.%(per_instance_label)s }} in %(alert_aggregation_variables)s reports {{ printf "%%.2f" $value }} pending jobs.' % $._config,
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

        // Alert if block-builder didn't ship blocks in the past hours.
        // This is similar to the blocks shipment alerts from the ingesters.
        {
          alert: $.alertName('BlockBuilderHasNotShippedBlocks'),
          'for': '15m',
          expr: |||
            (min by(%(alert_aggregation_labels)s, %(per_instance_label)s) (time() - cortex_blockbuilder_tsdb_last_successful_compact_and_upload_timestamp_seconds) > 60 * 60 * 4)
            and
            (max by(%(alert_aggregation_labels)s, %(per_instance_label)s) (cortex_blockbuilder_tsdb_last_successful_compact_and_upload_timestamp_seconds) > 0)
            # Only if blocks aren't shipped by ingesters.
            unless on (%(alert_aggregation_labels)s)
            (max by (%(alert_aggregation_labels)s) (cortex_ingester_shipper_last_successful_upload_timestamp_seconds) > 0)
          ||| % $._config,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: '%(product)s {{ $labels.%(per_instance_label)s }} in %(alert_aggregation_variables)s has not shipped any block in the last 4 hours.' % $._config,
          },
        },

        // Alert if block-builder-scheduler primary runloop doesn't seem to be running.
        {
          alert: $.alertName('BlockBuilderSchedulerNotRunning'),
          'for': '30m',
          // We are taking advantage of Prometheus' behavior where it will only report an increase of zero
          // if the series was previously present. Thus we do not need to predicate the alert on presence of
          // a block-builder-scheduler.
          expr: |||
            max by (%(alert_aggregation_labels)s, %(per_instance_label)s) (histogram_count(increase(cortex_blockbuilder_scheduler_schedule_update_seconds[5m])) == 0)
          ||| % $._config,
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: '%(product)s {{ $labels.%(per_instance_label)s }} in %(alert_aggregation_variables)s is not running.' % $._config,
          },
        },

        // Alert immediately if block-builder-scheduler detects an unexpected offset gap.
        {
          alert: $.alertName('BlockBuilderDataSkipped'),
          expr: |||
            sum by (%(alert_aggregation_labels)s, %(per_instance_label)s) (
              increase(cortex_blockbuilder_scheduler_job_gap_detected[1h])
            ) > 0
          ||| % $._config,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: '%(product)s {{ $labels.%(per_instance_label)s }} in %(alert_aggregation_variables)s has detected skipped data.' % $._config,
          },
        },

        // Alert immediately if block-builder-scheduler detects a persistently failing job.
        {
          alert: $.alertName('BlockBuilderPersistentJobFailure'),
          expr: |||
            increase(cortex_blockbuilder_scheduler_persistent_job_failures_total[1m]) > 0
          ||| % $._config,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: '%(product)s {{ $labels.%(per_instance_label)s }} in %(alert_aggregation_variables)s has detected a persistently failing job.' % $._config,
          },
        },

        // Alert if the number of ingesters consuming partitions is less than the number of active partitions.
        {
          alert: $.alertName('FewerIngestersConsumingThanActivePartitions'),
          expr: |||
            max(cortex_partition_ring_partitions{name="ingester-partitions", state="Active"}) by (%(alert_aggregation_labels)s) > count(count(cortex_ingest_storage_reader_last_consumed_offset{}) by (%(alert_aggregation_labels)s, partition)) by (%(alert_aggregation_labels)s)
          ||| % $._config,
          'for': '15m',
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: '%(product)s ingesters in %(alert_aggregation_variables)s have fewer ingesters consuming than active partitions.' % $._config,
          },
        },
      ],
    },
  ],

  groups+: $.withRunbookURL('https://grafana.com/docs/mimir/latest/operators-guide/mimir-runbooks/#%s', $.withExtraLabelsAnnotations(alertGroups)),
}
