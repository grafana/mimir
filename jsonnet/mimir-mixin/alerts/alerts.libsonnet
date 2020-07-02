{
  groups+: [
    {
      name: 'cortex_alerts',
      rules: [
        {
          alert: 'CortexIngesterUnhealthy',
          'for': '15m',
          expr: |||
            min by (%s) (cortex_ring_members{state="Unhealthy", name="ingester"}) > 0
          ||| % $._config.alert_aggregation_labels,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: 'There are {{ printf "%f" $value }} unhealthy ingester(s).',
          },
        },
        {
          alert: 'CortexRequestErrors',
          // Note is alert_aggregation_labels is "job", this will repeat the label.  But
          // prometheus seems to tolerate that.
          expr: |||
            100 * sum by (%s, job, route) (rate(cortex_request_duration_seconds_count{status_code=~"5.."}[1m]))
              /
            sum by (%s, job, route) (rate(cortex_request_duration_seconds_count[1m]))
              > 1
          ||| % [$._config.alert_aggregation_labels, $._config.alert_aggregation_labels],
          'for': '15m',
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: |||
              {{ $labels.job }} {{ $labels.route }} is experiencing {{ printf "%.2f" $value }}% errors.
            |||,
          },
        },
        {
          alert: 'CortexRequestLatency',
          expr: |||
            cluster_namespace_job_route:cortex_request_duration_seconds:99quantile{route!~"metrics|/frontend.Frontend/Process"}
               >
            %(cortex_p99_latency_threshold_seconds)s
          ||| % $._config,
          'for': '15m',
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: |||
              {{ $labels.job }} {{ $labels.route }} is experiencing {{ printf "%.2f" $value }}s 99th percentile latency.
            |||,
          },
        },
        {
          // We're syncing every 10mins, and this means with a 5min rate, we will have a NaN when syncs fail
          // and we will never trigger the alert.
          // We also have a 3h grace-period for creation of tables which means the we can fail for 3h before it's an outage.
          alert: 'CortexTableSyncFailure',
          expr: |||
            100 * rate(cortex_table_manager_sync_duration_seconds_count{status_code!~"2.."}[15m])
              /
            rate(cortex_table_manager_sync_duration_seconds_count[15m])
              > 10
          |||,
          'for': '30m',
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: |||
              {{ $labels.job }} is experiencing {{ printf "%.2f" $value }}% errors syncing tables.
            |||,
          },
        },
        {
          alert: 'CortexQueriesIncorrect',
          expr: |||
            100 * sum by (%s) (rate(test_exporter_test_case_result_total{result="fail"}[5m]))
              /
            sum by (%s) (rate(test_exporter_test_case_result_total[5m])) > 1
          ||| % [$._config.alert_aggregation_labels, $._config.alert_aggregation_labels],
          'for': '15m',
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: |||
              Incorrect results for {{ printf "%.2f" $value }}% of queries.
            |||,
          },
        },
        {
          // As of https://github.com/cortexproject/cortex/pull/2092, this metric is
          // only exposed when it is supposed to be non-zero, so we don't need to do
          // any special filtering on the job label.
          alert: 'CortexBadOverrides',
          expr: |||
            cortex_overrides_last_reload_successful == 0
          |||,
          'for': '15m',
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: |||
              {{ $labels.job }} failed to reload overrides.
            |||,
          },
        },
        {
          alert: 'CortexQuerierCapacityFull',
          expr: |||
            prometheus_engine_queries_concurrent_max{job=~".+/(cortex|ruler|querier)"} - prometheus_engine_queries{job=~".+/(cortex|ruler|querier)"} == 0
          |||,
          'for': '5m',  // We don't want to block for longer.
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: |||
              {{ $labels.job }} is at capacity processing queries.
            |||,
          },
        },
        {
          alert: 'CortexFrontendQueriesStuck',
          expr: |||
            sum by (%s) (cortex_query_frontend_queue_length) > 1
          ||| % $._config.alert_aggregation_labels,
          'for': '5m',  // We don't want to block for longer.
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: |||
              There are {{ $value }} queued up queries.
            |||,
          },
        },
        {
          alert: 'CortexCacheRequestErrors',
          expr: |||
            100 * sum by (%s, method) (rate(cortex_cache_request_duration_seconds_count{status_code=~"5.."}[1m]))
              /
            sum  by (%s, method) (rate(cortex_cache_request_duration_seconds_count[1m]))
              > 1
          ||| % [$._config.alert_aggregation_labels, $._config.alert_aggregation_labels],
          'for': '15m',
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: |||
              Cache {{ $labels.method }} is experiencing {{ printf "%.2f" $value }}% errors.
            |||,
          },
        },
        {
          alert: 'CortexIngesterRestarts',
          expr: |||
            changes(process_start_time_seconds{job=~".+(cortex|ingester)"}[30m]) > 1
          |||,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: '{{ $labels.job }}/{{ $labels.instance }} has restarted {{ printf "%.2f" $value }} times in the last 30 mins.',
          },
        },
        {
          alert: 'CortexTransferFailed',
          expr: |||
            max_over_time(cortex_shutdown_duration_seconds_count{op="transfer",status!="success"}[15m])
          |||,
          'for': '5m',
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: '{{ $labels.job }}/{{ $labels.instance }} transfer failed.',
          },
        },
        {
          alert: 'CortexOldChunkInMemory',
          // Even though we should flush chunks after 6h, we see that 99p of age of flushed chunks is closer
          // to 10 hours.
          // Ignore cortex_oldest_unflushed_chunk_timestamp_seconds that are zero (eg. distributors).
          expr: |||
            (time() - cortex_oldest_unflushed_chunk_timestamp_seconds > 36000)
              and
            (cortex_oldest_unflushed_chunk_timestamp_seconds > 0)
          |||,
          'for': '5m',
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: |||
              {{ $labels.job }}/{{ $labels.instance }} has very old unflushed chunk in memory.
            |||,
          },
        },
      ],
    },
    {
      name: 'cortex_wal_alerts',
      rules: [
        {
          // Alert immediately if WAL is corrupt.
          alert: 'CortexWALCorruption',
          expr: |||
            increase(cortex_ingester_wal_corruptions_total[5m]) > 0
          |||,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: |||
              {{ $labels.job }}/{{ $labels.instance }} has a corrupted WAL or checkpoint.
            |||,
          },
        },
        {
          // One or more failed checkpoint creation is a warning.
          alert: 'CortexCheckpointCreationFailed',
          expr: |||
            increase(cortex_ingester_checkpoint_creations_failed_total[10m]) > 0
          |||,
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: |||
              {{ $labels.job }}/{{ $labels.instance }} failed to create checkpoint.
            |||,
          },
        },
        {
          // Two or more failed checkpoint creation in 1h means something is wrong.
          alert: 'CortexCheckpointCreationFailed',
          expr: |||
            increase(cortex_ingester_checkpoint_creations_failed_total[1h]) > 1
          |||,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: |||
              {{ $labels.job }}/{{ $labels.instance }} is failing to create checkpoint.
            |||,
          },
        },
        {
          // One or more failed checkpoint deletion is a warning.
          alert: 'CortexCheckpointDeletionFailed',
          expr: |||
            increase(cortex_ingester_checkpoint_deletions_failed_total[10m]) > 0
          |||,
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: |||
              {{ $labels.job }}/{{ $labels.instance }} failed to delete checkpoint.
            |||,
          },
        },
        {
          // Two or more failed checkpoint deletion in 2h means something is wrong.
          // We give this more buffer than creation as this is a less critical operation.
          alert: 'CortexCheckpointDeletionFailed',
          expr: |||
            increase(cortex_ingester_checkpoint_deletions_failed_total[2h]) > 1
          |||,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: |||
              {{ $labels.instance }} is failing to delete checkpoint.
            |||,
          },
        },
      ],
    },
    {
      name: 'cortex-provisioning',
      rules: [
        {
          alert: 'CortexProvisioningMemcachedTooSmall',
          // 4 x in-memory series size = 24hrs of data.
          expr: |||
            (
              4 *
              sum by (%s) (cortex_ingester_memory_series * cortex_ingester_chunk_size_bytes_sum / cortex_ingester_chunk_size_bytes_count)
               / 1e9
            )
              >
            (
              sum by (%s) (memcached_limit_bytes{job=~".+/memcached"}) / 1e9
            )
          ||| % [$._config.alert_aggregation_labels, $._config.alert_aggregation_labels],
          'for': '15m',
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: |||
              Chunk memcached cluster is too small, should be at least {{ printf "%.2f" $value }}GB.
            |||,
          },
        },
        {
          alert: 'CortexProvisioningTooManyActiveSeries',
          // 1 million active series per ingester max.
          expr: |||
            avg by (%s) (cortex_ingester_memory_series) > 1.1e6
              and
            sum by (%s) (rate(cortex_ingester_received_chunks[1h])) == 0
          ||| % [$._config.alert_aggregation_labels, $._config.alert_aggregation_labels],
          'for': '1h',
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: |||
              Too many active series for ingesters, add more ingesters.
            |||,
          },
        },
        {
          alert: 'CortexProvisioningTooManyWrites',
          // 80k writes / s per ingester max.
          expr: |||
            avg by (%s) (rate(cortex_ingester_ingested_samples_total[1m])) > 80e3
          ||| % $._config.alert_aggregation_labels,
          'for': '15m',
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: |||
              High QPS for ingesters, add more ingesters.
            |||,
          },
        },
        {
          alert: 'CortexProvisioningTooMuchMemory',
          expr: |||
            avg by (%s) (
              container_memory_working_set_bytes{container_name="ingester"}
                /
              container_spec_memory_limit_bytes{container_name="ingester"}
            ) > 0.7
          ||| % $._config.alert_aggregation_labels,
          'for': '15m',
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: |||
              Too much memory being used by ingesters - add more ingesters.
            |||,
          },
        },
      ],
    },
    {
      name: 'ruler_alerts',
      rules: [
        {
          alert: 'CortexRulerNotConnectedToAlertmanagers',
          expr: 'max_over_time(cortex_prometheus_notifications_alertmanagers_discovered[1m]) < 1',
          'for': '5m',
          label: {
            severity: 'warning',
          },
          message: |||
            {{ $labels.instance }} is not connected to any Alertmanagers.
          |||,
        },
        {
          alert: 'CortexRulerErrorSendingAlertsToAnyAlertmanager',
          expr: |||
            min without(alertmanager) (
              rate(cortex_prometheus_notifications_errors_total[1m])
                /
              rate(cortex_prometheus_notifications_sent_total[1m])
            ) 
            * 100 
            > 3
          |||,
          'for': '5m',
          label: {
            severity: 'warning',
          },
          message: |||
            {{ printf "%.1f" $value }}% minimum errors while sending alerts from the Cortex Ruler {{$labels.instance}} to any Alertmanager.
          |||,
        },
        {
          alert: 'CortexRulerErrorSendingAlertsToSomeAlertmanagers',
          expr: |||
            (
              rate(cortex_prometheus_notifications_errors_total[1m]) 
                / 
              rate(cortex_prometheus_notifications_sent_total[1m])
            ) 
            * 100 
            > 1
          |||,
          'for': '5m',
          label: {
            severity: 'warning',
          },
          message: |||
            {{ printf "%.1f" $value }}% minimum errors while sending alerts from the Cortex Ruler {{$labels.instance}} to Alertmanager {{ $labels.alertmanager }}.
          |||,
        },
        {
          alert: 'CortexRulerNotificationQueueRunningFull',
          expr: |||
            (
              predict_linear(prometheus_notifications_queue_length[5m], 60 * 30)
              >
              min_over_time(prometheus_notifications_queue_capacity[5m])
            )
          |||,
          'for': '5m',
          label: {
            severity: 'warning',
          },
          message: |||
            Alert notification queue of Cortex Ruler {{$labels.instance}} is running full.
          |||,
        },
        {
          alert: 'CortexRulerFailedEvaluations',
          expr: |||
            sum by (%s, instance, rule_group) (rate(cortex_prometheus_rule_evaluation_failures_total[1m]))
              /
            sum by (%s, instance, rule_group) (rate(cortex_prometheus_rule_evaluations_total[1m]))
              > 0.01
          ||| % [$._config.alert_aggregation_labels, $._config.alert_aggregation_labels],
          'for': '5m',
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: |||
              Cortex Ruler {{ $labels.instance }} is experiencing {{ printf "%.2f" $value }}% errors for the rule group {{ $labels.rule_group }}
            |||,
          },
        },
        {
          alert: 'CortexRulerMissedEvaluations',
          expr: |||
            sum by (%s, instance, rule_group) (rate(cortex_prometheus_rule_group_iterations_missed_total[1m]))
              /
            sum by (%s, instance, rule_group) (rate(cortex_prometheus_rule_group_iterations_total[1m]))
              > 0.01
          ||| % [$._config.alert_aggregation_labels, $._config.alert_aggregation_labels],
          'for': '5m',
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: |||
              Cortex Ruler {{ $labels.instance }} is experiencing {{ printf "%.2f" $value }}% missed iterations for the rule group {{ $labels.rule_group }}.
            |||,
          },
        },
        {
          alert: 'CortexRulerFailedRingCheck',
          expr: |||
            sum by (%s) (rate(cortex_ruler_ring_check_errors_total[5m]))
               > 0
          ||| % $._config.alert_aggregation_labels,
          'for': '1m',
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: |||
              {{ $labels.job }} is experiencing {{ printf "%.2f" $value }}% errors when checking the ring for rule group ownership.
            |||,
          },
        },
      ],
    },
    {
      name: 'gossip_alerts',
      rules: [
        {
          alert: 'CortexGossipMembersMismatch',
          expr: |||
            memberlist_client_cluster_members_count
              != on (%s) group_left
            sum by (%s) (up{job=~".+/(distributor|ingester|querier|cortex|ruler)"})
          ||| % [$._config.alert_aggregation_labels, $._config.alert_aggregation_labels],
          'for': '5m',
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: '{{ $labels.job }}/{{ $labels.instance }} sees incorrect number of gossip members.',
          },
        },
      ],
    },
  ],
}
