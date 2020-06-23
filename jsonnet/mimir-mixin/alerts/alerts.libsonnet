(import 'alert-utils.libsonnet') {
  groups+: [
    {
      name: 'cortex_alerts',
      rules: [
        {
          alert: 'CortexIngesterUnhealthy',
          'for': '15m',
          expr: |||
            min(cortex_ring_members{state="Unhealthy", name="ingester" %s}) by (%s) > 0
          ||| % [$.namespace_matcher(','), $.aggregation_labels('namespace, job')],
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: '{{ $labels.job }} reports more than one unhealthy ingester.',
          },
        },
        {
          alert: 'CortexRequestErrors',
          expr: |||
            100 * sum(rate(cortex_request_duration_seconds_count{status_code=~"5.."}[1m])) by (%s, route)
              /
            sum(rate(cortex_request_duration_seconds_count[1m])) by (%s, route)
              > 1
          ||| % [$.aggregation_labels('namespace, job'), $.aggregation_labels('namespace, job')],
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
          ||| % [$.aggregation_labels('namespace, job'), $.aggregation_labels('namespace, job')],
          'for': '15m',
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: |||
              {{ $labels.job }} is reporting incorrect results for {{ printf "%.2f" $value }}% of queries.
            |||,
          },
        },
        {
          alert: 'CortexBadOverrides',
          expr: |||
            cortex_overrides_last_reload_successful{job!~".+/table-manager|.+/alertmanager" %s} == 0
          ||| % $.namespace_matcher(','),
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
            sum by (%s) (cortex_query_frontend_queue_length{%s}) > 1
          ||| % [$.aggregation_labels('namespace'), $.namespace_matcher('')],
          'for': '5m',  // We don't want to block for longer.
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: |||
              {{ $labels.job }} has {{ $value }} queued up queries.
            |||,
          },
        },
        {
          alert: 'CortexCacheRequestErrors',
          expr: |||
            100 * sum(rate(cortex_cache_request_duration_seconds_count{status_code=~"5.." %s}[1m])) by (%s, method)
              /
            sum(rate(cortex_cache_request_duration_seconds_count{%s}[1m])) by (%s, method)
              > 1
          ||| % [$.namespace_matcher(','), $.aggregation_labels('namespace, job'), $.namespace_matcher(), $.aggregation_labels('namespace, job')],
          'for': '15m',
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: |||
              {{ %s }} cache {{ $labels.method }} is experiencing {{ printf "%.2f" $value }}% errors.
            ||| % $.annotation_labels(),
          },
        },
        {
          alert: 'CortexIngesterRestarts',
          expr: |||
            rate(process_start_time_seconds{job=~".+(cortex|ingester)" %s}[30m]) > 0
          ||| % $.namespace_matcher(','),
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: '{{ %s }}/{{ %s }} is restarting',
          } % [$.annotation_labels(), $.annotation_labels('$labels.pod')],
        },
        {
          alert: 'CortexTransferFailed',
          expr: |||
            max_over_time(cortex_shutdown_duration_seconds_count{op="transfer",status!="success" %s}[15m])
          ||| % $.namespace_matcher(','),
          'for': '5m',
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: '{{ %s }}/{{ $labels.instance }} transfer failed.',
          } % $.annotation_labels(),
        },
        {
          alert: 'CortexOldChunkInMemory',
          // Even though we should flush chunks after 6h, we see that 99p of age of flushed chunks is closer
          // to 10 hours.
          // Ignore cortex_oldest_unflushed_chunk_timestamp_seconds that are zero (eg. distributors).
          expr: |||
            (time() - cortex_oldest_unflushed_chunk_timestamp_seconds > 36000) and cortex_oldest_unflushed_chunk_timestamp_seconds > 0
          |||,
          'for': '5m',
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: |||
              {{ %s }}/{{ $labels.instance }} has very old unflushed chunk in memory.
            ||| % $.annotation_labels(),
          },
        },
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
              {{ %s }}/{{ $labels.instance }} has a corrupted WAL or checkpoint.
            ||| % $.annotation_labels(),
          },
        },
        {
          // 1 failed checkpoint creation is a warning.
          alert: 'CortexCheckpointCreationFailed',
          expr: |||
            increase(cortex_ingester_checkpoint_creations_failed_total[10m]) > 0
          |||,
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: |||
              {{ %s }}/{{ $labels.instance }} failed to create checkpoint.
            ||| % $.annotation_labels(),
          },
        },
        {
          // 2 or more failed checkpoint creation in 1h means something is wrong.
          alert: 'CortexCheckpointCreationFailed',
          expr: |||
            increase(cortex_ingester_checkpoint_creations_failed_total[1h]) > 1
          |||,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: |||
              {{ %s }}/{{ $labels.instance }} is failing to create checkpoint.
            ||| % $.annotation_labels(),
          },
        },
        {
          // 1 failed checkpoint deletion is a warning.
          alert: 'CortexCheckpointDeletionFailed',
          expr: |||
            increase(cortex_ingester_checkpoint_deletions_failed_total[10m]) > 0
          |||,
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: |||
              {{ %s }}/{{ $labels.instance }} failed to delete checkpoint.
            ||| % $.annotation_labels(),
          },
        },
        {
          // 2 or more failed checkpoint deletion in 2h means something is wrong.
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
              {{ %s }}/{{ $labels.instance }} is failing to delete checkpoint.
            ||| % $.annotation_labels(),
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
              sum by(%s) (cortex_ingester_memory_series * cortex_ingester_chunk_size_bytes_sum / cortex_ingester_chunk_size_bytes_count)
               / 1e9
            )
              >
            (
              sum by(%s) (memcached_limit_bytes{job=~".+/memcached"}) / 1e9
            )
          ||| % [$.aggregation_labels('cluster, namespace'), $.aggregation_labels('cluster, namespace')],
          'for': '15m',
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: |||
              Chunk memcached cluster for {{ %s }} are too small, should be at least {{ printf "%.2f" $value }}GB.
            ||| % $.annotation_labels(),
          },
        },
        {
          alert: 'CortexProvisioningTooManyActiveSeries',
          // 1 million active series per ingester max.
          expr: |||
            avg by (%s) (cortex_ingester_memory_series) > 1.1e6
              and
            sum by (%s) (rate(cortex_ingester_received_chunks[1h])) == 0
          ||| % [$.aggregation_labels('cluster, namespace'), $.aggregation_labels('cluster, namespace')],
          'for': '1h',
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: |||
              Too many active series for ingesters in {{ %s }}, add more ingesters.
            ||| % $.annotation_labels(),
          },
        },
        {
          alert: 'CortexProvisioningTooManyWrites',
          // 80k writes / s per ingester max.
          expr: |||
            avg by (%s) (rate(cortex_ingester_ingested_samples_total[1m])) > 80e3
          ||| % $.aggregation_labels('cluster, namespace'),
          'for': '15m',
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: |||
              High QPS for ingesters in {{ %s }}, add more ingesters.
            ||| % $.annotation_labels(),
          },
        },
        {
          alert: 'CortexProvisioningTooMuchMemory',
          expr: |||
            avg by (%s) (container_memory_working_set_bytes{container_name="ingester" %s} / container_spec_memory_limit_bytes{container_name="ingester" %s}) > 0.7
          ||| % [$.aggregation_labels('cluster, namespace'), $.namespace_matcher(','), $.namespace_matcher(',')],
          'for': '15m',
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: |||
              Too much memory being used by ingesters for {{ %s }}, add more ingesters.
            ||| % $.annotation_labels(),
          },
        },
      ],
    },
    {
      name: 'ruler_alerts',
      rules: [
        {
          alert: 'CortexRulerFailedEvaluations',
          expr: |||
            sum(rate(cortex_prometheus_rule_evaluation_failures_total[1m])) by (%s)
              /
            sum(rate(cortex_prometheus_rule_evaluations_total[1m])) by (%s)
              > 0.01
          ||| % [$.aggregation_labels('namespace, job'), $.aggregation_labels('namespace, job')],
          'for': '5m',
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: |||
              {{ $labels.job }} is experiencing {{ printf "%.2f" $value }}% errors.
            |||,
          },
        },
        {
          alert: 'CortexRulerMissedEvaluations',
          expr: |||
            sum(rate(cortex_prometheus_rule_group_iterations_missed_total[1m])) by (%s)
              /
            sum(rate(cortex_prometheus_rule_group_iterations_total[1m])) by (%s)
              > 0.01
          |||  % [$.aggregation_labels('namespace, job'), $.aggregation_labels('namespace, job')],
          'for': '5m',
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: |||
              {{ $labels.job }} is experiencing {{ printf "%.2f" $value }}% missed iterations.
            |||,
          },
        },
        {
          alert: 'CortexRulerFailedRingCheck',
          expr: |||
            sum(rate(cortex_ruler_ring_check_errors_total[5m])) by (%s)
               > 0
          |||  % $.aggregation_labels('namespace, job'),
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
            memberlist_client_cluster_members_count{%s}
              != on (%s) group_left
            sum(up{job=~".+/(distributor|ingester|querier|cortex|ruler)"}) by (%s)
          ||| % [$.namespace_matcher(), $.aggregation_labels('cluster, namespace'), $.aggregation_labels('cluster, namespace')],
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
