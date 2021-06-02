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
          // Note if alert_aggregation_labels is "job", this will repeat the label. But
          // prometheus seems to tolerate that.
          expr: |||
            100 * sum by (%s, job, route) (rate(cortex_request_duration_seconds_count{status_code=~"5..",route!~"ready"}[1m]))
              /
            sum by (%s, job, route) (rate(cortex_request_duration_seconds_count{route!~"ready"}[1m]))
              > 1
          ||| % [$._config.alert_aggregation_labels, $._config.alert_aggregation_labels],
          'for': '15m',
          labels: {
            severity: 'critical',
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
            %(job_aggregation_prefix)s_route:cortex_request_duration_seconds:99quantile{route!~"metrics|/frontend.Frontend/Process|ready|/schedulerpb.SchedulerForFrontend/FrontendLoop|/schedulerpb.SchedulerForQuerier/QuerierLoop"}
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
          alert: 'CortexInconsistentConfig',
          expr: |||
            count(count by(%s, job, sha256) (cortex_config_hash)) without(sha256) > 1
          ||| % $._config.alert_aggregation_labels,
          'for': '1h',
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: |||
              An inconsistent config file hash is used across cluster {{ $labels.job }}.
            |||,
          },
        },
        {
          // As of https://github.com/cortexproject/cortex/pull/2092, this metric is
          // only exposed when it is supposed to be non-zero, so we don't need to do
          // any special filtering on the job label.
          // The metric itself was renamed in
          // https://github.com/cortexproject/cortex/pull/2874
          //
          // TODO: Remove deprecated metric name of
          // cortex_overrides_last_reload_successful in the future
          alert: 'CortexBadRuntimeConfig',
          expr: |||
            cortex_runtime_config_last_reload_successful == 0
              or
            cortex_overrides_last_reload_successful == 0
          |||,
          // Alert quicker for human errors.
          'for': '5m',
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: |||
              {{ $labels.job }} failed to reload runtime config.
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
              There are {{ $value }} queued up queries in query-frontend.
            |||,
          },
        },
        {
          alert: 'CortexSchedulerQueriesStuck',
          expr: |||
            sum by (%s) (cortex_query_scheduler_queue_length) > 1
          ||| % $._config.alert_aggregation_labels,
          'for': '5m',  // We don't want to block for longer.
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: |||
              There are {{ $value }} queued up queries in query-scheduler.
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
            changes(process_start_time_seconds{job=~".+(cortex|ingester.*)"}[30m]) > 1
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
        {
          alert: 'CortexMemoryMapAreasTooHigh',
          expr: |||
            process_memory_map_areas{job=~".+(cortex|ingester.*|store-gateway)"} / process_memory_map_areas_limit{job=~".+(cortex|ingester.*|store-gateway)"} > 0.8
          |||,
          'for': '5m',
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: '{{ $labels.job }}/{{ $labels.instance }} has a number of mmap-ed areas close to the limit.',
          },
        },
      ],
    },
    {
      name: 'cortex_ingester_instance_alerts',
      rules: [
        {
          alert: 'CortexIngesterReachingSeriesLimit',
          expr: |||
            (
                (cortex_ingester_memory_series / ignoring(limit) cortex_ingester_instance_limits{limit="max_series"})
                and ignoring (limit)
                (cortex_ingester_instance_limits{limit="max_series"} > 0)
            ) > 0.7
          |||,
          'for': '5m',
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: |||
              Ingester {{ $labels.job }}/{{ $labels.instance }} has reached {{ $value | humanizePercentage }} of its series limit.
            |||,
          },
        },
        {
          alert: 'CortexIngesterReachingSeriesLimit',
          expr: |||
            (
                (cortex_ingester_memory_series / ignoring(limit) cortex_ingester_instance_limits{limit="max_series"})
                and ignoring (limit)
                (cortex_ingester_instance_limits{limit="max_series"} > 0)
            ) > 0.8
          |||,
          'for': '5m',
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: |||
              Ingester {{ $labels.job }}/{{ $labels.instance }} has reached {{ $value | humanizePercentage }} of its series limit.
            |||,
          },
        },
        {
          alert: 'CortexIngesterReachingTenantsLimit',
          expr: |||
            (
                (cortex_ingester_memory_users / ignoring(limit) cortex_ingester_instance_limits{limit="max_tenants"})
                and ignoring (limit)
                (cortex_ingester_instance_limits{limit="max_tenants"} > 0)
            ) > 0.7
          |||,
          'for': '5m',
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: |||
              Ingester {{ $labels.job }}/{{ $labels.instance }} has reached {{ $value | humanizePercentage }} of its tenant limit.
            |||,
          },
        },
        {
          alert: 'CortexIngesterReachingTenantsLimit',
          expr: |||
            (
                (cortex_ingester_memory_users / ignoring(limit) cortex_ingester_instance_limits{limit="max_tenants"})
                and ignoring (limit)
                (cortex_ingester_instance_limits{limit="max_tenants"} > 0)
            ) > 0.8
          |||,
          'for': '5m',
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: |||
              Ingester {{ $labels.job }}/{{ $labels.instance }} has reached {{ $value | humanizePercentage }} of its tenant limit.
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
          // 1.5 million active series per ingester max.
          expr: |||
            avg by (%s) (cortex_ingester_memory_series) > 1.6e6
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
          alert: 'CortexAllocatingTooMuchMemory',
          expr: |||
            (
              container_memory_working_set_bytes{container="ingester"}
                /
              container_spec_memory_limit_bytes{container="ingester"}
            ) > 0.65
          |||,
          'for': '15m',
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: |||
              Too much memory being used by {{ $labels.namespace }}/{{ $labels.pod }} - add more ingesters.
            |||,
          },
        },
        {
          alert: 'CortexAllocatingTooMuchMemory',
          expr: |||
            (
              container_memory_working_set_bytes{container="ingester"}
                /
              container_spec_memory_limit_bytes{container="ingester"}
            ) > 0.8
          |||,
          'for': '15m',
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: |||
              Too much memory being used by {{ $labels.namespace }}/{{ $labels.pod }} - add more ingesters.
            |||,
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
              Cortex Ruler {{ $labels.instance }} is experiencing {{ printf "%.2f" $value }}% errors for the rule group {{ $labels.rule_group }}.
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
            sum by (%s, job) (rate(cortex_ruler_ring_check_errors_total[1m]))
               > 0
          ||| % $._config.alert_aggregation_labels,
          'for': '5m',
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: |||
              Cortex Rulers {{ $labels.job }} are experiencing errors when checking the ring for rule group ownership.
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
            sum by (%s) (up{job=~".+/(distributor|ingester.*|querier|cortex|ruler)"})
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
    {
      name: 'etcd_alerts',
      rules: [
        {
          alert: 'EtcdAllocatingTooMuchMemory',
          expr: |||
            (
              container_memory_working_set_bytes{container="etcd"}
                /
              container_spec_memory_limit_bytes{container="etcd"}
            ) > 0.65 
          |||,
          'for': '15m',
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: |||
              Too much memory being used by {{ $labels.namespace }}/{{ $labels.pod }} - bump memory limit.
            |||,
          },
        },
        {
          alert: 'EtcdAllocatingTooMuchMemory',
          expr: |||
            (
              container_memory_working_set_bytes{container="etcd"}
                /
              container_spec_memory_limit_bytes{container="etcd"}
            ) > 0.8
          |||,
          'for': '15m',
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: |||
              Too much memory being used by {{ $labels.namespace }}/{{ $labels.pod }} - bump memory limit.
            |||,
          },
        },
      ],
    },
  ],
}
