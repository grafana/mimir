{
  // simpleRegexpOpt produces a simple regexp that matches all strings in the input array.
  local simpleRegexpOpt(strings) =
    assert std.isArray(strings) : 'simpleRegexpOpt requires that `strings` is an array of strings`';
    '(' + std.join('|', strings) + ')',

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
            message: 'Cortex cluster %(alert_aggregation_variables)s has {{ printf "%%f" $value }} unhealthy ingester(s).' % $._config,
          },
        },
        {
          alert: 'CortexRequestErrors',
          // Note if alert_aggregation_labels is "job", this will repeat the label. But
          // prometheus seems to tolerate that.
          expr: |||
            100 * sum by (%(group_by)s, job, route) (rate(cortex_request_duration_seconds_count{status_code=~"5..",route!~"%(excluded_routes)s"}[1m]))
              /
            sum by (%(group_by)s, job, route) (rate(cortex_request_duration_seconds_count{route!~"%(excluded_routes)s"}[1m]))
              > 1
          ||| % {
            group_by: $._config.alert_aggregation_labels,
            excluded_routes: std.join('|', ['ready'] + $._config.alert_excluded_routes),
          },
          'for': '15m',
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: |||
              The route {{ $labels.route }} in %(alert_aggregation_variables)s is experiencing {{ printf "%%.2f" $value }}%% errors.
            ||| % $._config,
          },
        },
        {
          alert: 'CortexRequestLatency',
          expr: |||
            %(group_prefix_jobs)s_route:cortex_request_duration_seconds:99quantile{route!~"%(excluded_routes)s"}
               >
            %(cortex_p99_latency_threshold_seconds)s
          ||| % $._config {
            excluded_routes: std.join('|', [
              'metrics',
              '/frontend.Frontend/Process',
              'ready',
              '/schedulerpb.SchedulerForFrontend/FrontendLoop',
              '/schedulerpb.SchedulerForQuerier/QuerierLoop',
            ] + $._config.alert_excluded_routes),
          },
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
              The Cortex cluster %(alert_aggregation_variables)s is experiencing {{ printf "%%.2f" $value }}%% incorrect query results.
            ||| % $._config,
          },
        },
        {
          alert: 'CortexInconsistentRuntimeConfig',
          expr: |||
            count(count by(%s, job, sha256) (cortex_runtime_config_hash)) without(sha256) > 1
          ||| % $._config.alert_aggregation_labels,
          'for': '1h',
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: |||
              An inconsistent runtime config file is used across cluster %(alert_aggregation_variables)s.
            ||| % $._config,
          },
        },
        {
          alert: 'CortexBadRuntimeConfig',
          expr: |||
            # The metric value is reset to 0 on error while reloading the config at runtime.
            cortex_runtime_config_last_reload_successful == 0
          |||,
          // Alert quicker for human errors.
          'for': '5m',
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: |||
              {{ $labels.job }} failed to reload runtime config.
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
              There are {{ $value }} queued up queries in %(alert_aggregation_variables)s query-frontend.
            ||| % $._config,
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
              There are {{ $value }} queued up queries in %(alert_aggregation_variables)s query-scheduler.
            ||| % $._config,
          },
        },
        {
          alert: 'CortexMemcachedRequestErrors',
          expr: |||
            (
              sum by(%s, name, operation) (rate(thanos_memcached_operation_failures_total[1m])) /
              sum by(%s, name, operation) (rate(thanos_memcached_operations_total[1m]))
            ) * 100 > 5
          ||| % [$._config.alert_aggregation_labels, $._config.alert_aggregation_labels],
          'for': '5m',
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: |||
              Memcached {{ $labels.name }} used by Cortex %(alert_aggregation_variables)s is experiencing {{ printf "%%.2f" $value }}%% errors for {{ $labels.operation }} operation.
            ||| % $._config,
          },
        },
        {
          alert: 'CortexIngesterRestarts',
          expr: |||
            changes(process_start_time_seconds{job=~".+(cortex|ingester.*)"}[30m]) >= 2
          |||,
          labels: {
            // This alert is on a cause not symptom. A couple of ingesters restarts may be suspicious but
            // not necessarily an issue (eg. may happen because of the K8S node autoscaler), so we're
            // keeping the alert as warning as a signal in case of an outage.
            severity: 'warning',
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
            ) > 0.8
          |||,
          'for': '3h',
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
            ) > 0.9
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
      name: 'cortex-rollout-alerts',
      rules: [
        {
          alert: 'CortexRolloutStuck',
          expr: |||
            (
              max without (revision) (
                kube_statefulset_status_current_revision
                  unless
                kube_statefulset_status_update_revision
              )
                *
              (
                kube_statefulset_replicas
                  !=
                kube_statefulset_status_replicas_updated
              )
            )  and (
              changes(kube_statefulset_status_replicas_updated[15m])
                ==
              0
            )
            * on(%s) group_left max by(%s) (cortex_build_info)
          ||| % [$._config.alert_aggregation_labels, $._config.alert_aggregation_labels],
          'for': '15m',
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: |||
              The {{ $labels.statefulset }} rollout is stuck in %(alert_aggregation_variables)s.
            ||| % $._config,
          },
        },
        {
          alert: 'CortexRolloutStuck',
          expr: |||
            (
              kube_deployment_spec_replicas
                !=
              kube_deployment_status_replicas_updated
            ) and (
              changes(kube_deployment_status_replicas_updated[15m])
                ==
              0
            )
            * on(%s) group_left max by(%s) (cortex_build_info)
          ||| % [$._config.alert_aggregation_labels, $._config.alert_aggregation_labels],
          'for': '15m',
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: |||
              The {{ $labels.deployment }} rollout is stuck in %(alert_aggregation_variables)s.
            ||| % $._config,
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
              Chunk memcached cluster in %(alert_aggregation_variables)s is too small, should be at least {{ printf "%%.2f" $value }}GB.
            ||| % $._config,
          },
        },
        {
          alert: 'CortexProvisioningTooManyActiveSeries',
          // We target each ingester to 1.5M in-memory series. This alert fires if the average
          // number of series / ingester in a Cortex cluster is > 1.6M for 2h (we compact
          // the TSDB head every 2h).
          expr: |||
            avg by (%s) (cortex_ingester_memory_series) > 1.6e6
          ||| % [$._config.alert_aggregation_labels],
          'for': '2h',
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: |||
              The number of in-memory series per ingester in %(alert_aggregation_variables)s is too high.
            ||| % $._config,
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
              Ingesters in %(alert_aggregation_variables)s ingest too many samples per second.
            ||| % $._config,
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
              Ingester {{ $labels.pod }} in %(alert_aggregation_variables)s is using too much memory.
            ||| % $._config,
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
              Ingester {{ $labels.pod }} in %(alert_aggregation_variables)s is using too much memory.
            ||| % $._config,
          },
        },
      ],
    },
    {
      name: 'ruler_alerts',
      rules: [
        {
          alert: 'CortexRulerTooManyFailedPushes',
          expr: |||
            100 * (
            sum by (%s, instance) (rate(cortex_ruler_write_requests_failed_total[1m]))
              /
            sum by (%s, instance) (rate(cortex_ruler_write_requests_total[1m]))
            ) > 1
          ||| % [$._config.alert_aggregation_labels, $._config.alert_aggregation_labels],
          'for': '5m',
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: |||
              Cortex Ruler {{ $labels.instance }} in %(alert_aggregation_variables)s is experiencing {{ printf "%%.2f" $value }}%% write (push) errors.
            ||| % $._config,
          },
        },
        {
          alert: 'CortexRulerTooManyFailedQueries',
          expr: |||
            100 * (
            sum by (%s, instance) (rate(cortex_ruler_queries_failed_total[1m]))
              /
            sum by (%s, instance) (rate(cortex_ruler_queries_total[1m]))
            ) > 1
          ||| % [$._config.alert_aggregation_labels, $._config.alert_aggregation_labels],
          'for': '5m',
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: |||
              Cortex Ruler {{ $labels.instance }} in %(alert_aggregation_variables)s is experiencing {{ printf "%%.2f" $value }}%% errors while evaluating rules.
            ||| % $._config,
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
              Cortex Ruler {{ $labels.instance }} in %(alert_aggregation_variables)s is experiencing {{ printf "%%.2f" $value }}%% missed iterations for the rule group {{ $labels.rule_group }}.
            ||| % $._config,
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
              Cortex Rulers in %(alert_aggregation_variables)s are experiencing errors when checking the ring for rule group ownership.
            ||| % $._config,
          },
        },
      ],
    },
    {
      name: 'gossip_alerts',
      rules: [
        {
          alert: 'CortexGossipMembersMismatch',
          expr:
            |||
              memberlist_client_cluster_members_count
                != on (%s) group_left
              sum by (%s) (up{job=~".+/%s"})
            ||| % [$._config.alert_aggregation_labels, $._config.alert_aggregation_labels, simpleRegexpOpt($._config.job_names.ring_members)],
          'for': '5m',
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: 'Cortex instance {{ $labels.instance }} in %(alert_aggregation_variables)s sees incorrect number of gossip members.' % $._config,
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
    {
      name: 'cortex-consul-alerts',
      rules: [
        {
          alert: 'CortexFailingToTalkToConsul',
          expr: |||
            (
              sum by(%s, pod, status_code, kv_name) (rate(cortex_consul_request_duration_seconds_count{status_code!~"2.+"}[1m]))
              /
              sum by(%s, pod, status_code, kv_name) (rate(cortex_consul_request_duration_seconds_count[1m]))
            )
            # We want to get alerted only in case there's a constant failure.
            == 1
          ||| % [$._config.alert_aggregation_labels, $._config.alert_aggregation_labels],
          'for': '5m',
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: |||
              Cortex {{ $labels.pod }} in  %(alert_aggregation_variables)s is failing to talk to Consul store ${{ labels.kv_name }}.
            ||| % $._config,
          },
        },
      ],
    },
  ],
}
