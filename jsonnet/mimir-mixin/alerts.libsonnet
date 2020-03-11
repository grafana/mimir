// According to https://developers.soundcloud.com/blog/alerting-on-slos :
local windows = [
  { long_period: '1h', short_period: '5m', for_period: '2m', factor: 14.4, severity: 'critical' },
  { long_period: '6h', short_period: '30m', for_period: '15m', factor: 6, severity: 'critical' },
  { long_period: '1d', short_period: '2h', for_period: '1h', factor: 3, severity: 'warning' },
  { long_period: '3d', short_period: '6h', for_period: '3h', factor: 1, severity: 'warning' },
];

{
  _config+:: {
    cortex_p99_latency_threshold_seconds: 2.5,
    alert_namespace_matcher: '',
  },

  prometheus_alerts+:: {
    local namespace_matcher(prefix='') =
      if std.length($._config.alert_namespace_matcher) != 0
      then '%s namespace=~"%s"' % [prefix, $._config.alert_namespace_matcher]
      else '',
    groups+: [
      {
        name: 'cortex_alerts',
        rules: [
          {
            alert: 'CortexIngesterUnhealthy',
            'for': '15m',
            expr: |||
              min(cortex_ring_members{state="Unhealthy", job=~"[a-z]+/distributor" %s}) by (namespace, job) > 0
            ||| % namespace_matcher(','),
            labels: {
              severity: 'critical',
            },
            annotations: {
              message: '{{ $labels.job }} reports more than one unhealthy ingester.',
            },
          },
          {
            alert: 'CortexFlushStuck',
            expr: |||
              (cortex_ingester_memory_chunks / cortex_ingester_memory_series) > 1.3
            |||,
            'for': '15m',
            labels: {
              severity: 'critical',
            },
            annotations: {
              message: '{{ $labels.job }}/{{ $labels.instance }} is stuck flushing chunks.',
            },
          },
          {
            alert: 'CortexRequestErrors',
            expr: |||
              100 * sum(rate(cortex_request_duration_seconds_count{status_code=~"5.."}[1m])) by (namespace, job, route)
                /
              sum(rate(cortex_request_duration_seconds_count[1m])) by (namespace, job, route)
                > 1
            |||,
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
              100 * rate(cortex_dynamo_sync_tables_seconds_count{status_code!~"2.."}[15m])
                /
              rate(cortex_dynamo_sync_tables_seconds_count[15m])
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
              100 * sum by (job, namespace) (rate(test_exporter_test_case_result_total{result="fail"}[5m]))
                /
              sum by (job, namespace) (rate(test_exporter_test_case_result_total[5m])) > 1
            |||,
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
            ||| % namespace_matcher(','),
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
              prometheus_engine_queries_concurrent_max{job=~".+/querier"} - prometheus_engine_queries{job=~".+/querier"} == 0
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
              sum by (namespace) (cortex_query_frontend_queue_length{job=~".+/query-frontend" %s}) > 1
            ||| % namespace_matcher(','),
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
              100 * sum(rate(cortex_cache_request_duration_seconds_count{status_code=~"5.." %s}[1m])) by (namespace, job, method)
                /
              sum(rate(cortex_cache_request_duration_seconds_count{%s}[1m])) by (namespace, job, method)
                > 1
            ||| % [namespace_matcher(','), namespace_matcher()],
            'for': '15m',
            labels: {
              severity: 'warning',
            },
            annotations: {
              message: |||
                {{ $labels.job }} cache {{ $labels.method }} is experiencing {{ printf "%.2f" $value }}% errors.
              |||,
            },
          },
          {
            alert: 'CortexIngesterRestarts',
            expr: |||
              rate(kube_pod_container_status_restarts_total{container="ingester" %s}[30m]) > 0
            ||| % namespace_matcher(','),
            labels: {
              severity: 'critical',
            },
            annotations: {
              message: '{{ $labels.namespace }}/{{ $labels.pod }} is restarting',
            },
          },
          {
            alert: 'CortexTransferFailed',
            expr: |||
              max_over_time(cortex_shutdown_duration_seconds_count{op="transfer",status!="success" %s}[15m])
            ||| % namespace_matcher(','),
            'for': '5m',
            labels: {
              severity: 'critical',
            },
            annotations: {
              message: '{{ $labels.namespace }}/{{ $labels.instance }} transfer failed.',
            },
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
                {{ $labels.namespace }}/{{ $labels.instance }} has very old unflushed chunk in memory.
              |||,
            },
          },
        ],
      },
      {
        name: 'cortex_slo_alerts',
        rules: [
          {
            alert: 'CortexWriteErrorBudgetBurn',
            expr: |||
              (
                (
                100 * namespace_job:cortex_gateway_write_slo_errors_per_request:ratio_rate%(long_period)s
                > 0.1 * %(factor)f
                )
              and
                (
                100 * namespace_job:cortex_gateway_write_slo_errors_per_request:ratio_rate%(short_period)s
                > 0.1 * %(factor)f
                )
              )
            ||| % window,
            'for': window.for_period,
            labels: {
              severity: window.severity,
              period: window.long_period,  // The annotation alone doesn't make this alert unique.
            },
            annotations: {
              summary: 'Cortex burns its write error budget too fast.',
              description: "{{ $value | printf `%%.2f` }}%% of {{ $labels.job }}'s write requests in the last %(long_period)s are failing or too slow to meet the SLO." % window,
            },
          }
          for window in windows
        ] + [
          {
            alert: 'CortexReadErrorBudgetBurn',
            expr: |||
              (
                (
                100 * namespace_job:cortex_gateway_read_slo_errors_per_request:ratio_rate%(long_period)s
                > 0.5 * %(factor)f
                )
              and
                (
                100 * namespace_job:cortex_gateway_read_slo_errors_per_request:ratio_rate%(short_period)s
                > 0.5 * %(factor)f
                )
              )
            ||| % window,
            'for': window.for_period,
            labels: {
              severity: window.severity,
              period: window.long_period,  // The annotation alone doesn't make this alert unique.
            },
            annotations: {
              summary: 'Cortex burns its read error budget too fast.',
              description: "{{ $value | printf `%%.2f` }}%% of {{ $labels.job }}'s read requests in the last %(long_period)s are failing or too slow to meet the SLO." % window,
            },
          }
          for window in windows
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
                sum by(cluster, namespace) (cortex_ingester_memory_series{job=~".+/ingester"} * cortex_ingester_chunk_size_bytes_sum{job=~".+/ingester"} / cortex_ingester_chunk_size_bytes_count{job=~".+/ingester"})
                 / 1e9
              )
                >
              (
                sum by (cluster, namespace) (memcached_limit_bytes{job=~".+/memcached"}) / 1e9
              )
            |||,
            'for': '15m',
            labels: {
              severity: 'warning',
            },
            annotations: {
              message: |||
                Chunk memcached cluster for namespace {{ $labels.namespace }} are too small, should be at least {{ printf "%.2f" $value }}GB.
              |||,
            },
          },
          {
            alert: 'CortexProvisioningTooManyActiveSeries',
            // 1 million active series per ingester max.
            expr: |||
              avg by (cluster, namespace) (cortex_ingester_memory_series{job=~".+/ingester"}) > 1.1e6
                and
              sum by (cluster, namespace) (rate(cortex_ingester_received_chunks{job=~".+/ingester"}[1h])) == 0
            |||,
            'for': '1h',
            labels: {
              severity: 'warning',
            },
            annotations: {
              message: |||
                Too many active series for ingesters in namespace {{ $labels.namespace }}, add more ingesters.
              |||,
            },
          },
          {
            alert: 'CortexProvisioningTooManyWrites',
            // 80k writes / s per ingester max.
            expr: |||
              avg by (cluster,namespace) (rate(cortex_ingester_ingested_samples_total[1m])) > 80e3
            |||,
            'for': '15m',
            labels: {
              severity: 'warning',
            },
            annotations: {
              message: |||
                Too much write QPS for ingesters in namespace {{ $labels.namespace }}, add more ingesters.
              |||,
            },
          },
          {
            alert: 'CortexProvisioningTooMuchMemory',
            expr: |||
              avg by (cluster, namespace) (container_memory_working_set_bytes{container_name="ingester" %s} / container_spec_memory_limit_bytes{container_name="ingester" %s}) > 0.7
            ||| % [namespace_matcher(','), namespace_matcher(',')],
            'for': '15m',
            labels: {
              severity: 'critical',
            },
            annotations: {
              message: |||
                Too much memory being used by ingesters in namespace {{ $labels.namespace }}, add more ingesters.
              |||,
            },
          },
        ],
      },
      {
        name: 'memcached',
        rules: [
          {
            alert: 'MemcachedDown',
            expr: |||
              memcached_up == 0
            |||,
            'for': '15m',
            labels: {
              severity: 'critical',
            },
            annotations: {
              message: |||
                Memcached Instance {{ $labels.instance }} is down for more than 15mins.
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
              sum(rate(cortex_prometheus_rule_evaluation_failures_total[1m])) by (namespace, job)
                /
              sum(rate(cortex_prometheus_rule_evaluations_total[1m])) by (namespace, job)
                > 0.01
            |||,
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
              sum(rate(cortex_prometheus_rule_group_iterations_missed_total[1m])) by (namespace, job)
                /
              sum(rate(cortex_prometheus_rule_group_iterations_total[1m])) by (namespace, job)
                > 0.01
            |||,
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
        ],
      },
      {
        name: 'gossip_alerts',
        rules: [
          {
            alert: 'CortexGossipMembersMismatch',
            expr: |||
              memberlist_client_cluster_members_count{%s}
                != on (cluster,namespace) group_left
              sum(up{job=~".+/(distributor|ingester|querier)"}) by (cluster,namespace)
            ||| % namespace_matcher(),
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
  },
}
