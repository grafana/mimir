local utils = import 'mixin-utils/utils.libsonnet';

(import 'alerts-utils.libsonnet') {
  // simpleRegexpOpt produces a simple regexp that matches all strings in the input array.
  local simpleRegexpOpt(strings) =
    assert std.isArray(strings) : 'simpleRegexpOpt requires that `strings` is an array of strings`';
    '(' + std.join('|', strings) + ')',

  local groupDeploymentByRolloutGroup(metricName) =
    'sum without(deployment) (label_replace(%s, "rollout_group", "$1", "deployment", "(.*?)(?:-zone-[a-z])?"))' % metricName,

  local groupStatefulSetByRolloutGroup(metricName) =
    'sum without(statefulset) (label_replace(%s, "rollout_group", "$1", "statefulset", "(.*?)(?:-zone-[a-z])?"))' % metricName,

  local alertGroups = [
    {
      name: 'mimir_alerts',
      rules: [
        {
          alert: $.alertName('IngesterUnhealthy'),
          'for': '15m',
          expr: |||
            min by (%s) (cortex_ring_members{state="Unhealthy", name="ingester"}) > 0
          ||| % $._config.alert_aggregation_labels,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: '%(product)s cluster %(alert_aggregation_variables)s has {{ printf "%%f" $value }} unhealthy ingester(s).' % $._config,
          },
        },
        {
          alert: $.alertName('RequestErrors'),
          // Note if alert_aggregation_labels is "job", this will repeat the label. But
          // prometheus seems to tolerate that.
          expr: |||
            100 * sum by (%(group_by)s, %(job_label)s, route) (rate(cortex_request_duration_seconds_count{status_code=~"5..",route!~"%(excluded_routes)s"}[1m]))
              /
            sum by (%(group_by)s, %(job_label)s, route) (rate(cortex_request_duration_seconds_count{route!~"%(excluded_routes)s"}[1m]))
              > 1
          ||| % {
            group_by: $._config.alert_aggregation_labels,
            job_label: $._config.per_job_label,
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
          alert: $.alertName('RequestLatency'),
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
              {{ $labels.%(per_job_label)s }} {{ $labels.route }} is experiencing {{ printf "%%.2f" $value }}s 99th percentile latency.
            ||| % $._config,
          },
        },
        {
          alert: $.alertName('QueriesIncorrect'),
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
              The %(product)s cluster %(alert_aggregation_variables)s is experiencing {{ printf "%%.2f" $value }}%% incorrect query results.
            ||| % $._config,
          },
        },
        {
          alert: $.alertName('InconsistentRuntimeConfig'),
          expr: |||
            count(count by(%(alert_aggregation_labels)s, %(per_job_label)s, sha256) (cortex_runtime_config_hash)) without(sha256) > 1
          ||| % $._config,
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
          alert: $.alertName('BadRuntimeConfig'),
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
              {{ $labels.%(per_job_label)s }} failed to reload runtime config.
            ||| % $._config,
          },
        },
        {
          alert: $.alertName('FrontendQueriesStuck'),
          expr: |||
            sum by (%(alert_aggregation_labels)s, %(per_job_label)s) (min_over_time(cortex_query_frontend_queue_length[1m])) > 0
          ||| % $._config,
          'for': '5m',  // We don't want to block for longer.
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: |||
              There are {{ $value }} queued up queries in %(alert_aggregation_variables)s {{ $labels.%(per_job_label)s }}.
            ||| % $._config,
          },
        },
        {
          alert: $.alertName('SchedulerQueriesStuck'),
          expr: |||
            sum by (%(alert_aggregation_labels)s, %(per_job_label)s) (min_over_time(cortex_query_scheduler_queue_length[1m])) > 0
          ||| % $._config,
          'for': '7m',  // We don't want to block for longer.
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: |||
              There are {{ $value }} queued up queries in %(alert_aggregation_variables)s {{ $labels.%(per_job_label)s }}.
            ||| % $._config,
          },
        },
        {
          alert: $.alertName('MemcachedRequestErrors'),
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
              Memcached {{ $labels.name }} used by %(product)s %(alert_aggregation_variables)s is experiencing {{ printf "%%.2f" $value }}%% errors for {{ $labels.operation }} operation.
            ||| % $._config,
          },
        },
        {
          alert: $.alertName('IngesterRestarts'),
          expr: |||
            changes(process_start_time_seconds{%s=~".+(cortex|ingester.*)"}[30m]) >= 2
          ||| % $._config.per_job_label,
          labels: {
            // This alert is on a cause not symptom. A couple of ingesters restarts may be suspicious but
            // not necessarily an issue (eg. may happen because of the K8S node autoscaler), so we're
            // keeping the alert as warning as a signal in case of an outage.
            severity: 'warning',
          },
          annotations: {
            message: '{{ $labels.%(per_job_label)s }}/%(alert_instance_variable)s has restarted {{ printf "%%.2f" $value }} times in the last 30 mins.' % $._config,
          },
        },
        {
          alert: $.alertName('KVStoreFailure'),
          expr: |||
            (
              sum by(%(alert_aggregation_labels)s, %(per_instance_label)s, status_code, kv_name) (rate(cortex_kv_request_duration_seconds_count{status_code!~"2.+"}[1m]))
              /
              sum by(%(alert_aggregation_labels)s, %(per_instance_label)s, status_code, kv_name) (rate(cortex_kv_request_duration_seconds_count[1m]))
            )
            # We want to get alerted only in case there's a constant failure.
            == 1
          ||| % $._config,
          'for': '5m',
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: |||
              %(product)s %(alert_instance_variable)s in  %(alert_aggregation_variables)s is failing to talk to the KV store {{ $labels.kv_name }}.
            ||| % $._config,
          },
        },
        {
          alert: $.alertName('MemoryMapAreasTooHigh'),
          expr: |||
            process_memory_map_areas{%(per_job_label)s=~".+(cortex|ingester.*|store-gateway.*)"} / process_memory_map_areas_limit{%(per_job_label)s=~".+(cortex|ingester.*|store-gateway.*)"} > 0.8
          ||| % $._config,
          'for': '5m',
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: '{{ $labels.%(per_job_label)s }}/%(alert_instance_variable)s has a number of mmap-ed areas close to the limit.' % $._config,
          },
        },
        {
          alert: $.alertName('DistributorForwardingErrorRate'),
          expr: |||
            sum by (%(alert_aggregation_labels)s) (rate(cortex_distributor_forward_errors_total{}[1m]))
            /
            sum by (%(alert_aggregation_labels)s) (rate(cortex_distributor_forward_requests_total{}[1m]))
            > 0.01
          ||| % {
            alert_aggregation_labels: $._config.alert_aggregation_labels,
          },
          'for': '5m',
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: |||
              %(product)s in %(alert_aggregation_variables)s has a high failure rate when forwarding samples.
            ||| % $._config,
          },
        },
      ] + [
        {
          alert: $.alertName('RingMembersMismatch'),
          expr: |||
            (
              avg by(%(alert_aggregation_labels)s) (sum by(%(alert_aggregation_labels)s, %(per_instance_label)s) (cortex_ring_members{name="%(component)s",job=~"(.*/)?%(job)s"}))
              != sum by(%(alert_aggregation_labels)s) (up{job=~"(.*/)?%(job)s"})
            )
            and
            (
              count by(%(alert_aggregation_labels)s) (cortex_build_info) > 0
            )
          ||| % {
            alert_aggregation_labels: $._config.alert_aggregation_labels,
            per_instance_label: $._config.per_instance_label,
            component: component_job[0],
            job: component_job[1],
          },
          'for': '15m',
          labels: {
            component: component_job[0],
            severity: 'warning',
          },
          annotations: {
            message: |||
              Number of members in %(product)s %(component)s hash ring does not match the expected number in %(alert_aggregation_variables)s.
            ||| % { component: component_job[0], alert_aggregation_variables: $._config.alert_aggregation_variables, product: $._config.product },
          },
        }
        // NOTE(jhesketh): It is expected that the stateless components may trigger this alert
        //                 too often. Just alert on ingester for now.
        for component_job in [
          // ['compactor', $._config.job_names.compactor],
          // ['distributor', $._config.job_names.distributor],
          ['ingester', $._config.job_names.ingester],
          // ['ruler', $._config.job_names.ruler],
          // ['store-gateway', $._config.job_names.store_gateway],
        ]
      ],
    },
    {
      name: 'mimir_instance_limits_alerts',
      rules: [
        {
          alert: $.alertName('IngesterReachingSeriesLimit'),
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
              Ingester {{ $labels.%(per_job_label)s }}/%(alert_instance_variable)s has reached {{ $value | humanizePercentage }} of its series limit.
            ||| % $._config,
          },
        },
        {
          alert: $.alertName('IngesterReachingSeriesLimit'),
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
              Ingester {{ $labels.%(per_job_label)s }}/%(alert_instance_variable)s has reached {{ $value | humanizePercentage }} of its series limit.
            ||| % $._config,
          },
        },
        {
          alert: $.alertName('IngesterReachingTenantsLimit'),
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
              Ingester {{ $labels.%(per_job_label)s }}/%(alert_instance_variable)s has reached {{ $value | humanizePercentage }} of its tenant limit.
            ||| % $._config,
          },
        },
        {
          alert: $.alertName('IngesterReachingTenantsLimit'),
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
              Ingester {{ $labels.%(per_job_label)s }}/%(alert_instance_variable)s has reached {{ $value | humanizePercentage }} of its tenant limit.
            ||| % $._config,
          },
        },
        {
          alert: $.alertName('ReachingTCPConnectionsLimit'),
          expr: |||
            cortex_tcp_connections / cortex_tcp_connections_limit > 0.8 and
            cortex_tcp_connections_limit > 0
          |||,
          'for': '5m',
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: |||
              %(product)s instance {{ $labels.%(per_job_label)s }}/%(alert_instance_variable)s has reached {{ $value | humanizePercentage }} of its TCP connections limit for {{ $labels.protocol }} protocol.
            ||| % $._config,
          },
        },
        {
          alert: $.alertName('DistributorReachingInflightPushRequestLimit'),
          expr: |||
            (
                (cortex_distributor_inflight_push_requests / ignoring(limit) cortex_distributor_instance_limits{limit="max_inflight_push_requests"})
                and ignoring (limit)
                (cortex_distributor_instance_limits{limit="max_inflight_push_requests"} > 0)
            ) > 0.8
          |||,
          'for': '5m',
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: |||
              Distributor {{ $labels.%(per_job_label)s }}/%(alert_instance_variable)s has reached {{ $value | humanizePercentage }} of its inflight push request limit.
            ||| % $._config,
          },
        },
      ],
    },
    {
      name: 'mimir-rollout-alerts',
      rules: [
        {
          alert: $.alertName('RolloutStuck'),
          expr: |||
            (
              max without (revision) (
                %(kube_statefulset_status_current_revision)s
                  unless
                %(kube_statefulset_status_update_revision)s
              )
                *
              (
                %(kube_statefulset_replicas)s
                  !=
                %(kube_statefulset_status_replicas_updated)s
              )
            ) and (
              changes(%(kube_statefulset_status_replicas_updated)s[15m:1m])
                ==
              0
            )
            * on(%(aggregation_labels)s) group_left max by(%(aggregation_labels)s) (cortex_build_info)
          ||| % {
            aggregation_labels: $._config.alert_aggregation_labels,
            kube_statefulset_status_current_revision: groupStatefulSetByRolloutGroup('kube_statefulset_status_current_revision'),
            kube_statefulset_status_update_revision: groupStatefulSetByRolloutGroup('kube_statefulset_status_update_revision'),
            kube_statefulset_replicas: groupStatefulSetByRolloutGroup('kube_statefulset_replicas'),
            kube_statefulset_status_replicas_updated: groupStatefulSetByRolloutGroup('kube_statefulset_status_replicas_updated'),
          },
          'for': '30m',
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: |||
              The {{ $labels.rollout_group }} rollout is stuck in %(alert_aggregation_variables)s.
            ||| % $._config,
          },
        },
        {
          alert: $.alertName('RolloutStuck'),
          expr: |||
            (
              %(kube_deployment_spec_replicas)s
                !=
              %(kube_deployment_status_replicas_updated)s
            ) and (
              changes(%(kube_deployment_status_replicas_updated)s[15m:1m])
                ==
              0
            )
            * on(%(aggregation_labels)s) group_left max by(%(aggregation_labels)s) (cortex_build_info)
          ||| % {
            aggregation_labels: $._config.alert_aggregation_labels,
            kube_deployment_spec_replicas: groupDeploymentByRolloutGroup('kube_deployment_spec_replicas'),
            kube_deployment_status_replicas_updated: groupDeploymentByRolloutGroup('kube_deployment_status_replicas_updated'),
          },
          'for': '30m',
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: |||
              The {{ $labels.rollout_group }} rollout is stuck in %(alert_aggregation_variables)s.
            ||| % $._config,
          },
        },
        {
          alert: 'RolloutOperatorNotReconciling',
          expr: |||
            max by(%s, rollout_group) (time() - rollout_operator_last_successful_group_reconcile_timestamp_seconds) > 600
          ||| % $._config.alert_aggregation_labels,
          'for': '5m',
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: |||
              Rollout operator is not reconciling the rollout group {{ $labels.rollout_group }} in %(alert_aggregation_variables)s.
            ||| % $._config,
          },
        },
      ],
    },
    {
      name: 'mimir-provisioning',
      rules: [
        {
          alert: $.alertName('ProvisioningTooManyActiveSeries'),
          // We target each ingester to 1.5M in-memory series. This alert fires if the average
          // number of series / ingester in a Mimir cluster is > 1.6M for 2h (we compact
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
          alert: $.alertName('ProvisioningTooManyWrites'),
          // 80k writes / s per ingester max.
          expr: |||
            avg by (%(alert_aggregation_labels)s) (%(alert_aggregation_rule_prefix)s_%(per_instance_label)s:cortex_ingester_ingested_samples_total:rate1m) > 80e3
          ||| % $._config,
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
          alert: $.alertName('AllocatingTooMuchMemory'),
          expr: |||
            (
              # We use RSS instead of working set memory because of the ingester's extensive usage of mmap.
              # See: https://github.com/grafana/mimir/issues/2466
              container_memory_rss{container="ingester"}
                /
              ( container_spec_memory_limit_bytes{container="ingester"} > 0 )
            ) > 0.65
          |||,
          'for': '15m',
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: |||
              Ingester %(alert_instance_variable)s in %(alert_aggregation_variables)s is using too much memory.
            ||| % $._config,
          },
        },
        {
          alert: $.alertName('AllocatingTooMuchMemory'),
          expr: |||
            (
              # We use RSS instead of working set memory because of the ingester's extensive usage of mmap.
              # See: https://github.com/grafana/mimir/issues/2466
              container_memory_rss{container="ingester"}
                /
              ( container_spec_memory_limit_bytes{container="ingester"} > 0 )
            ) > 0.8
          |||,
          'for': '15m',
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: |||
              Ingester %(alert_instance_variable)s in %(alert_aggregation_variables)s is using too much memory.
            ||| % $._config,
          },
        },
      ],
    },
    {
      name: 'ruler_alerts',
      rules: [
        {
          alert: $.alertName('RulerTooManyFailedPushes'),
          expr: |||
            100 * (
            sum by (%(alert_aggregation_labels)s, %(per_instance_label)s) (rate(cortex_ruler_write_requests_failed_total[1m]))
              /
            sum by (%(alert_aggregation_labels)s, %(per_instance_label)s) (rate(cortex_ruler_write_requests_total[1m]))
            ) > 1
          ||| % $._config,
          'for': '5m',
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: |||
              %(product)s Ruler %(alert_instance_variable)s in %(alert_aggregation_variables)s is experiencing {{ printf "%%.2f" $value }}%% write (push) errors.
            ||| % $._config,
          },
        },
        {
          alert: $.alertName('RulerTooManyFailedQueries'),
          expr: |||
            100 * (
            sum by (%(alert_aggregation_labels)s, %(per_instance_label)s) (rate(cortex_ruler_queries_failed_total[1m]))
              /
            sum by (%(alert_aggregation_labels)s, %(per_instance_label)s) (rate(cortex_ruler_queries_total[1m]))
            ) > 1
          ||| % $._config,
          'for': '5m',
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: |||
              %(product)s Ruler %(alert_instance_variable)s in %(alert_aggregation_variables)s is experiencing {{ printf "%%.2f" $value }}%% errors while evaluating rules.
            ||| % $._config,
          },
        },
        {
          alert: $.alertName('RulerMissedEvaluations'),
          expr: |||
            100 * (
            sum by (%(alert_aggregation_labels)s, %(per_instance_label)s, rule_group) (rate(cortex_prometheus_rule_group_iterations_missed_total[1m]))
              /
            sum by (%(alert_aggregation_labels)s, %(per_instance_label)s, rule_group) (rate(cortex_prometheus_rule_group_iterations_total[1m]))
            ) > 1
          ||| % $._config,
          'for': '5m',
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: |||
              %(product)s Ruler %(alert_instance_variable)s in %(alert_aggregation_variables)s is experiencing {{ printf "%%.2f" $value }}%% missed iterations for the rule group {{ $labels.rule_group }}.
            ||| % $._config,
          },
        },
        {
          alert: $.alertName('RulerFailedRingCheck'),
          expr: |||
            sum by (%(alert_aggregation_labels)s, %(per_job_label)s) (rate(cortex_ruler_ring_check_errors_total[1m]))
               > 0
          ||| % $._config,
          'for': '5m',
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: |||
              %(product)s Rulers in %(alert_aggregation_variables)s are experiencing errors when checking the ring for rule group ownership.
            ||| % $._config,
          },
        },
        {
          alert: $.alertName('RulerRemoteEvaluationFailing'),
          expr: |||
            100 * (
            sum by (%s) (rate(cortex_request_duration_seconds_count{route="/httpgrpc.HTTP/Handle", status_code=~"5..", %s}[5m]))
              /
            sum by (%s) (rate(cortex_request_duration_seconds_count{route="/httpgrpc.HTTP/Handle", %s}[5m]))
            ) > 1
          ||| % [$._config.alert_aggregation_labels, $.jobMatcher($._config.job_names.ruler_query_frontend), $._config.alert_aggregation_labels, $.jobMatcher($._config.job_names.ruler_query_frontend)],
          'for': '5m',
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: |||
              %(product)s rulers in %(alert_aggregation_variables)s are failing to perform {{ printf "%%.2f" $value }}%% of remote evaluations through the ruler-query-frontend.
            ||| % $._config,
          },
        },
      ],
    },
    {
      name: 'gossip_alerts',
      rules: [
        {
          alert: $.alertName('GossipMembersMismatch'),
          expr:
            |||
              avg by (%s) (memberlist_client_cluster_members_count) != sum by (%s) (up{%s=~".+/%s"})
            ||| % [$._config.alert_aggregation_labels, $._config.alert_aggregation_labels, $._config.per_job_label, simpleRegexpOpt($._config.job_names.ring_members)],
          'for': '15m',
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: '%(product)s instance %(alert_instance_variable)s in %(alert_aggregation_variables)s sees incorrect number of gossip members.' % $._config,
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
              ( container_spec_memory_limit_bytes{container="etcd"} > 0 )
            ) > 0.65
          |||,
          'for': '15m',
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: |||
              Too much memory being used by {{ $labels.namespace }}/%(alert_instance_variable)s - bump memory limit.
            ||| % $._config,
          },
        },
        {
          alert: 'EtcdAllocatingTooMuchMemory',
          expr: |||
            (
              container_memory_working_set_bytes{container="etcd"}
                /
              ( container_spec_memory_limit_bytes{container="etcd"} > 0 )
            ) > 0.8
          |||,
          'for': '15m',
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: |||
              Too much memory being used by {{ $labels.namespace }}/%(alert_instance_variable)s - bump memory limit.
            ||| % $._config,
          },
        },
      ],
    },
  ],

  groups+: $.withRunbookURL('https://grafana.com/docs/mimir/latest/operators-guide/mimir-runbooks/#%s', alertGroups),
}
