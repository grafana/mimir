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

  local request_metric = 'cortex_request_duration_seconds',

  local rate(error_query, total_query, comment='') = |||
    %(comment)s(
      %(errorQuery)s
      /
      %(totalQuery)s
    ) * 100 > 1
  ||| % { comment: comment, errorQuery: error_query, totalQuery: total_query },

  local requestErrorsQuery(selector, error_selector, rate_interval, sum_by, comment='') =
    local errorSelector = '%s, %s' % [error_selector, selector];
    local errorQuery = utils.ncHistogramSumBy(utils.ncHistogramCountRate(request_metric, errorSelector, rate_interval), sum_by);
    local totalQuery = utils.ncHistogramSumBy(utils.ncHistogramCountRate(request_metric, selector, rate_interval), sum_by);
    {
      classic: rate(errorQuery.classic, totalQuery.classic, comment),
      native: rate(errorQuery.native, totalQuery.native, comment),
    },

  local requestErrorsAlert(histogram) =
    local query = requestErrorsQuery(
      selector='route!~"%s"' % std.join('|', ['ready'] + $._config.alert_excluded_routes),
      // Note if alert_aggregation_labels is "job", this will repeat the label. But
      // prometheus seems to tolerate that.
      error_selector='status_code=~"5..", status_code!~"529|598"',
      rate_interval=$.alertRangeInterval(1),
      sum_by=[$._config.alert_aggregation_labels, $._config.per_job_label, 'route'],
      comment=|||
        # The following 5xx errors considered as non-error:
        # - 529: used by distributor rate limiting (using 529 instead of 429 to let the client retry)
        # - 598: used by GEM gateway when the client is very slow to send the request and the gateway times out reading the request body
      |||,
    );
    if histogram != 'classic' && histogram != 'native'
    then {}
    else {
      alert: $.alertName('RequestErrors'),
      expr: if histogram == 'classic' then query.classic else query.native,
      'for': '15m',
      labels: {
        severity: 'critical',
        histogram: histogram,
      },
      annotations: {
        message: |||
          The route {{ $labels.route }} in %(alert_aggregation_variables)s is experiencing {{ printf "%%.2f" $value }}%% errors.
        ||| % $._config,
      },
    },

  local rulerRemoteEvaluationFailingAlert(histogram) =
    local query = requestErrorsQuery(
      selector='route="/httpgrpc.HTTP/Handle", %s' % $.jobMatcher($._config.job_names.ruler_query_frontend),
      error_selector='status_code=~"5.."',
      rate_interval=$.alertRangeInterval(5),
      sum_by=[$._config.alert_aggregation_labels],
    );
    if histogram != 'classic' && histogram != 'native'
    then {}
    else {
      alert: $.alertName('RulerRemoteEvaluationFailing'),
      expr: if histogram == 'classic' then query.classic else query.native,
      'for': '5m',
      labels: {
        severity: 'warning',
        histogram: histogram,
      },
      annotations: {
        message: |||
          %(product)s rulers in %(alert_aggregation_variables)s are failing to perform {{ printf "%%.2f" $value }}%% of remote evaluations through the ruler-query-frontend.
        ||| % $._config,
      },
    },

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
        requestErrorsAlert('classic'),
        requestErrorsAlert('native'),
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
            sum by (%(group_by)s, %(job_label)s) (min_over_time(cortex_query_frontend_queue_length[%(range_interval)s])) > 0
          ||| % {
            group_by: $._config.alert_aggregation_labels,
            job_label: $._config.per_job_label,
            range_interval: $.alertRangeInterval(1),
          },
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
            sum by (%(group_by)s, %(job_label)s) (min_over_time(cortex_query_scheduler_queue_length[%(range_interval)s])) > 0
          ||| % {
            group_by: $._config.alert_aggregation_labels,
            job_label: $._config.per_job_label,
            range_interval: $.alertRangeInterval(1),
          },
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
          alert: $.alertName('CacheRequestErrors'),
          // Specifically exclude "add" operations which are used for cache invalidation and "locking" since
          // they are expected to sometimes fail in normal operation (such as when a "lock" already exists).
          expr: |||
            (
              sum by(%(group_by)s, name, operation) (
                rate(thanos_cache_operation_failures_total{operation!="add"}[%(range_interval)s])
              )
              /
              sum by(%(group_by)s, name, operation) (
                rate(thanos_cache_operations_total{operation!="add"}[%(range_interval)s])
              )
            ) * 100 > 5
          ||| % {
            group_by: $._config.alert_aggregation_labels,
            range_interval: $.alertRangeInterval(1),
          },
          'for': '5m',
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: |||
              The cache {{ $labels.name }} used by %(product)s %(alert_aggregation_variables)s is experiencing {{ printf "%%.2f" $value }}%% errors for {{ $labels.operation }} operation.
            ||| % $._config,
          },
        },
        {
          alert: $.alertName('IngesterRestarts'),
          expr: |||
            (
              sum by(%(alert_aggregation_labels)s, %(per_instance_label)s) (
                increase(kube_pod_container_status_restarts_total{container=~"(%(ingester)s|%(mimir_write)s)"}[30m])
              )
              >= 2
            )
            and
            (
              count by(%(alert_aggregation_labels)s, %(per_instance_label)s) (cortex_build_info) > 0
            )
          ||| % $._config {
            ingester: $._config.container_names.ingester,
            mimir_write: $._config.container_names.mimir_write,
          },
          labels: {
            // This alert is on a cause not symptom. A couple of ingesters restarts may be suspicious but
            // not necessarily an issue (eg. may happen because of the K8S node autoscaler), so we're
            // keeping the alert as warning as a signal in case of an outage.
            severity: 'warning',
          },
          annotations: {
            message: '%(product)s %(alert_instance_variable)s in %(alert_aggregation_variables)s has restarted {{ printf "%%.2f" $value }} times in the last 30 mins.' % $._config,
          },
        },
        {
          alert: $.alertName('KVStoreFailure'),
          expr: |||
            (
              sum by(%(alert_aggregation_labels)s, %(per_instance_label)s, status_code, kv_name) (rate(cortex_kv_request_duration_seconds_count{status_code!~"2.+"}[%(range_interval)s]))
              /
              sum by(%(alert_aggregation_labels)s, %(per_instance_label)s, status_code, kv_name) (rate(cortex_kv_request_duration_seconds_count[%(range_interval)s]))
            )
            # We want to get alerted only in case there's a constant failure.
            == 1
          ||| % $._config {
            range_interval: $.alertRangeInterval(1),
          },
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
            process_memory_map_areas{%(job_regex)s} / process_memory_map_areas_limit{%(job_regex)s} > 0.8
          ||| % { job_regex: $.jobMatcher($._config.job_names.ingester + $._config.job_names.store_gateway) },
          'for': '5m',
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: '{{ $labels.%(per_job_label)s }}/%(alert_instance_variable)s has a number of mmap-ed areas close to the limit.' % $._config,
          },
        },
        {
          // Alert if an ingester instance has no tenants assigned while other instances in the same cell do.
          alert: $.alertName('IngesterInstanceHasNoTenants'),
          'for': '1h',
          expr: |||
            (
              (min by(%(alert_aggregation_labels)s, %(per_instance_label)s) (cortex_ingester_memory_users) == 0)
              unless
              (max by(%(alert_aggregation_labels)s, %(per_instance_label)s) (cortex_lifecycler_read_only) > 0)
            )
            and on (%(alert_aggregation_labels)s)
            # Only if there are more timeseries than would be expected due to continuous testing load
            (
              ( # Classic storage timeseries
                sum by(%(alert_aggregation_labels)s) (cortex_ingester_memory_series)
                /
                max by(%(alert_aggregation_labels)s) (cortex_distributor_replication_factor)
              )
              or
              ( # Ingest storage timeseries
                sum by(%(alert_aggregation_labels)s) (
                  max by(ingester_id, %(alert_aggregation_labels)s) (
                    label_replace(cortex_ingester_memory_series,
                      "ingester_id", "$1",
                      "%(per_instance_label)s", ".*-([0-9]+)$"
                    )
                  )
                )
              )
            ) > 100000
          ||| % $._config,
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: '%(product)s ingester %(alert_instance_variable)s in %(alert_aggregation_variables)s has no tenants assigned.' % $._config,
          },
        },
        {
          // Alert if a ruler instance has no rule groups assigned while other instances in the same cell do.
          alert: $.alertName('RulerInstanceHasNoRuleGroups'),
          'for': '1h',
          expr: |||
            # Alert on ruler instances in microservices mode that have no rule groups assigned,
            min by(%(alert_aggregation_labels)s, %(per_instance_label)s) (cortex_ruler_managers_total{%(per_instance_label)s=~"%(rulerInstanceName)s"}) == 0
            # but only if other ruler instances of the same cell do have rule groups assigned
            and on (%(alert_aggregation_labels)s)
            (max by(%(alert_aggregation_labels)s) (cortex_ruler_managers_total) > 0)
            # and there are more than two instances overall
            and on (%(alert_aggregation_labels)s)
            (count by (%(alert_aggregation_labels)s) (cortex_ruler_managers_total) > 2)
          ||| % {
            alert_aggregation_labels: $._config.alert_aggregation_labels,
            per_instance_label: $._config.per_instance_label,
            rulerInstanceName: $._config.instance_names.ruler,
          },
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: '%(product)s ruler %(alert_instance_variable)s in %(alert_aggregation_variables)s has no rule groups assigned.' % $._config,
          },
        },
        {
          // Alert if a ruler instance has no rule groups assigned while other instances in the same cell do.
          alert: $.alertName('IngestedDataTooFarInTheFuture'),
          'for': '5m',
          expr: |||
            max by(%(alert_aggregation_labels)s, %(per_instance_label)s) (
                cortex_ingester_tsdb_head_max_timestamp_seconds - time()
                and
                cortex_ingester_tsdb_head_max_timestamp_seconds > 0
            ) > 60*60
          ||| % {
            alert_aggregation_labels: $._config.alert_aggregation_labels,
            per_instance_label: $._config.per_instance_label,
          },
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: '%(product)s ingester %(alert_instance_variable)s in %(alert_aggregation_variables)s has ingested samples with timestamps more than 1h in the future.' % $._config,
          },
        },
        {
          alert: $.alertName('StoreGatewayTooManyFailedOperations'),
          'for': '5m',
          expr: |||
            sum by(%(alert_aggregation_labels)s, operation) (rate(thanos_objstore_bucket_operation_failures_total{component="store-gateway"}[%(range_interval)s])) > 0
          ||| % {
            alert_aggregation_labels: $._config.alert_aggregation_labels,
            range_interval: $.alertRangeInterval(1),
          },
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: '%(product)s store-gateway in %(alert_aggregation_variables)s is experiencing {{ $value | humanizePercentage }} errors while doing {{ $labels.operation }} on the object storage.' % $._config,
          },
        },
      ] + [
        {
          alert: $.alertName('RingMembersMismatch'),
          expr: |||
            (
              avg by(%(alert_aggregation_labels)s) (sum by(%(alert_aggregation_labels)s, %(per_instance_label)s) (cortex_ring_members{name="ingester",%(job_regex)s,%(job_not_regex)s}))
              != sum by(%(alert_aggregation_labels)s) (up{%(job_regex)s,%(job_not_regex)s})
            )
            and
            (
              count by(%(alert_aggregation_labels)s) (cortex_build_info) > 0
            )
          ||| % {
            alert_aggregation_labels: $._config.alert_aggregation_labels,
            per_instance_label: $._config.per_instance_label,
            job_regex: $.jobMatcher($._config.job_names.ingester),

            // Exclude temporarily partition ingesters used during the migration to ingest storage.
            // We exclude them because they will build a different ring, still named "ingester" but
            // stored under a different prefix in the KV store.
            job_not_regex: $.jobNotMatcher($._config.job_names.ingester_partition),
          },
          'for': '15m',
          labels: {
            component: 'ingester',
            severity: 'warning',
          },
          annotations: {
            message: |||
              Number of members in %(product)s ingester hash ring does not match the expected number in %(alert_aggregation_variables)s.
            ||| % { alert_aggregation_variables: $._config.alert_aggregation_variables, product: $._config.product },
          },
        },
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
              changes(%(kube_statefulset_status_replicas_updated)s[%(range_interval)s])
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
            range_interval: '15m:' + $.alertRangeInterval(1),
          },
          'for': '30m',
          labels: {
            severity: 'warning',
            workload_type: 'statefulset',
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
              changes(%(kube_deployment_status_replicas_updated)s[%(range_interval)s])
                ==
              0
            )
            * on(%(aggregation_labels)s) group_left max by(%(aggregation_labels)s) (cortex_build_info)
          ||| % {
            aggregation_labels: $._config.alert_aggregation_labels,
            kube_deployment_spec_replicas: groupDeploymentByRolloutGroup('kube_deployment_spec_replicas'),
            kube_deployment_status_replicas_updated: groupDeploymentByRolloutGroup('kube_deployment_status_replicas_updated'),
            range_interval: '15m:' + $.alertRangeInterval(1),
          },
          'for': '30m',
          labels: {
            severity: 'warning',
            workload_type: 'deployment',
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
          alert: $.alertName('AllocatingTooMuchMemory'),
          expr: $._config.ingester_alerts[$._config.deployment_type].memory_allocation % $._config {
            threshold: '0.65',
            ingester: $._config.container_names.ingester,
            mimir_write: $._config.container_names.mimir_write,
            mimir_backend: $._config.container_names.mimir_backend,
          },
          'for': '15m',
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: |||
              Instance %(alert_instance_variable)s in %(alert_aggregation_variables)s is using too much memory.
            ||| % $._config,
          },
        },
        {
          alert: $.alertName('AllocatingTooMuchMemory'),
          expr: $._config.ingester_alerts[$._config.deployment_type].memory_allocation % $._config {
            threshold: '0.8',
            ingester: $._config.container_names.ingester,
            mimir_write: $._config.container_names.mimir_write,
            mimir_backend: $._config.container_names.mimir_backend,
          },
          'for': '15m',
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: |||
              Instance %(alert_instance_variable)s in %(alert_aggregation_variables)s is using too much memory.
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
            sum by (%(alert_aggregation_labels)s, %(per_instance_label)s) (rate(cortex_ruler_write_requests_failed_total[%(range_interval)s]))
              /
            sum by (%(alert_aggregation_labels)s, %(per_instance_label)s) (rate(cortex_ruler_write_requests_total[%(range_interval)s]))
            ) > 1
          ||| % $._config {
            range_interval: $.alertRangeInterval(1),
          },
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
            sum by (%(alert_aggregation_labels)s, %(per_instance_label)s) (rate(cortex_ruler_queries_failed_total[%(range_interval)s]))
              /
            sum by (%(alert_aggregation_labels)s, %(per_instance_label)s) (rate(cortex_ruler_queries_total[%(range_interval)s]))
            ) > 1
          ||| % $._config {
            range_interval: $.alertRangeInterval(1),
          },
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
            sum by (%(alert_aggregation_labels)s, %(per_instance_label)s, rule_group) (rate(cortex_prometheus_rule_group_iterations_missed_total[%(range_interval)s]))
              /
            sum by (%(alert_aggregation_labels)s, %(per_instance_label)s, rule_group) (rate(cortex_prometheus_rule_group_iterations_total[%(range_interval)s]))
            ) > 1
          ||| % $._config {
            range_interval: $.alertRangeInterval(1),
          },
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
            sum by (%(alert_aggregation_labels)s, %(per_job_label)s) (rate(cortex_ruler_ring_check_errors_total[%(range_interval)s]))
               > 0
          ||| % $._config {
            range_interval: $.alertRangeInterval(1),
          },
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
        rulerRemoteEvaluationFailingAlert('classic'),
        rulerRemoteEvaluationFailingAlert('native'),
      ],
    },
    {
      name: 'gossip_alerts',
      rules: [
        {
          // What's the purpose of this alert? We want to know if two databases' Memberlist clusters have merged.
          // We do this by comparing the reported number of cluster members with the expected number of members based on the number of running pods in that namespace.
          // If two Memberlist clusters have merged, then the reported number of members will be higher than the expected number.
          // However, during rollouts, the number of reported cluster members can be higher than the expected number because it takes some time for the removal of old
          // pods to be propagated to all members of the cluster, so we add a fudge factor of 10 extra members.
          // This value is designed to be low enough that the alert will trigger if another cluster merges with this one (assuming that most clusters have more than 10
          // members), but high enough to not result in false positives during rollouts.
          // We don't use a percentage because this would not be reliable: in a large Mimir cluster of 1000+ instances, even a small percentage like 5% would be 50
          // instances - too high to catch a small cluster merging with a big one.
          alert: $.alertName('GossipMembersTooHigh'),
          expr:
            |||
              max by (%s) (memberlist_client_cluster_members_count)
              >
              (sum by (%s) (up{%s}) + 10)
            ||| % [$._config.alert_aggregation_labels, $._config.alert_aggregation_labels, $.jobMatcher($._config.job_names.ring_members)],
          'for': '20m',
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: 'One or more %(product)s instances in %(alert_aggregation_variables)s consistently sees a higher than expected number of gossip members.' % $._config,
          },
        },
        {
          // What's the purpose of this alert? We want to know if a cell has reached a split brain scenario.
          // We do this by comparing the reported number of cluster members with the expected number of members based on the number of running pods in that namespace.
          // If a split has occurred, then the reported number of members will be lower than the expected number.
          alert: $.alertName('GossipMembersTooLow'),
          expr:
            |||
              min by (%s) (memberlist_client_cluster_members_count)
              <
              (sum by (%s) (up{%s=~".+/%s"}) * 0.5)
            ||| % [$._config.alert_aggregation_labels, $._config.alert_aggregation_labels, $._config.per_job_label, simpleRegexpOpt($._config.job_names.ring_members)],
          'for': '20m',
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: 'One or more %(product)s instances in %(alert_aggregation_variables)s consistently sees a lower than expected number of gossip members.' % $._config,
          },
        },
        {
          // Alert if the list of endpoints returned by the gossip-ring service (used as memberlist seed nodes)
          // is out-of-sync. This is a warning alert with 10% out-of-sync threshold.
          alert: $.alertName('GossipMembersEndpointsOutOfSync'),
          expr:
            |||
              (
                count by(%(alert_aggregation_labels)s) (
                  kube_endpoint_address{endpoint="gossip-ring"}
                  unless on (%(alert_aggregation_labels)s, ip)
                  label_replace(kube_pod_info, "ip", "$1", "pod_ip", "(.*)"))
                /
                count by(%(alert_aggregation_labels)s) (
                  kube_endpoint_address{endpoint="gossip-ring"}
                )
                * 100 > 10
              )

              # Filter by Mimir only.
              and (count by(%(alert_aggregation_labels)s) (cortex_build_info) > 0)
            ||| % $._config,
          'for': '15m',
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: '%(product)s gossip-ring service endpoints list in %(alert_aggregation_variables)s is out of sync.' % $._config,
          },
        },
        {
          // Alert if the list of endpoints returned by the gossip-ring service (used as memberlist seed nodes)
          // is out-of-sync. This is a critical alert with 50% out-of-sync threshold.
          alert: $.alertName('GossipMembersEndpointsOutOfSync'),
          expr:
            |||
              (
                count by(%(alert_aggregation_labels)s) (
                  kube_endpoint_address{endpoint="gossip-ring"}
                  unless on (%(alert_aggregation_labels)s, ip)
                  label_replace(kube_pod_info, "ip", "$1", "pod_ip", "(.*)"))
                /
                count by(%(alert_aggregation_labels)s) (
                  kube_endpoint_address{endpoint="gossip-ring"}
                )
                * 100 > 50
              )

              # Filter by Mimir only.
              and (count by(%(alert_aggregation_labels)s) (cortex_build_info) > 0)
            ||| % $._config,
          'for': '5m',
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: '%(product)s gossip-ring service endpoints list in %(alert_aggregation_variables)s is out of sync.' % $._config,
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

  groups+: $.withRunbookURL('https://grafana.com/docs/mimir/latest/operators-guide/mimir-runbooks/#%s', $.withExtraLabelsAnnotations(alertGroups)),
}
