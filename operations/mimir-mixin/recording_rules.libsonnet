local utils = import 'mixin-utils/utils.libsonnet';

{
  local _config = {
    max_series_per_ingester: 1.5e6,
    max_samples_per_sec_per_ingester: 80e3,
    max_samples_per_sec_per_distributor: 240e3,
    limit_utilisation_target: 0.6,
  } + $._config + $._group_config,
  prometheusRules+:: {
    groups+: [
      {
        name: 'mimir_api_1',
        rules:
          utils.histogramRules('cortex_request_duration_seconds', [$._config.per_cluster_label, 'job'], $._config.recording_rules_range_interval),
      },
      {
        name: 'mimir_api_2',
        rules:
          utils.histogramRules('cortex_request_duration_seconds', [$._config.per_cluster_label, 'job', 'route'], $._config.recording_rules_range_interval),
      },
      {
        name: 'mimir_api_3',
        rules:
          utils.histogramRules('cortex_request_duration_seconds', $._config.job_labels + ['route'], $._config.recording_rules_range_interval),
      },
      {
        name: 'mimir_querier_api',
        rules:
          utils.histogramRules('cortex_querier_request_duration_seconds', [$._config.per_cluster_label, 'job'], $._config.recording_rules_range_interval) +
          utils.histogramRules('cortex_querier_request_duration_seconds', [$._config.per_cluster_label, 'job', 'route'], $._config.recording_rules_range_interval) +
          utils.histogramRules('cortex_querier_request_duration_seconds', $._config.job_labels + ['route'], $._config.recording_rules_range_interval),
      },
      {
        name: 'mimir_cache',
        rules:
          utils.histogramRules('cortex_memcache_request_duration_seconds', [$._config.per_cluster_label, 'job', 'method'], $._config.recording_rules_range_interval) +
          utils.histogramRules('cortex_cache_request_duration_seconds', [$._config.per_cluster_label, 'job'], $._config.recording_rules_range_interval) +
          utils.histogramRules('cortex_cache_request_duration_seconds', [$._config.per_cluster_label, 'job', 'method'], $._config.recording_rules_range_interval),
      },
      {
        name: 'mimir_storage',
        rules:
          utils.histogramRules('cortex_kv_request_duration_seconds', [$._config.per_cluster_label, 'job'], $._config.recording_rules_range_interval),
      },
      {
        name: 'mimir_queries',
        rules:
          utils.histogramRules('cortex_query_frontend_retries', [$._config.per_cluster_label, 'job'], $._config.recording_rules_range_interval) +
          utils.histogramRules('cortex_query_frontend_queue_duration_seconds', [$._config.per_cluster_label, 'job'], $._config.recording_rules_range_interval),
      },
      {
        name: 'mimir_ingester_queries',
        rules:
          utils.histogramRules('cortex_ingester_queried_series', [$._config.per_cluster_label, 'job'], $._config.recording_rules_range_interval) +
          utils.histogramRules('cortex_ingester_queried_samples', [$._config.per_cluster_label, 'job'], $._config.recording_rules_range_interval) +
          utils.histogramRules('cortex_ingester_queried_exemplars', [$._config.per_cluster_label, 'job'], $._config.recording_rules_range_interval),
      },
      {
        name: 'mimir_received_samples',
        rules: [
          {
            record: '%(group_prefix_jobs)s:cortex_distributor_received_samples:rate5m' % _config,
            expr: |||
              sum by (%(group_by_job)s) (rate(cortex_distributor_received_samples_total[5m]))
            ||| % _config,
          },
        ],
      },
      {
        name: 'mimir_exemplars_in',
        rules: [
          {
            record: '%(group_prefix_jobs)s:cortex_distributor_exemplars_in:rate5m' % _config,
            expr: |||
              sum by (%(group_by_job)s) (rate(cortex_distributor_exemplars_in_total[5m]))
            ||| % _config,
          },
        ],
      },
      {
        name: 'mimir_received_exemplars',
        rules: [
          {
            record: '%(group_prefix_jobs)s:cortex_distributor_received_exemplars:rate5m' % _config,
            expr: |||
              sum by (%(group_by_job)s) (rate(cortex_distributor_received_exemplars_total[5m]))
            ||| % _config,
          },
        ],
      },
      {
        name: 'mimir_exemplars_ingested',
        rules: [
          {
            record: '%(group_prefix_jobs)s:cortex_ingester_ingested_exemplars:rate5m' % _config,
            expr: |||
              sum by (%(group_by_job)s) (rate(cortex_ingester_ingested_exemplars_total[5m]))
            ||| % _config,
          },
        ],
      },
      {
        name: 'mimir_exemplars_appended',
        rules: [
          {
            record: '%(group_prefix_jobs)s:cortex_ingester_tsdb_exemplar_exemplars_appended:rate5m' % _config,
            expr: |||
              sum by (%(group_by_job)s) (rate(cortex_ingester_tsdb_exemplar_exemplars_appended_total[5m]))
            ||| % _config,
          },
        ],
      },
      {
        name: 'mimir_scaling_rules',
        rules: [
          {
            record: '%(alert_aggregation_rule_prefix)s_deployment:actual_replicas:count' % _config,
            expr: _config.mimir_scaling_rules[_config.deployment_type].actual_replicas_count % _config,
          },
          {
            // Distributors should be able to deal with 240k samples/s.
            record: '%(alert_aggregation_rule_prefix)s_deployment_reason:required_replicas:count' % _config,
            labels: {
              deployment: 'distributor',
              reason: 'sample_rate',
            },
            expr: |||
              ceil(
                quantile_over_time(0.99,
                  sum by (%(alert_aggregation_labels)s) (
                    %(group_prefix_jobs)s:cortex_distributor_received_samples:rate5m
                  )[24h:]
                )
                / %(max_samples_per_sec_per_distributor)s
              )
            ||| % _config,
          },
          {
            // We should be about to cover 80% of our limits,
            // and ingester can have 80k samples/s.
            record: '%(alert_aggregation_rule_prefix)s_deployment_reason:required_replicas:count' % _config,
            labels: {
              deployment: 'distributor',
              reason: 'sample_rate_limits',
            },
            expr: |||
              ceil(
                sum by (%(alert_aggregation_labels)s) (cortex_limits_overrides{limit_name="ingestion_rate"})
                * %(limit_utilisation_target)s / %(max_samples_per_sec_per_distributor)s
              )
            ||| % _config,
          },
          {
            // We want ingesters each ingester to deal with 80k samples/s.
            // NB we measure this at the distributors and multiple by RF (3).
            record: '%(alert_aggregation_rule_prefix)s_deployment_reason:required_replicas:count' % _config,
            labels: {
              deployment: 'ingester',
              reason: 'sample_rate',
            },
            expr: |||
              ceil(
                quantile_over_time(0.99,
                  sum by (%(alert_aggregation_labels)s) (
                    %(group_prefix_jobs)s:cortex_distributor_received_samples:rate5m
                  )[24h:]
                )
                * 3 / %(max_samples_per_sec_per_ingester)s
              )
            ||| % _config,
          },
          {
            // Ingester should have 1.5M series in memory
            record: '%(alert_aggregation_rule_prefix)s_deployment_reason:required_replicas:count' % _config,
            labels: {
              deployment: 'ingester',
              reason: 'active_series',
            },
            expr: |||
              ceil(
                quantile_over_time(0.99,
                  sum by(%(alert_aggregation_labels)s) (
                    cortex_ingester_memory_series
                  )[24h:]
                )
                / %(max_series_per_ingester)s
              )
            ||| % _config,
          },
          {
            // We should be about to cover 60% of our limits,
            // and ingester can have 1.5M series in memory
            record: '%(alert_aggregation_rule_prefix)s_deployment_reason:required_replicas:count' % _config,
            labels: {
              deployment: 'ingester',
              reason: 'active_series_limits',
            },
            expr: |||
              ceil(
                sum by (%(alert_aggregation_labels)s) (cortex_limits_overrides{limit_name="max_global_series_per_user"})
                * 3 * %(limit_utilisation_target)s / %(max_series_per_ingester)s
              )
            ||| % _config,
          },
          {
            // We should be about to cover 60% of our limits,
            // and ingester can have 80k samples/s.
            record: '%(alert_aggregation_rule_prefix)s_deployment_reason:required_replicas:count' % _config,
            labels: {
              deployment: 'ingester',
              reason: 'sample_rate_limits',
            },
            expr: |||
              ceil(
                sum by (%(alert_aggregation_labels)s) (cortex_limits_overrides{limit_name="ingestion_rate"})
                * %(limit_utilisation_target)s / %(max_samples_per_sec_per_ingester)s
              )
            ||| % _config,
          },
          {
            // Ingesters store 96h of data on disk - we want memcached to store 1/4 of that.
            record: '%(alert_aggregation_rule_prefix)s_deployment_reason:required_replicas:count' % _config,
            labels: {
              deployment: 'memcached',
              reason: 'active_series',
            },
            expr: |||
              ceil(
                (sum by (%(alert_aggregation_labels)s) (
                  cortex_ingester_tsdb_storage_blocks_bytes{job=~".+/ingester.*"}
                ) / 4)
                  /
                avg by (%(alert_aggregation_labels)s) (
                  memcached_limit_bytes{job=~".+/memcached"}
                )
              )
            ||| % _config,
          },
          {
            record: '%(alert_aggregation_rule_prefix)s_deployment:container_cpu_usage_seconds_total:sum_rate' % _config,
            expr: _config.mimir_scaling_rules[_config.deployment_type].cpu_usage_seconds_total % _config,
          },
          {
            record: '%(alert_aggregation_rule_prefix)s_deployment:kube_pod_container_resource_requests_cpu_cores:sum' % _config,
            expr: _config.mimir_scaling_rules[_config.deployment_type].resource_requests_cpu_cores % _config,
          },
          {
            record: '%(alert_aggregation_rule_prefix)s_deployment_reason:required_replicas:count' % _config,
            labels: {
              reason: 'cpu_usage',
            },
            expr: _config.mimir_scaling_rules[_config.deployment_type].cpu_required_replicas_count % _config,
          },
          {
            record: '%(alert_aggregation_rule_prefix)s_deployment:container_memory_usage_bytes:sum' % _config,
            expr: _config.mimir_scaling_rules[_config.deployment_type].memory_usage % _config,
          },
          {
            record: '%(alert_aggregation_rule_prefix)s_deployment:kube_pod_container_resource_requests_memory_bytes:sum' % _config,
            expr: _config.mimir_scaling_rules[_config.deployment_type].memory_requests % _config,
          },
          {
            record: '%(alert_aggregation_rule_prefix)s_deployment_reason:required_replicas:count' % _config,
            labels: {
              reason: 'memory_usage',
            },
            expr: _config.mimir_scaling_rules[_config.deployment_type].memory_required_replicas_count % _config,
          },
        ],
      },
      {
        name: 'mimir_alertmanager_rules',
        rules: [
          // Aggregations of per-user Alertmanager metrics used in dashboards.
          {
            record: '%s_job_%s:cortex_alertmanager_alerts:sum' % [$._config.per_cluster_label, $._config.per_instance_label],
            expr: |||
              sum by (%s, job, %s) (cortex_alertmanager_alerts)
            ||| % [$._config.per_cluster_label, $._config.per_instance_label],
          },
          {
            record: '%s_job_%s:cortex_alertmanager_silences:sum' % [$._config.per_cluster_label, $._config.per_instance_label],
            expr: |||
              sum by (%s, job, %s) (cortex_alertmanager_silences)
            ||| % [$._config.per_cluster_label, $._config.per_instance_label],
          },
          {
            record: '%s_job:cortex_alertmanager_alerts_received_total:rate5m' % $._config.per_cluster_label,
            expr: |||
              sum by (%(per_cluster_label)s, job) (rate(cortex_alertmanager_alerts_received_total[5m]))
            ||| % _config,
          },
          {
            record: '%s_job:cortex_alertmanager_alerts_invalid_total:rate5m' % $._config.per_cluster_label,
            expr: |||
              sum by (%(per_cluster_label)s, job) (rate(cortex_alertmanager_alerts_invalid_total[5m]))
            ||| % _config,
          },
          {
            record: '%s_job_integration:cortex_alertmanager_notifications_total:rate5m' % $._config.per_cluster_label,
            expr: |||
              sum by (%(per_cluster_label)s, job, integration) (rate(cortex_alertmanager_notifications_total[5m]))
            ||| % _config,
          },
          {
            record: '%s_job_integration:cortex_alertmanager_notifications_failed_total:rate5m' % $._config.per_cluster_label,
            expr: |||
              sum by (%(per_cluster_label)s, job, integration) (rate(cortex_alertmanager_notifications_failed_total[5m]))
            ||| % _config,
          },
          {
            record: '%s_job:cortex_alertmanager_state_replication_total:rate5m' % $._config.per_cluster_label,
            expr: |||
              sum by (%(per_cluster_label)s, job) (rate(cortex_alertmanager_state_replication_total[5m]))
            ||| % _config,
          },
          {
            record: '%s_job:cortex_alertmanager_state_replication_failed_total:rate5m' % $._config.per_cluster_label,
            expr: |||
              sum by (%(per_cluster_label)s, job) (rate(cortex_alertmanager_state_replication_failed_total[5m]))
            ||| % _config,
          },
          {
            record: '%s_job:cortex_alertmanager_partial_state_merges_total:rate5m' % $._config.per_cluster_label,
            expr: |||
              sum by (%(per_cluster_label)s, job) (rate(cortex_alertmanager_partial_state_merges_total[5m]))
            ||| % _config,
          },
          {
            record: '%s_job:cortex_alertmanager_partial_state_merges_failed_total:rate5m' % $._config.per_cluster_label,
            expr: |||
              sum by (%(per_cluster_label)s, job) (rate(cortex_alertmanager_partial_state_merges_failed_total[5m]))
            ||| % _config,
          },
        ],
      },
      {
        name: 'mimir_ingester_rules',
        rules: [
          {
            // cortex_ingester_ingested_samples_total is per user, in this rule we want to see the sum per cluster/namespace/instance
            record: '%s_%s:cortex_ingester_ingested_samples_total:rate1m' % [$._config.alert_aggregation_rule_prefix, $._config.per_instance_label],
            expr: |||
              sum by(%(alert_aggregation_labels)s, %(per_instance_label)s) (rate(cortex_ingester_ingested_samples_total[1m]))
            ||| % $._config,
          },
        ],
      },
    ],
  },
}
