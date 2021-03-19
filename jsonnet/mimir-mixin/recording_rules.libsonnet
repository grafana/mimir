local utils = import 'mixin-utils/utils.libsonnet';

{
  prometheusRules+:: {
    groups+: [
      {
        name: 'cortex_api',
        rules:
          utils.histogramRules('cortex_request_duration_seconds', ['cluster', 'job']) +
          utils.histogramRules('cortex_request_duration_seconds', ['cluster', 'job', 'route']) +
          utils.histogramRules('cortex_request_duration_seconds', ['cluster', 'namespace', 'job', 'route']),
      },
      {
        name: 'cortex_querier_api',
        rules:
          utils.histogramRules('cortex_querier_request_duration_seconds', ['cluster', 'job']) +
          utils.histogramRules('cortex_querier_request_duration_seconds', ['cluster', 'job', 'route']) +
          utils.histogramRules('cortex_querier_request_duration_seconds', ['cluster', 'namespace', 'job', 'route']),
      },
      {
        name: 'cortex_cache',
        rules:
          utils.histogramRules('cortex_memcache_request_duration_seconds', ['cluster', 'job', 'method']) +
          utils.histogramRules('cortex_cache_request_duration_seconds', ['cluster', 'job']) +
          utils.histogramRules('cortex_cache_request_duration_seconds', ['cluster', 'job', 'method']),
      },
      {
        name: 'cortex_storage',
        rules:
          utils.histogramRules('cortex_bigtable_request_duration_seconds', ['cluster', 'job', 'operation']) +
          utils.histogramRules('cortex_cassandra_request_duration_seconds', ['cluster', 'job', 'operation']) +
          utils.histogramRules('cortex_dynamo_request_duration_seconds', ['cluster', 'job', 'operation']) +
          utils.histogramRules('cortex_chunk_store_index_lookups_per_query', ['cluster', 'job']) +
          utils.histogramRules('cortex_chunk_store_series_pre_intersection_per_query', ['cluster', 'job']) +
          utils.histogramRules('cortex_chunk_store_series_post_intersection_per_query', ['cluster', 'job']) +
          utils.histogramRules('cortex_chunk_store_chunks_per_query', ['cluster', 'job']) +
          utils.histogramRules('cortex_database_request_duration_seconds', ['cluster', 'job', 'method']) +
          utils.histogramRules('cortex_gcs_request_duration_seconds', ['cluster', 'job', 'operation']) +
          utils.histogramRules('cortex_kv_request_duration_seconds', ['cluster', 'job']),
      },
      {
        name: 'cortex_queries',
        rules:
          utils.histogramRules('cortex_query_frontend_retries', ['cluster', 'job']) +
          utils.histogramRules('cortex_query_frontend_queue_duration_seconds', ['cluster', 'job']) +
          utils.histogramRules('cortex_ingester_queried_series', ['cluster', 'job']) +
          utils.histogramRules('cortex_ingester_queried_chunks', ['cluster', 'job']) +
          utils.histogramRules('cortex_ingester_queried_samples', ['cluster', 'job']),
      },
      {
        name: 'cortex_received_samples',
        rules: [
          {
            record: 'cluster_namespace_job:cortex_distributor_received_samples:rate5m',
            expr: |||
              sum by (cluster, namespace, job) (rate(cortex_distributor_received_samples_total[5m]))
            |||,
          },
        ],
      },
      {
        local _config = {
          max_series_per_ingester: 1.5e6,
          max_samples_per_sec_per_ingester: 80e3,
          max_samples_per_sec_per_distributor: 240e3,
          limit_utilisation_target: 0.6,
        },
        name: 'cortex_scaling_rules',
        rules: [
          {
            // Convenience rule to get the number of replicas for both a deployment and a statefulset.
            record: 'cluster_namespace_deployment:actual_replicas:count',
            expr: |||
              sum by (cluster, namespace, deployment) (kube_deployment_spec_replicas)
                or
              sum by (cluster, namespace, deployment) (
                label_replace(kube_statefulset_replicas, "deployment", "$1", "statefulset", "(.*)")
              )
            |||,
          },
          {
            // Distributors should be able to deal with 240k samples/s.
            record: 'cluster_namespace_deployment_reason:required_replicas:count',
            labels: {
              deployment: 'distributor',
              reason: 'sample_rate',
            },
            expr: |||
              ceil(
                quantile_over_time(0.99,
                  sum by (cluster, namespace) (
                    cluster_namespace_job:cortex_distributor_received_samples:rate5m
                  )[24h:]
                )
                / %(max_samples_per_sec_per_distributor)s
              )
            ||| % _config,
          },
          {
            // We should be about to cover 80% of our limits,
            // and ingester can have 80k samples/s.
            record: 'cluster_namespace_deployment_reason:required_replicas:count',
            labels: {
              deployment: 'distributor',
              reason: 'sample_rate_limits',
            },
            expr: |||
              ceil(
                sum by (cluster, namespace) (cortex_overrides{limit_name="ingestion_rate"})
                * %(limit_utilisation_target)s / %(max_samples_per_sec_per_distributor)s
              )
            ||| % _config,
          },
          {
            // We want ingesters each ingester to deal with 80k samples/s.
            // NB we measure this at the distributors and multiple by RF (3).
            record: 'cluster_namespace_deployment_reason:required_replicas:count',
            labels: {
              deployment: 'ingester',
              reason: 'sample_rate',
            },
            expr: |||
              ceil(
                quantile_over_time(0.99,
                  sum by (cluster, namespace) (
                    cluster_namespace_job:cortex_distributor_received_samples:rate5m
                  )[24h:]
                )
                * 3 / %(max_samples_per_sec_per_ingester)s
              )
            ||| % _config,
          },
          {
            // Ingester should have 1.5M series in memory
            record: 'cluster_namespace_deployment_reason:required_replicas:count',
            labels: {
              deployment: 'ingester',
              reason: 'active_series',
            },
            expr: |||
              ceil(
                quantile_over_time(0.99,
                  sum by(cluster, namespace) (
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
            record: 'cluster_namespace_deployment_reason:required_replicas:count',
            labels: {
              deployment: 'ingester',
              reason: 'active_series_limits',
            },
            expr: |||
              ceil(
                sum by (cluster, namespace) (cortex_overrides{limit_name="max_global_series_per_user"})
                * 3 * %(limit_utilisation_target)s / %(max_series_per_ingester)s
              )
            ||| % _config,
          },
          {
            // We should be about to cover 60% of our limits,
            // and ingester can have 80k samples/s.
            record: 'cluster_namespace_deployment_reason:required_replicas:count',
            labels: {
              deployment: 'ingester',
              reason: 'sample_rate_limits',
            },
            expr: |||
              ceil(
                sum by (cluster, namespace) (cortex_overrides{limit_name="ingestion_rate"})
                * %(limit_utilisation_target)s / %(max_samples_per_sec_per_ingester)s
              )
            ||| % _config,
          },
          {
            // Ingesters store 96h of data on disk - we want memcached to store 1/4 of that.
            record: 'cluster_namespace_deployment_reason:required_replicas:count',
            labels: {
              deployment: 'memcached',
              reason: 'active_series',
            },
            expr: |||
              ceil(
                (sum by (cluster, namespace) (
                  cortex_ingester_tsdb_storage_blocks_bytes{job=~".+/ingester"}
                ) / 4)
                  /
                avg by (cluster, namespace) (
                  memcached_limit_bytes{job=~".+/memcached"}
                )
              )
            |||,
          },
          {
            // Jobs should be sized to their CPU usage.
            // We do this by comparing 99th percentile usage over the last 24hrs to
            // their current provisioned #replicas and resource requests.
            record: 'cluster_namespace_deployment_reason:required_replicas:count',
            labels: {
              reason: 'cpu_usage',
            },
            expr: |||
              ceil(
                cluster_namespace_deployment:actual_replicas:count
                  *
                quantile_over_time(0.99,
                  sum by (cluster, namespace, deployment) (
                    label_replace(
                      node_namespace_pod_container:container_cpu_usage_seconds_total:sum_rate,
                      "deployment", "$1", "pod", "(.*)-(?:([0-9]+)|([a-z0-9]+)-([a-z0-9]+))"
                    )
                  )[24h:5m]
                )
                  /
                sum by (cluster, namespace, deployment) (
                  label_replace(
                    kube_pod_container_resource_requests_cpu_cores,
                    "deployment", "$1", "pod", "(.*)-(?:([0-9]+)|([a-z0-9]+)-([a-z0-9]+))"
                  )
                )
              )
            |||,
          },
          {
            // Jobs should be sized to their Memory usage.
            // We do this by comparing 99th percentile usage over the last 24hrs to
            // their current provisioned #replicas and resource requests.
            record: 'cluster_namespace_deployment_reason:required_replicas:count',
            labels: {
              reason: 'memory_usage',
            },
            expr: |||
              ceil(
                cluster_namespace_deployment:actual_replicas:count
                  *
                quantile_over_time(0.99,
                  sum by (cluster, namespace, deployment) (
                    label_replace(
                      container_memory_usage_bytes,
                      "deployment", "$1", "pod", "(.*)-(?:([0-9]+)|([a-z0-9]+)-([a-z0-9]+))"
                    )
                  )[24h:5m]
                )
                  /
                sum by (cluster, namespace, deployment) (
                  label_replace(
                    kube_pod_container_resource_requests_memory_bytes,
                    "deployment", "$1", "pod", "(.*)-(?:([0-9]+)|([a-z0-9]+)-([a-z0-9]+))"
                  )
                )
              )
            |||,
          },
        ],
      },
    ],
  },
}
