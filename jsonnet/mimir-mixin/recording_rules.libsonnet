local utils = import 'mixin-utils/utils.libsonnet';
local windows = [
  { period: '5m' },
  { period: '30m' },
  { period: '1h' },
  { period: '2h' },
  { period: '6h' },
  { period: '1d' },
  { period: '3d' },
];

{
  prometheusRules+:: {
    groups+: [{
      name: 'cortex_rules',
      rules:
        utils.histogramRules('cortex_request_duration_seconds', ['cluster', 'job']) +
        utils.histogramRules('cortex_request_duration_seconds', ['cluster', 'job', 'route']) +
        utils.histogramRules('cortex_request_duration_seconds', ['cluster', 'namespace', 'job', 'route']) +
        utils.histogramRules('cortex_memcache_request_duration_seconds', ['cluster', 'job', 'method']) +
        utils.histogramRules('cortex_cache_request_duration_seconds', ['cluster', 'job']) +
        utils.histogramRules('cortex_cache_request_duration_seconds', ['cluster', 'job', 'method']) +
        utils.histogramRules('cortex_bigtable_request_duration_seconds', ['cluster', 'job', 'operation']) +
        utils.histogramRules('cortex_cassandra_request_duration_seconds', ['cluster', 'job', 'operation']) +
        utils.histogramRules('cortex_dynamo_request_duration_seconds', ['cluster', 'job', 'operation']) +
        utils.histogramRules('cortex_query_frontend_retries', ['cluster', 'job']) +
        utils.histogramRules('cortex_query_frontend_queue_duration_seconds', ['cluster', 'job']) +
        utils.histogramRules('cortex_ingester_queried_series', ['cluster', 'job']) +
        utils.histogramRules('cortex_ingester_queried_chunks', ['cluster', 'job']) +
        utils.histogramRules('cortex_ingester_queried_samples', ['cluster', 'job']) +
        utils.histogramRules('cortex_chunk_store_index_lookups_per_query', ['cluster', 'job']) +
        utils.histogramRules('cortex_chunk_store_series_pre_intersection_per_query', ['cluster', 'job']) +
        utils.histogramRules('cortex_chunk_store_series_post_intersection_per_query', ['cluster', 'job']) +
        utils.histogramRules('cortex_chunk_store_chunks_per_query', ['cluster', 'job']) +
        utils.histogramRules('cortex_database_request_duration_seconds', ['cluster', 'job', 'method']) +
        utils.histogramRules('cortex_gcs_request_duration_seconds', ['cluster', 'job', 'operation']) +
        utils.histogramRules('cortex_kv_request_duration_seconds', ['cluster', 'job']),
    }, {
      name: 'cortex_slo_rules',
      rules: [
        {
          record: 'namespace_job:cortex_gateway_write_slo_errors_per_request:ratio_rate%(period)s' % window,
          expr: |||
            1 -
            (
              sum by (namespace, job) (rate(cortex_request_duration_seconds_bucket{status_code!~"5..", le="1", route="api_prom_push", job=~".*/cortex-gw"}[%(period)s]))
            /
              sum by (namespace, job) (rate(cortex_request_duration_seconds_count{route="api_prom_push", job=~".*/cortex-gw"}[%(period)s]))
            )
          ||| % window,
        }
        for window in windows
      ] + [
        {
          record: 'namespace_job:cortex_gateway_read_slo_errors_per_request:ratio_rate%(period)s' % window,
          expr: |||
            1 -
            (
              sum by (namespace, job) (rate(cortex_request_duration_seconds_bucket{status_code!~"5..",le="2.5",route=~"api_prom_api_v1_query.*", job=~".*/cortex-gw"}[%(period)s]))
            /
              sum by (namespace, job) (rate(cortex_request_duration_seconds_count{route=~"api_prom_api_v1_query.*", job=~".*/cortex-gw"}[%(period)s]))
            )
          ||| % window,
        }
        for window in windows
      ],
    }, {
      name: 'cortex_received_samples',
      rules: [
        {
          record: 'cluster_namespace:cortex_distributor_received_samples:rate5m',
          expr: |||
            sum by (cluster, namespace) (rate(cortex_distributor_received_samples_total{job=~".*/distributor"}[5m]))
          |||,
        },
      ],
    }],
  },
}
