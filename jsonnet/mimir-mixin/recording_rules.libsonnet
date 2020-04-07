local utils = import 'mixin-utils/utils.libsonnet';

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
    }],
  },
}
