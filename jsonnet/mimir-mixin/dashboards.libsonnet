local utils = (import 'mixin-utils/utils.libsonnet');

local g = (import 'grafana-builder/grafana.libsonnet') + {
  qpsPanel(selector)::
    super.qpsPanel(selector) + {
      targets: [
        target {
          interval: '1m',
        }
        for target in super.targets
      ],
    },

  latencyPanel(metricName, selector, multiplier='1e3')::
    super.latencyPanel(metricName, selector, multiplier) + {
      targets: [
        target {
          interval: '1m',
        }
        for target in super.targets
      ],
    },

  successFailurePanel(title, successMetric, failureMetric)::
    g.panel(title) +
    g.queryPanel([successMetric, failureMetric], ['successful', 'failed']) +
    g.stack + {
      aliasColors: {
        successful: '#7EB26D',
        failed: '#E24D42',
      },
    },

  objectStorePanels1(title, metricPrefix)::
    local opsTotal = '%s_thanos_objstore_bucket_operations_total' % [metricPrefix];
    local opsTotalFailures = '%s_thanos_objstore_bucket_operation_failures_total' % [metricPrefix];
    local operationDuration = '%s_thanos_objstore_bucket_operation_duration_seconds' % [metricPrefix];
    local interval = '$__interval';
    super.row(title)
    .addPanel(
      // We use 'up{cluster=~"$cluster", job="($namespace)/.+"}' to add 0 if there are no failed operations.
      self.successFailurePanel(
        'Operations/sec',
        'sum(rate(%s{cluster=~"$cluster"}[%s])) - sum(rate(%s{cluster=~"$cluster"}[%s]) or (up{cluster=~"$cluster", job="($namespace)/.+"}*0))' % [opsTotal, interval, opsTotalFailures, interval],
        'sum(rate(%s{cluster=~"$cluster"}[%s]) or (up{cluster=~"$cluster", job="($namespace)/.+"}*0))' % [opsTotalFailures, interval]
      )
    )
    .addPanel(
      g.panel('Op: ObjectSize') +
      g.latencyPanel(operationDuration, '{cluster=~"$cluster", operation="objectsize"}'),
    )
    .addPanel(
      g.panel('Op: Iter') +
      g.latencyPanel(operationDuration, '{cluster=~"$cluster", operation="iter"}'),
    )
    .addPanel(
      g.panel('Op: Exists') +
      g.latencyPanel(operationDuration, '{cluster=~"$cluster", operation="exists"}'),
    ),

  // Second row of Object Store stats
  objectStorePanels2(title, metricPrefix)::
    local operationDuration = '%s_thanos_objstore_bucket_operation_duration_seconds' % [metricPrefix];
    super.row(title)
    .addPanel(
      g.panel('Op: Get') +
      g.latencyPanel(operationDuration, '{cluster=~"$cluster", operation="get"}'),
    )
    .addPanel(
      g.panel('Op: GetRange') +
      g.latencyPanel(operationDuration, '{cluster=~"$cluster", operation="get_range"}'),
    )
    .addPanel(
      g.panel('Op: Upload') +
      g.latencyPanel(operationDuration, '{cluster=~"$cluster", operation="upload"}'),
    )
    .addPanel(
      g.panel('Op: Delete') +
      g.latencyPanel(operationDuration, '{cluster=~"$cluster", operation="delete"}'),
    ),
};

{
  _config+:: {
    storage_backend: error 'must specify storage backend (cassandra, gcp)',
    // may contain 'chunks', 'tsdb' or both. Enables chunks- or tsdb- specific panels and dashboards.
    storage_engine: ['chunks'],
    gcs_enabled: false,
  },

  dashboards+: {
    'cortex-writes.json':
      local addGcsRow(dashboard) = if $._config.gcs_enabled then
        dashboard.addRow(
          g.row('GCS')
          .addPanel(
            g.panel('QPS') +
            g.qpsPanel('cortex_gcs_request_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/querier", operation="POST"}')
          )
          .addPanel(
            g.panel('Latency') +
            utils.latencyRecordingRulePanel('cortex_gcs_request_duration_seconds', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/querier'), utils.selector.eq('operation', 'POST')])
          )
        ) else dashboard;

      local addBlocksRows(dashboard) = if std.setMember('tsdb', $._config.storage_engine) then
        // Used by ingester when using TSDB storage engine.
        dashboard.addRow(
          g.row('Blocks Shipper')
          .addPanel(
            g.successFailurePanel(
              'Uploaded blocks / sec',
              'sum(rate(cortex_ingester_shipper_uploads_total{cluster=~"$cluster"}[$__interval])) - sum(rate(cortex_ingester_shipper_upload_failures_total{cluster=~"$cluster"}[$__interval]))',
              'sum(rate(cortex_ingester_shipper_upload_failures_total{cluster=~"$cluster"}[$__interval]))'
            )
          )
        )
        .addRow(g.objectStorePanels1('Blocks Object Store Stats (Ingester)', 'cortex_ingester'))
        .addRow(g.objectStorePanels2('', 'cortex_ingester'))
      else dashboard;

      addBlocksRows(addGcsRow($.cortex_writes_dashboard)),

    'cortex-reads.json':
      local addGcsRows(dashboard) = if $._config.gcs_enabled then
        dashboard.addRow(
          g.row('GCS')
          .addPanel(
            g.panel('QPS') +
            g.qpsPanel('cortex_gcs_request_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/querier", operation="GET"}')
          )
          .addPanel(
            g.panel('Latency') +
            utils.latencyRecordingRulePanel('cortex_gcs_request_duration_seconds', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/querier'), utils.selector.eq('operation', 'GET')])
          )
        )
      else dashboard;

      local addBlocksRows(dashboard) = if std.setMember('tsdb', $._config.storage_engine) then
        dashboard.addRow(
          g.row('Querier - Blocks Storage')
          .addPanel(
            g.successFailurePanel(
              'Block Loads / sec',
              'sum(rate(cortex_querier_bucket_store_block_loads_total{cluster=~"$cluster"}[$__interval])) - sum(rate(cortex_querier_bucket_store_block_load_failures_total{cluster=~"$cluster"}[$__interval]))',
              'sum(rate(cortex_querier_bucket_store_block_load_failures_total{cluster=~"$cluster"}[$__interval]))'
            )
          )
          .addPanel(
            g.successFailurePanel(
              'Block Drops / sec',
              'sum(rate(cortex_querier_bucket_store_block_drops_total{cluster=~"$cluster"}[$__interval])) - sum(rate(cortex_querier_bucket_store_block_drop_failures_total{cluster=~"$cluster"}[$__interval]))',
              'sum(rate(cortex_querier_bucket_store_block_drop_failures_total{cluster=~"$cluster"}[$__interval]))'
            )
          )
          .addPanel(
            g.panel('Per-block prepares and preloads duration') +
            g.latencyPanel('cortex_querier_bucket_store_series_get_all_duration_seconds', '{cluster=~"$cluster"}'),
          )
          .addPanel(
            g.panel('Series merge duration') +
            g.latencyPanel('cortex_querier_bucket_store_series_merge_duration_seconds', '{cluster=~"$cluster"}'),
          )
        )
        .addRow(g.objectStorePanels1('Blocks Object Store Stats (Querier)', 'cortex_querier'))
        .addRow(g.objectStorePanels2('', 'cortex_querier'))
      else dashboard;

      addBlocksRows(addGcsRows($.cortex_reads_dashboard)),

    [if std.setMember('chunks', $._config.storage_engine) then 'cortex-chunks.json' else null]:
      g.dashboard('Cortex / Chunks')
      .addMultiTemplate('cluster', 'kube_pod_container_info{image=~".*cortex.*"}', 'cluster')
      .addMultiTemplate('namespace', 'kube_pod_container_info{image=~".*cortex.*"}', 'namespace')
      .addRow(
        g.row('Active Series / Chunks')
        .addPanel(
          g.panel('Series') +
          g.queryPanel('sum(cortex_ingester_memory_series{cluster=~"$cluster", job=~"($namespace)/ingester"})', 'series'),
        )
        .addPanel(
          g.panel('Chunks per series') +
          g.queryPanel('sum(cortex_ingester_memory_chunks{cluster=~"$cluster", job=~"($namespace)/ingester"}) / sum(cortex_ingester_memory_series{cluster=~"$cluster", job=~"($namespace)/ingester"})', 'chunks'),
        )
      )
      .addRow(
        g.row('Flush Stats')
        .addPanel(
          g.panel('Utilization') +
          g.latencyPanel('cortex_ingester_chunk_utilization', '{cluster=~"$cluster", job=~"($namespace)/ingester"}', multiplier='1') +
          { yaxes: g.yaxes('percentunit') },
        )
        .addPanel(
          g.panel('Age') +
          g.latencyPanel('cortex_ingester_chunk_age_seconds', '{cluster=~"$cluster", job=~"($namespace)/ingester"}'),
        ),
      )
      .addRow(
        g.row('Flush Stats')
        .addPanel(
          g.panel('Size') +
          g.latencyPanel('cortex_ingester_chunk_length', '{cluster=~"$cluster", job=~"($namespace)/ingester"}', multiplier='1') +
          { yaxes: g.yaxes('short') },
        )
        .addPanel(
          g.panel('Entries') +
          g.queryPanel('sum(rate(cortex_chunk_store_index_entries_per_chunk_sum{cluster=~"$cluster", job=~"($namespace)/ingester"}[5m])) / sum(rate(cortex_chunk_store_index_entries_per_chunk_count{cluster=~"$cluster", job=~"($namespace)/ingester"}[5m]))', 'entries'),
        ),
      )
      .addRow(
        g.row('Flush Stats')
        .addPanel(
          g.panel('Queue Length') +
          g.queryPanel('cortex_ingester_flush_queue_length{cluster=~"$cluster", job=~"($namespace)/ingester"}', '{{instance}}'),
        )
        .addPanel(
          g.panel('Flush Rate') +
          g.qpsPanel('cortex_ingester_chunk_age_seconds_count{cluster=~"$cluster", job=~"($namespace)/ingester"}'),
        ),
      ),

    'cortex-queries.json':
      g.dashboard('Cortex / Queries')
      .addMultiTemplate('cluster', 'kube_pod_container_info{image=~".*cortex.*"}', 'cluster')
      .addMultiTemplate('namespace', 'kube_pod_container_info{image=~".*cortex.*"}', 'namespace')
      .addRow(
        g.row('Query Frontend')
        .addPanel(
          g.panel('Queue Duration') +
          g.latencyPanel('cortex_query_frontend_queue_duration_seconds', '{cluster=~"$cluster", job=~"($namespace)/query-frontend"}'),
        )
        .addPanel(
          g.panel('Retries') +
          g.latencyPanel('cortex_query_frontend_retries', '{cluster=~"$cluster", job=~"($namespace)/query-frontend"}', multiplier=1) +
          { yaxes: g.yaxes('short') },
        )
        .addPanel(
          g.panel('Queue Length') +
          g.queryPanel('cortex_query_frontend_queue_length{cluster=~"$cluster", job=~"($namespace)/query-frontend"}', '{{cluster}} / {{namespace}} / {{instance}}'),
        )
      )
      .addRow(
        g.row('Query Frontend - Results Cache')
        .addPanel(
          g.panel('Cache Hit %') +
          g.queryPanel('sum(rate(cortex_cache_hits{cluster=~"$cluster",job=~"($namespace)/query-frontend"}[1m])) / sum(rate(cortex_cache_fetched_keys{cluster=~"$cluster",job=~"($namespace)/query-frontend"}[1m]))', 'Hit Rate') +
          { yaxes: g.yaxes({ format: 'percentunit', max: 1 }) },
        )
        .addPanel(
          g.panel('Cache misses') +
          g.queryPanel('sum(rate(cortex_cache_fetched_keys{cluster=~"$cluster",job=~"($namespace)/query-frontend"}[1m])) - sum(rate(cortex_cache_hits{cluster=~"$cluster",job=~"($namespace)/query-frontend"}[1m]))', 'Miss Rate'),
        )
      )
      .addRow(
        g.row('Query Frontend - Sharding/Splitting')
        .addPanel(
          g.panel('Intervals per Query') +
          g.queryPanel('sum(rate(cortex_frontend_split_queries_total{cluster="$cluster", namespace="$namespace"}[1m])) / sum(rate(cortex_frontend_query_range_duration_seconds_count{cluster="$cluster", namespace="$namespace", method="split_by_interval"}[1m]))', 'partition rate'),
        )
        .addPanel(
          g.panel('Sharded Queries %') +
          g.queryPanel('sum(rate(cortex_frontend_mapped_asts_total{cluster="$cluster", namespace="$namespace"}[1m])) / sum(rate(cortex_frontend_split_queries_total{cluster="$cluster", namespace="$namespace"}[1m])) * 100', 'shard rate'),
        )
        .addPanel(
          g.panel('Sharding factor') +
          g.queryPanel('sum(rate(cortex_frontend_sharded_queries_total{cluster="$cluster", namespace="$namespace"}[1m])) / sum(rate(cortex_frontend_mapped_asts_total{cluster="$cluster", namespace="$namespace"}[1m]))', 'Average'),
        )
      )
      .addRow(
        g.row('Querier')
        .addPanel(
          g.panel('Stages') +
          g.queryPanel('max by (slice) (prometheus_engine_query_duration_seconds{quantile="0.9",cluster=~"$cluster",job=~"($namespace)/querier"}) * 1e3', '{{slice}}') +
          { yaxes: g.yaxes('ms') } +
          g.stack,
        )
        .addPanel(
          g.panel('Chunk cache misses') +
          g.queryPanel('sum(rate(cortex_cache_fetched_keys{cluster=~"$cluster",job=~"($namespace)/querier",name="chunksmemcache"}[1m])) - sum(rate(cortex_cache_hits{cluster=~"$cluster",job=~"($namespace)/querier",name="chunksmemcache"}[1m]))', 'Hit rate'),
        )
        .addPanel(
          g.panel('Chunk cache corruptions') +
          g.queryPanel('sum(rate(cortex_cache_corrupt_chunks_total{cluster=~"$cluster",job=~"($namespace)/querier"}[1m]))', 'Corrupt chunks'),
        )
      )
      .addRow(
        g.row('Querier - Index Cache')
        .addPanel(
          g.panel('Total entries') +
          g.queryPanel('sum(querier_cache_added_new_total{cache="store.index-cache-read.fifocache", cluster=~"$cluster",job=~"($namespace)/querier"}) - sum(querier_cache_evicted_total{cache="store.index-cache-read.fifocache", cluster=~"$cluster",job=~"($namespace)/querier"})', 'Entries'),
        )
        .addPanel(
          g.panel('Cache Hit %') +
          g.queryPanel('(sum(rate(querier_cache_gets_total{cache="store.index-cache-read.fifocache", cluster=~"$cluster",job=~"($namespace)/querier"}[1m])) - sum(rate(querier_cache_misses_total{cache="store.index-cache-read.fifocache", cluster=~"$cluster",job=~"($namespace)/querier"}[1m]))) / sum(rate(querier_cache_gets_total{cache="store.index-cache-read.fifocache", cluster=~"$cluster",job=~"($namespace)/querier"}[1m]))', 'hit rate')
          { yaxes: g.yaxes({ format: 'percentunit', max: 1 }) },
        )
        .addPanel(
          g.panel('Churn Rate') +
          g.queryPanel('sum(rate(querier_cache_evicted_total{cache="store.index-cache-read.fifocache", cluster=~"$cluster",job=~"($namespace)/querier"}[1m]))', 'churn rate'),
        )
      )
      .addRow(
        g.row('Ingester')
        .addPanel(
          g.panel('Series per Query') +
          utils.latencyRecordingRulePanel('cortex_ingester_queried_series', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/ingester')], multiplier=1) +
          { yaxes: g.yaxes('short') },
        )
        .addPanel(
          g.panel('Chunks per Query') +
          utils.latencyRecordingRulePanel('cortex_ingester_queried_chunks', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/ingester')], multiplier=1) +
          { yaxes: g.yaxes('short') },
        )
        .addPanel(
          g.panel('Samples per Query') +
          utils.latencyRecordingRulePanel('cortex_ingester_queried_samples', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/ingester')], multiplier=1) +
          { yaxes: g.yaxes('short') },
        )
      )
      .addRow(
        g.row('Chunk Store')
        .addPanel(
          g.panel('Index Lookups per Query') +
          utils.latencyRecordingRulePanel('cortex_chunk_store_index_lookups_per_query', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/querier')], multiplier=1) +
          { yaxes: g.yaxes('short') },
        )
        .addPanel(
          g.panel('Series (pre-intersection) per Query') +
          utils.latencyRecordingRulePanel('cortex_chunk_store_series_pre_intersection_per_query', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/querier')], multiplier=1) +
          { yaxes: g.yaxes('short') },
        )
        .addPanel(
          g.panel('Series (post-intersection) per Query') +
          utils.latencyRecordingRulePanel('cortex_chunk_store_series_post_intersection_per_query', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/querier')], multiplier=1) +
          { yaxes: g.yaxes('short') },
        )
        .addPanel(
          g.panel('Chunks per Query') +
          utils.latencyRecordingRulePanel('cortex_chunk_store_chunks_per_query', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/querier')], multiplier=1) +
          { yaxes: g.yaxes('short') },
        )
      ),

    'ruler.json':
      g.dashboard('Cortex / Ruler')
      .addMultiTemplate('cluster', 'kube_pod_container_info{image=~".*cortex.*"}', 'cluster')
      .addMultiTemplate('namespace', 'kube_pod_container_info{image=~".*cortex.*"}', 'namespace')
      .addRow(
        g.row('Rule Evaluations')
        .addPanel(
          g.panel('EPS') +
          g.queryPanel('sum(rate(cortex_prometheus_rule_evaluations_total{cluster=~"$cluster", job=~"($namespace)/ruler"}[$__interval]))', 'rules processed'),
        )
        .addPanel(
          g.panel('Latency') +
          g.queryPanel(
            |||
              sum (rate(cortex_prometheus_rule_evaluation_duration_seconds_sum{cluster=~"$cluster", job=~"($namespace)/ruler"}[$__interval]))
                /
              sum (rate(cortex_prometheus_rule_evaluation_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/ruler"}[$__interval]))
            |||, 'average'
          ),
        )
      )
      .addRow(
        g.row('Group Evaluations')
        .addPanel(
          g.panel('Missed Iterations') +
          g.queryPanel('sum(rate(cortex_prometheus_rule_group_iterations_missed_total{cluster=~"$cluster", job=~"($namespace)/ruler"}[$__interval]))', 'iterations missed'),
        )
        .addPanel(
          g.panel('Latency') +
          g.queryPanel(
            |||
              sum (rate(cortex_prometheus_rule_group_duration_seconds_sum{cluster=~"$cluster", job=~"($namespace)/ruler"}[$__interval]))
                /
              sum (rate(cortex_prometheus_rule_group_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/ruler"}[$__interval]))
            |||, 'average'
          ),
        )
      ),

    'cortex-scaling.json':
      g.dashboard('Cortex / Scaling')
      .addMultiTemplate('cluster', 'kube_pod_container_info{image=~".*cortex.*"}', 'cluster')
      .addMultiTemplate('namespace', 'kube_pod_container_info{image=~".*cortex.*"}', 'namespace')
      .addRow(
        g.row('Workload-based scaling')
        .addPanel(
          g.panel('Workload-based scaling') + { sort: { col: 1, desc: false } } +
          g.tablePanel([
            |||
              sum by (cluster, namespace, deployment) (
                kube_deployment_spec_replicas{cluster=~"$cluster", namespace=~"$namespace", deployment=~"ingester|memcached"}
                or
                label_replace(
                  kube_statefulset_replicas{cluster=~"$cluster", namespace=~"$namespace", deployment=~"ingester|memcached"},
                  "deployment", "$1", "statefulset", "(.*)"
                )
              )
            |||,
            |||
              quantile_over_time(0.99, sum by (cluster, namespace, deployment) (label_replace(rate(cortex_distributor_received_samples_total{cluster=~"$cluster", namespace=~"$namespace"}[1m]), "deployment", "ingester", "cluster", ".*"))[1h:])
                * 3 / 80e3
            |||,
            |||
              label_replace(
                sum by(cluster, namespace) (
                  cortex_ingester_memory_series{cluster=~"$cluster", namespace=~"$namespace"}
                ) / 1e+6,
                "deployment", "ingester", "cluster", ".*"
              )
                or
              label_replace(
                sum by (cluster, namespace) (
                  4 * cortex_ingester_memory_series{cluster=~"$cluster", namespace=~"$namespace", job=~".+/ingester"}
                    *
                  cortex_ingester_chunk_size_bytes_sum{cluster=~"$cluster", namespace=~"$namespace", job=~".+/ingester"}
                    /
                  cortex_ingester_chunk_size_bytes_count{cluster=~"$cluster", namespace=~"$namespace", job=~".+/ingester"}
                )
                  /
                avg by (cluster, namespace) (memcached_limit_bytes{cluster=~"$cluster", namespace=~"$namespace", job=~".+/memcached"}),
                "deployment", "memcached", "namespace", ".*"
              )
            |||,
          ], {
            cluster: { alias: 'Cluster' },
            namespace: { alias: 'Namespace' },
            deployment: { alias: 'Deployment' },
            'Value #A': { alias: 'Current Replicas', decimals: 0 },
            'Value #B': { alias: 'Required Replicas, by ingestion rate', decimals: 0 },
            'Value #C': { alias: 'Required Replicas, by active series', decimals: 0 },
          })
        )
      )
      .addRow(
        (g.row('Resource-based scaling') + { height: '500px' })
        .addPanel(
          g.panel('Resource-based scaling') + { sort: { col: 1, desc: false } } +
          g.tablePanel([
            |||
              sum by (cluster, namespace, deployment) (
                kube_deployment_spec_replicas{cluster=~"$cluster", namespace=~"$namespace"}
                or
                label_replace(
                  kube_statefulset_replicas{cluster=~"$cluster", namespace=~"$namespace"},
                  "deployment", "$1", "statefulset", "(.*)"
                )
              )
            |||,
            |||
              sum by (cluster, namespace, deployment) (
                kube_deployment_spec_replicas{cluster=~"$cluster", namespace=~"$namespace"}
                or
                label_replace(
                  kube_statefulset_replicas{cluster=~"$cluster", namespace=~"$namespace"},
                  "deployment", "$1", "statefulset", "(.*)"
                )
              )
                *
              quantile_over_time(0.99, sum by (cluster, namespace, deployment) (label_replace(rate(container_cpu_usage_seconds_total{cluster=~"$cluster", namespace=~"$namespace"}[1m]), "deployment", "$1", "pod_name", "(.*)-(?:([0-9]+)|([a-z0-9]+)-([a-z0-9]+))"))[24h:])
                /
              sum by (cluster, namespace, deployment) (label_replace(kube_pod_container_resource_requests_cpu_cores{cluster=~"$cluster", namespace=~"$namespace"}, "deployment", "$1", "pod", "(.*)-(?:([0-9]+)|([a-z0-9]+)-([a-z0-9]+))"))
            |||,
            |||
              sum by (cluster, namespace, deployment) (
                kube_deployment_spec_replicas{cluster=~"$cluster", namespace=~"$namespace"}
                or
                label_replace(
                  kube_statefulset_replicas{cluster=~"$cluster", namespace=~"$namespace"},
                  "deployment", "$1", "statefulset", "(.*)"
                )
              )
                *
              quantile_over_time(0.99, sum by (cluster, namespace, deployment) (label_replace(container_memory_usage_bytes{cluster=~"$cluster", namespace=~"$namespace"}, "deployment", "$1", "pod_name", "(.*)-(?:([0-9]+)|([a-z0-9]+)-([a-z0-9]+))"))[24h:1m])
                /
              sum by (cluster, namespace, deployment) (label_replace(kube_pod_container_resource_requests_memory_bytes{cluster=~"$cluster", namespace=~"$namespace"}, "deployment", "$1", "pod", "(.*)-(?:([0-9]+)|([a-z0-9]+)-([a-z0-9]+))"))
            |||,
          ], {
            cluster: { alias: 'Cluster' },
            namespace: { alias: 'Namespace' },
            deployment: { alias: 'Deployment' },
            'Value #A': { alias: 'Current Replicas', decimals: 0 },
            'Value #B': { alias: 'Required Replicas, by CPU usage', decimals: 0 },
            'Value #C': { alias: 'Required Replicas, by Memory usage', decimals: 0 },
          })
        )
      ),

    [if std.setMember('tsdb', $._config.storage_engine) then 'cortex-blocks.json' else null]:
      g.dashboard('Cortex / Blocks')
      .addMultiTemplate('cluster', 'kube_pod_container_info{image=~".*cortex.*"}', 'cluster')
      .addMultiTemplate('namespace', 'kube_pod_container_info{image=~".*cortex.*"}', 'namespace')
      // repeated from Cortex / Chunks
      .addRow(
        g.row('Active Series / Chunks')
        .addPanel(
          g.panel('Series') +
          g.queryPanel('sum(cortex_ingester_memory_series{cluster=~"$cluster", job=~"($namespace)/ingester"})', 'series'),
        )
        // Chunks per series doesn't make sense for Blocks storage
      )
      .addRow(
        g.row('Compactor')
        .addPanel(
          g.successFailurePanel(
            'Compactor Runs / second',
            'sum(rate(cortex_compactor_runs_completed_total{cluster=~"$cluster"}[$__interval]))',
            'sum(rate(cortex_compactor_runs_failed_total{cluster=~"$cluster"}[$__interval]))'
          )
        )
        .addPanel(
          g.successFailurePanel(
            'Per-tenant Compaction Runs / seconds',
            'sum(rate(cortex_compactor_group_compactions_total{cluster=~"$cluster", job=~"($namespace)/compactor"}[$__interval])) - sum(rate(cortex_compactor_group_compactions_failures_total{cluster=~"$cluster", job=~"($namespace)/compactor"}[$__interval]))',
            'sum(rate(cortex_compactor_group_compactions_failures_total{cluster=~"$cluster", job=~"($namespace)/compactor"}[$__interval]))',
          )
        )
      )
      .addRow(
        g.row('Compactor – Blocks Garbage Collections')
        .addPanel(
          g.successFailurePanel(
            'Collections Rate',
            'sum(rate(cortex_compactor_garbage_collection_total{cluster=~"$cluster", job=~"($namespace)/compactor"}[$__interval])) - sum(rate(cortex_compactor_garbage_collection_failures_total{cluster=~"$cluster", job=~"($namespace)/compactor"}[$__interval]))',
            'sum(rate(cortex_compactor_garbage_collection_failures_total{cluster=~"$cluster", job=~"($namespace)/compactor"}[$__interval]))',
          )
        )
        .addPanel(
          g.panel('Collections Duration') +
          g.latencyPanel('cortex_compactor_garbage_collection_duration_seconds', '{cluster=~"$cluster", job=~"($namespace)/compactor"}')
        )
        .addPanel(
          g.panel('Collected Blocks Rate') +
          g.queryPanel('sum(rate(cortex_compactor_garbage_collected_blocks_total{cluster=~"$cluster"}[$__interval]))', 'blocks')
        )
      )
      .addRow(
        g.row('Compactor - Meta Syncs')
        .addPanel(
          g.successFailurePanel(
            'Meta Syncs / sec',
            'sum(rate(cortex_compactor_sync_meta_total{cluster=~"$cluster", job=~"($namespace)/compactor"}[$__interval])) - sum(rate(cortex_compactor_sync_meta_failures_total{cluster=~"$cluster", job=~"($namespace)/compactor"}[$__interval]))',
            'sum(rate(cortex_compactor_sync_meta_failures_total{cluster=~"$cluster", job=~"($namespace)/compactor"}[$__interval]))',
          )
        )
        .addPanel(
          g.panel('Meta Sync Durations') +
          g.latencyPanel('cortex_compactor_sync_meta_duration_seconds', '{cluster=~"$cluster"}'),
        )
      )
      .addRow(
        g.row('Prometheus TSDB Compactions')
        .addPanel(
          g.panel('Compactions Rate') +
          g.queryPanel('sum(rate(prometheus_tsdb_compactions_total{cluster=~"$cluster", job=~"($namespace)/compactor"}[$__interval]))', 'rate')
        )
        .addPanel(
          g.panel('Compaction Duration') +
          g.latencyPanel('prometheus_tsdb_compaction_duration_seconds', '{cluster=~"$cluster", job=~"($namespace)/compactor"}')
        )
        .addPanel(
          g.panel('Chunk Size Bytes') +
          g.latencyPanel('prometheus_tsdb_compaction_chunk_size_bytes', '{cluster=~"$cluster", job=~"($namespace)/compactor"}') +
          { yaxes: g.yaxes('bytes') }
        )
        .addPanel(
          g.panel('Chunk Samples') +
          g.latencyPanel('prometheus_tsdb_compaction_chunk_samples', '{cluster=~"$cluster", job=~"($namespace)/compactor"}') +
          { yaxes: g.yaxes('short') }
        )
        .addPanel(
          g.panel('Chunk Range (seconds)') +
          g.latencyPanel('prometheus_tsdb_compaction_chunk_range_seconds', '{cluster=~"$cluster", job=~"($namespace)/compactor"}')
        )
      )
      .addRow(g.objectStorePanels1('Object Store Stats', 'cortex_compactor'))
      .addRow(g.objectStorePanels2('', 'cortex_compactor')),

    [if std.setMember('tsdb', $._config.storage_engine) && std.setMember('chunks', $._config.storage_engine) then 'cortex-blocks-vs-chunks.json' else null]:
      g.dashboard('Cortex / Blocks vs Chunks')
      .addMultiTemplate('cluster', 'kube_pod_container_info{image=~".*cortex.*"}', 'cluster')
      .addTemplate('blocks_namespace', 'kube_pod_container_info{image=~".*cortex.*"}', 'namespace')
      .addTemplate('chunks_namespace', 'kube_pod_container_info{image=~".*cortex.*"}', 'namespace')
      .addRow(
        g.row('Ingesters')
        .addPanel(
          g.panel('Samples / sec') +
          g.queryPanel('sum(rate(cortex_ingester_ingested_samples_total{cluster=~"$cluster",job=~"($blocks_namespace)/ingester"}[$__interval]))', 'blocks') +
          g.queryPanel('sum(rate(cortex_ingester_ingested_samples_total{cluster=~"$cluster",job=~"($chunks_namespace)/ingester"}[$__interval]))', 'chunks')
        )
      )
      .addRow(
        g.row('')
        .addPanel(
          g.panel('Blocks Latency') +
          utils.latencyRecordingRulePanel('cortex_request_duration_seconds', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($blocks_namespace)/ingester'), utils.selector.eq('route', '/cortex.Ingester/Push')])
        )
        .addPanel(
          g.panel('Chunks Latency') +
          utils.latencyRecordingRulePanel('cortex_request_duration_seconds', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($chunks_namespace)/ingester'), utils.selector.eq('route', '/cortex.Ingester/Push')])
        )
      )
      .addRow(
        g.row('')
        .addPanel(
          g.panel('CPU per sample') +
          g.queryPanel('sum(rate(container_cpu_usage_seconds_total{cluster=~"$cluster",namespace="$blocks_namespace",container_name="ingester"}[$__interval])) / sum(rate(cortex_ingester_ingested_samples_total{cluster=~"$cluster",job="$blocks_namespace/ingester"}[$__interval]))', 'blocks') +
          g.queryPanel('sum(rate(container_cpu_usage_seconds_total{cluster=~"$cluster",namespace="$chunks_namespace",container_name="ingester"}[$__interval])) / sum(rate(cortex_ingester_ingested_samples_total{cluster=~"$cluster",job="$chunks_namespace/ingester"}[$__interval]))', 'chunks')
        )
        .addPanel(
          g.panel('Memory per active series') +
          g.queryPanel('sum(container_memory_working_set_bytes{cluster=~"$cluster",namespace="$blocks_namespace",container_name="ingester"}) / sum(cortex_ingester_memory_series{cluster=~"$cluster",job=~"$blocks_namespace/ingester"})', 'blocks - working set') +
          g.queryPanel('sum(container_memory_working_set_bytes{cluster=~"$cluster",namespace="$chunks_namespace",container_name="ingester"}) / sum(cortex_ingester_memory_series{cluster=~"$cluster",job=~"$chunks_namespace/ingester"})', 'chunks - working set') +
          g.queryPanel('sum(go_memstats_heap_inuse_bytes{cluster=~"$cluster",job=~"$blocks_namespace/ingester"}) / sum(cortex_ingester_memory_series{cluster=~"$cluster",job=~"$blocks_namespace/ingester"})', 'blocks - heap inuse') +
          g.queryPanel('sum(go_memstats_heap_inuse_bytes{cluster=~"$cluster",job=~"$chunks_namespace/ingester"}) / sum(cortex_ingester_memory_series{cluster=~"$cluster",job=~"$chunks_namespace/ingester"})', 'chunks - heap inuse') +
          { yaxes: g.yaxes('bytes') }
        )
      )
      .addRow(
        g.row('')
        .addPanel(
          g.panel('CPU') +
          g.queryPanel('sum(rate(container_cpu_usage_seconds_total{cluster=~"$cluster",namespace="$blocks_namespace",container_name="ingester"}[$__interval]))', 'blocks') +
          g.queryPanel('sum(rate(container_cpu_usage_seconds_total{cluster=~"$cluster",namespace="$chunks_namespace",container_name="ingester"}[$__interval]))', 'chunks')
        )
        .addPanel(
          g.panel('Memory') +
          g.queryPanel('sum(container_memory_working_set_bytes{cluster=~"$cluster",namespace="$blocks_namespace",container_name="ingester"})', 'blocks - working set') +
          g.queryPanel('sum(container_memory_working_set_bytes{cluster=~"$cluster",namespace="$chunks_namespace",container_name="ingester"})', 'chunks - working set') +
          g.queryPanel('sum(go_memstats_heap_inuse_bytes{cluster=~"$cluster",job=~"$blocks_namespace/ingester"})', 'blocks - heap inuse') +
          g.queryPanel('sum(go_memstats_heap_inuse_bytes{cluster=~"$cluster",job=~"$chunks_namespace/ingester"})', 'chunks - heap inuse') +
          { yaxes: g.yaxes('bytes') }
        )
      )
      .addRow(
        g.row('Queriers')
        .addPanel(
          g.panel('Queries / sec (query-frontend)') +
          g.queryPanel('sum(rate(cortex_request_duration_seconds_count{cluster=~"$cluster",job="$blocks_namespace/query-frontend",route!="metrics"}[$__interval]))', 'blocks') +
          g.queryPanel('sum(rate(cortex_request_duration_seconds_count{cluster=~"$cluster",job="$chunks_namespace/query-frontend",route!="metrics"}[$__interval]))', 'chunks')
        )
        .addPanel(
          g.panel('Queries / sec (query-tee)') +
          g.queryPanel('sum(rate(cortex_querytee_request_duration_seconds_count{cluster=~"$cluster",backend=~".*\\\\.$blocks_namespace\\\\..*"}[$__interval]))', 'blocks') +
          g.queryPanel('sum(rate(cortex_querytee_request_duration_seconds_count{cluster=~"$cluster",backend=~".*\\\\.$chunks_namespace\\\\..*"}[$__interval]))', 'chunks')
        )
      )
      .addRow(
        g.row('')
        .addPanel(
          g.panel('Latency 99th') +
          g.queryPanel('histogram_quantile(0.99, sum by(backend, le) (rate(cortex_querytee_request_duration_seconds_bucket{cluster=~"$cluster",backend=~".*\\\\.$blocks_namespace\\\\..*"}[$__interval])))', 'blocks') +
          g.queryPanel('histogram_quantile(0.99, sum by(backend, le) (rate(cortex_querytee_request_duration_seconds_bucket{cluster=~"$cluster",backend=~".*\\\\.$chunks_namespace\\\\..*"}[$__interval])))', 'chunks') +
          { yaxes: g.yaxes('s') }
        )
        .addPanel(
          g.panel('Latency average') +
          g.queryPanel('sum by(backend) (rate(cortex_querytee_request_duration_seconds_sum{cluster=~"$cluster",backend=~".*\\\\.$blocks_namespace\\\\..*"}[$__interval])) / sum by(backend) (rate(cortex_querytee_request_duration_seconds_count{cluster=~"$cluster",backend=~".*\\\\.$blocks_namespace\\\\..*"}[$__interval]))', 'blocks') +
          g.queryPanel('sum by(backend) (rate(cortex_querytee_request_duration_seconds_sum{cluster=~"$cluster",backend=~".*\\\\.$chunks_namespace\\\\..*"}[$__interval])) / sum by(backend) (rate(cortex_querytee_request_duration_seconds_count{cluster=~"$cluster",backend=~".*\\\\.$chunks_namespace\\\\..*"}[$__interval]))', 'chunks') +
          { yaxes: g.yaxes('s') }
        )
      )
      .addRow(
        g.row('')
        .addPanel(
          g.panel('CPU') +
          g.queryPanel('sum(rate(container_cpu_usage_seconds_total{cluster=~"$cluster",namespace="$blocks_namespace",container_name="querier"}[$__interval]))', 'blocks') +
          g.queryPanel('sum(rate(container_cpu_usage_seconds_total{cluster=~"$cluster",namespace="$chunks_namespace",container_name="querier"}[$__interval]))', 'chunks')
        )
        .addPanel(
          g.panel('Memory') +
          g.queryPanel('sum(container_memory_working_set_bytes{cluster=~"$cluster",namespace="$blocks_namespace",container_name="querier"})', 'blocks - working set') +
          g.queryPanel('sum(container_memory_working_set_bytes{cluster=~"$cluster",namespace="$chunks_namespace",container_name="querier"})', 'chunks - working set') +
          g.queryPanel('sum(go_memstats_heap_inuse_bytes{cluster=~"$cluster",job=~"$blocks_namespace/querier"})', 'blocks - heap inuse') +
          g.queryPanel('sum(go_memstats_heap_inuse_bytes{cluster=~"$cluster",job=~"$chunks_namespace/querier"})', 'chunks - heap inuse') +
          { yaxes: g.yaxes('bytes') }
        )
      ),
  },

  cortex_writes_dashboard::
    local out =
      g.dashboard('Cortex / Writes')
      .addMultiTemplate('cluster', 'kube_pod_container_info{image=~".*cortex.*"}', 'cluster')
      .addMultiTemplate('namespace', 'kube_pod_container_info{image=~".*cortex.*"}', 'namespace')
      .addRow(
        (g.row('Headlines') +
         {
           height: '100px',
           showTitle: false,
         })
        .addPanel(
          g.panel('Samples / s') +
          g.statPanel('sum(cluster_namespace:cortex_distributor_received_samples:rate5m{cluster=~"$cluster", namespace=~"$namespace"})', format='reqps')
        )
        .addPanel(
          g.panel('Active Series') +
          g.statPanel(|||
            sum(cortex_ingester_memory_series{cluster=~"$cluster", job=~"($namespace)/ingester"}
            / on(namespace) group_left
            max by (namespace) (cortex_distributor_replication_factor{cluster=~"$cluster", job=~"($namespace)/distributor"}))
          |||, format='short')
        )
        .addPanel(
          g.panel('QPS') +
          g.statPanel('sum(rate(cortex_request_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/cortex-gw", route="api_prom_push"}[5m]))', format='reqps')
        )
      )
      .addRow(
        g.row('Gateway')
        .addPanel(
          g.panel('QPS') +
          g.qpsPanel('cortex_request_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/cortex-gw", route="api_prom_push"}')
        )
        .addPanel(
          g.panel('Latency') +
          utils.latencyRecordingRulePanel('cortex_request_duration_seconds', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/cortex-gw'), utils.selector.eq('route', 'api_prom_push')])
        )
      )
      .addRow(
        g.row('Distributor')
        .addPanel(
          g.panel('QPS') +
          g.qpsPanel('cortex_request_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/distributor"}')
        )
        .addPanel(
          g.panel('Latency') +
          utils.latencyRecordingRulePanel('cortex_request_duration_seconds', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/distributor')])
        )
      )
      .addRow(
        g.row('Etcd (HA Dedupe)')
        .addPanel(
          g.panel('QPS') +
          g.qpsPanel('cortex_kv_request_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/distributor"}')
        )
        .addPanel(
          g.panel('Latency') +
          utils.latencyRecordingRulePanel('cortex_kv_request_duration_seconds', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/distributor')])
        )
      )
      .addRow(
        g.row('Ingester')
        .addPanel(
          g.panel('QPS') +
          g.qpsPanel('cortex_request_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/ingester",route="/cortex.Ingester/Push"}')
        )
        .addPanel(
          g.panel('Latency') +
          utils.latencyRecordingRulePanel('cortex_request_duration_seconds', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/ingester'), utils.selector.eq('route', '/cortex.Ingester/Push')])
        )
      )
      .addRow(
        g.row('Consul (Ring)')
        .addPanel(
          g.panel('QPS') +
          g.qpsPanel('cortex_kv_request_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/ingester"}')
        )
        .addPanel(
          g.panel('Latency') +
          utils.latencyRecordingRulePanel('cortex_kv_request_duration_seconds', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/ingester')])
        )
      );

    local addChunksRows(dashboard) =
      if std.setMember('chunks', $._config.storage_engine) then
        dashboard.addRow(
          g.row('Memcached')
          .addPanel(
            g.panel('QPS') +
            g.qpsPanel('cortex_memcache_request_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/ingester",method="Memcache.Put"}')
          )
          .addPanel(
            g.panel('Latency') +
            utils.latencyRecordingRulePanel('cortex_memcache_request_duration_seconds', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/ingester'), utils.selector.eq('method', 'Memcache.Put')])
          )
        ) else dashboard;

    local addStorageRows(dashboard) = if $._config.storage_backend == 'cassandra' then
      dashboard.addRow(
        g.row('Cassandra')
        .addPanel(
          g.panel('QPS') +
          g.qpsPanel('cortex_cassandra_request_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/ingester", operation="INSERT"}')
        )
        .addPanel(
          g.panel('Latency') +
          utils.latencyRecordingRulePanel('cortex_cassandra_request_duration_seconds', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/ingester'), utils.selector.eq('operation', 'INSERT')])
        )
      )
    else if $._config.storage_backend == 'gcp' && std.setMember('chunks', $._config.storage_engine) then  // only show BigTable if chunks panels are enabled
      dashboard.addRow(
        g.row('BigTable')
        .addPanel(
          g.panel('QPS') +
          g.qpsPanel('cortex_bigtable_request_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/ingester", operation="/google.bigtable.v2.Bigtable/MutateRows"}')
        )
        .addPanel(
          g.panel('Latency') +
          utils.latencyRecordingRulePanel('cortex_bigtable_request_duration_seconds', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/ingester'), utils.selector.eq('operation', '/google.bigtable.v2.Bigtable/MutateRows')])
        )
      )
    else if $._config.storage_backend == 'dynamodb' then
      dashboard.addRow(
        g.row('DynamoDB')
        .addPanel(
          g.panel('QPS') +
          g.qpsPanel('cortex_dynamo_request_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/ingester", operation="DynamoDB.BatchWriteItem"}')
        )
        .addPanel(
          g.panel('Latency') +
          utils.latencyRecordingRulePanel('cortex_dynamo_request_duration_seconds', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/ingester'), utils.selector.eq('operation', 'DynamoDB.BatchWriteItem')])
        )
      ) else dashboard;

    addStorageRows(addChunksRows(out)),

  cortex_reads_dashboard::
    local out =
      g.dashboard('Cortex / Reads')
      .addMultiTemplate('cluster', 'kube_pod_container_info{image=~".*cortex.*"}', 'cluster')
      .addMultiTemplate('namespace', 'kube_pod_container_info{image=~".*cortex.*"}', 'namespace')
      .addRow(
        g.row('Gateway')
        .addPanel(
          g.panel('QPS') +
          g.qpsPanel('cortex_request_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/cortex-gw", route=~"(api_prom_api_v1_query_range|api_prom_api_v1_query|api_prom_api_v1_label_name_values|api_prom_api_v1_series|api_prom_api_v1_labels)"}')
        )
        .addPanel(
          g.panel('Latency') +
          utils.latencyRecordingRulePanel('cortex_request_duration_seconds', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/cortex-gw'), utils.selector.re('route', '(api_prom_api_v1_query_range|api_prom_api_v1_query|api_prom_api_v1_label_name_values|api_prom_api_v1_series|api_prom_api_v1_labels)')])
        )
      )
      .addRow(
        g.row('Query Frontend')
        .addPanel(
          g.panel('QPS') +
          g.qpsPanel('cortex_request_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/query-frontend"}')
        )
        .addPanel(
          g.panel('Latency') +
          utils.latencyRecordingRulePanel('cortex_request_duration_seconds', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/query-frontend'), utils.selector.neq('route', '/frontend.Frontend/Process')])
        )
      )
      .addRow(
        g.row('Cache - Query Results')
        .addPanel(
          g.panel('QPS') +
          g.qpsPanel('cortex_cache_request_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/query-frontend"}')
        )
        .addPanel(
          g.panel('Latency') +
          utils.latencyRecordingRulePanel('cortex_cache_request_duration_seconds', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/query-frontend')])
        )
      )
      .addRow(
        g.row('Querier')
        .addPanel(
          g.panel('QPS') +
          g.qpsPanel('cortex_request_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/querier"}')
        )
        .addPanel(
          g.panel('Latency') +
          utils.latencyRecordingRulePanel('cortex_request_duration_seconds', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/querier')])
        )
      )
      .addRow(
        g.row('Ingester')
        .addPanel(
          g.panel('QPS') +
          g.qpsPanel('cortex_request_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/ingester",route!~"/cortex.Ingester/Push|metrics|ready|traces"}')
        )
        .addPanel(
          g.panel('Latency') +
          utils.latencyRecordingRulePanel('cortex_request_duration_seconds', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/ingester'), utils.selector.nre('route', '/cortex.Ingester/Push|metrics|ready')])
        )
      );

    local addChunksRows(dashboard) = if std.setMember('chunks', $._config.storage_engine) then
      dashboard.addRow(
        g.row('Memcached - Index')
        .addPanel(
          g.panel('QPS') +
          g.qpsPanel('cortex_cache_request_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/querier",method="store.index-cache-read.memcache.fetch"}')
        )
        .addPanel(
          g.panel('Latency') +
          utils.latencyRecordingRulePanel('cortex_cache_request_duration_seconds', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/querier'), utils.selector.eq('method', 'store.index-cache-read.memcache.fetch')])
        )
      )
      .addRow(
        g.row('Memcached - Chunks')
        .addPanel(
          g.panel('QPS') +
          g.qpsPanel('cortex_cache_request_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/querier",method="chunksmemcache.fetch"}')
        )
        .addPanel(
          g.panel('Latency') +
          utils.latencyRecordingRulePanel('cortex_cache_request_duration_seconds', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/querier'), utils.selector.eq('method', 'chunksmemcache.fetch')])
        )
      ) else dashboard;

    local addBlocksRows(dashboard) = if std.setMember('tsdb', $._config.storage_engine) then
      dashboard.addRow(
        g.row('Memcached - Blocks Index')
        .addPanel(
          g.panel('QPS') +
          g.qpsPanel('cortex_querier_blocks_index_cache_memcached_operation_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/querier",operation="getmulti"}')
        )
        .addPanel(
          g.panel('Latency') +
          g.latencyPanel('cortex_querier_blocks_index_cache_memcached_operation_duration_seconds', '{cluster=~"$cluster", job=~"($namespace)/querier", operation="getmulti"}')
        )
      )
    else dashboard;

    local addStorageRows(dashboard) =
      if $._config.storage_backend == 'cassandra' then
        dashboard.addRow(
          g.row('Cassandra')
          .addPanel(
            g.panel('QPS') +
            g.qpsPanel('cortex_cassandra_request_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/querier", operation="SELECT"}')
          )
          .addPanel(
            g.panel('Latency') +
            utils.latencyRecordingRulePanel('cortex_cassandra_request_duration_seconds', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/querier'), utils.selector.eq('operation', 'SELECT')])
          ),
        )
      else if $._config.storage_backend == 'gcp' && std.setMember('chunks', $._config.storage_engine) then  // only show BigTable if chunks panels are enabled
        dashboard.addRow(
          g.row('BigTable')
          .addPanel(
            g.panel('QPS') +
            g.qpsPanel('cortex_bigtable_request_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/querier", operation="/google.bigtable.v2.Bigtable/ReadRows"}')
          )
          .addPanel(
            g.panel('Latency') +
            utils.latencyRecordingRulePanel('cortex_bigtable_request_duration_seconds', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/querier'), utils.selector.eq('operation', '/google.bigtable.v2.Bigtable/ReadRows')])
          ),
        )
      else if $._config.storage_backend == 'dynamodb' then
        dashboard.addRow(
          g.row('DynamoDB')
          .addPanel(
            g.panel('QPS') +
            g.qpsPanel('cortex_dynamo_request_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/querier", operation="DynamoDB.QueryPages"}')
          )
          .addPanel(
            g.panel('Latency') +
            utils.latencyRecordingRulePanel('cortex_dynamo_request_duration_seconds', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/querier'), utils.selector.eq('operation', 'DynamoDB.QueryPages')])
          ),
        ) else dashboard;

    addStorageRows(addBlocksRows(addChunksRows(out))),
}
