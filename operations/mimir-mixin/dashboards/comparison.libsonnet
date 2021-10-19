local utils = import 'mixin-utils/utils.libsonnet';

(import 'dashboard-utils.libsonnet')
{
  'cortex-blocks-vs-chunks.json':
    ($.dashboard('Cortex / Blocks vs Chunks') + { uid: '0e2b4dd23df9921972e3fb554c0fc483' })
    .addMultiTemplate('cluster', 'kube_pod_container_info{image=~".*cortex.*"}', 'cluster')
    .addTemplate('blocks_namespace', 'kube_pod_container_info{image=~".*cortex.*"}', 'namespace')
    .addTemplate('chunks_namespace', 'kube_pod_container_info{image=~".*cortex.*"}', 'namespace')
    .addRow(
      $.row('Ingesters')
      .addPanel(
        $.panel('Samples / sec') +
        $.queryPanel('sum(rate(cortex_ingester_ingested_samples_total{cluster=~"$cluster",job=~"($blocks_namespace)/ingester"}[$__rate_interval]))', 'blocks') +
        $.queryPanel('sum(rate(cortex_ingester_ingested_samples_total{cluster=~"$cluster",job=~"($chunks_namespace)/ingester"}[$__rate_interval]))', 'chunks')
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.panel('Blocks Latency') +
        utils.latencyRecordingRulePanel('cortex_request_duration_seconds', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($blocks_namespace)/ingester'), utils.selector.eq('route', '/cortex.Ingester/Push')])
      )
      .addPanel(
        $.panel('Chunks Latency') +
        utils.latencyRecordingRulePanel('cortex_request_duration_seconds', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($chunks_namespace)/ingester'), utils.selector.eq('route', '/cortex.Ingester/Push')])
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.panel('CPU per sample') +
        $.queryPanel('sum(rate(container_cpu_usage_seconds_total{cluster=~"$cluster",namespace="$blocks_namespace",container="ingester"}[$__rate_interval])) / sum(rate(cortex_ingester_ingested_samples_total{cluster=~"$cluster",job="$blocks_namespace/ingester"}[$__rate_interval]))', 'blocks') +
        $.queryPanel('sum(rate(container_cpu_usage_seconds_total{cluster=~"$cluster",namespace="$chunks_namespace",container="ingester"}[$__rate_interval])) / sum(rate(cortex_ingester_ingested_samples_total{cluster=~"$cluster",job="$chunks_namespace/ingester"}[$__rate_interval]))', 'chunks')
      )
      .addPanel(
        $.panel('Memory per active series') +
        $.queryPanel('sum(container_memory_working_set_bytes{cluster=~"$cluster",namespace="$blocks_namespace",container="ingester"}) / sum(cortex_ingester_memory_series{cluster=~"$cluster",job=~"$blocks_namespace/ingester"})', 'blocks - working set') +
        $.queryPanel('sum(container_memory_working_set_bytes{cluster=~"$cluster",namespace="$chunks_namespace",container="ingester"}) / sum(cortex_ingester_memory_series{cluster=~"$cluster",job=~"$chunks_namespace/ingester"})', 'chunks - working set') +
        $.queryPanel('sum(go_memstats_heap_inuse_bytes{cluster=~"$cluster",job=~"$blocks_namespace/ingester"}) / sum(cortex_ingester_memory_series{cluster=~"$cluster",job=~"$blocks_namespace/ingester"})', 'blocks - heap inuse') +
        $.queryPanel('sum(go_memstats_heap_inuse_bytes{cluster=~"$cluster",job=~"$chunks_namespace/ingester"}) / sum(cortex_ingester_memory_series{cluster=~"$cluster",job=~"$chunks_namespace/ingester"})', 'chunks - heap inuse') +
        { yaxes: $.yaxes('bytes') }
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.panel('CPU') +
        $.queryPanel('sum(rate(container_cpu_usage_seconds_total{cluster=~"$cluster",namespace="$blocks_namespace",container="ingester"}[$__rate_interval]))', 'blocks') +
        $.queryPanel('sum(rate(container_cpu_usage_seconds_total{cluster=~"$cluster",namespace="$chunks_namespace",container="ingester"}[$__rate_interval]))', 'chunks')
      )
      .addPanel(
        $.panel('Memory') +
        $.queryPanel('sum(container_memory_working_set_bytes{cluster=~"$cluster",namespace="$blocks_namespace",container="ingester"})', 'blocks - working set') +
        $.queryPanel('sum(container_memory_working_set_bytes{cluster=~"$cluster",namespace="$chunks_namespace",container="ingester"})', 'chunks - working set') +
        $.queryPanel('sum(go_memstats_heap_inuse_bytes{cluster=~"$cluster",job=~"$blocks_namespace/ingester"})', 'blocks - heap inuse') +
        $.queryPanel('sum(go_memstats_heap_inuse_bytes{cluster=~"$cluster",job=~"$chunks_namespace/ingester"})', 'chunks - heap inuse') +
        { yaxes: $.yaxes('bytes') }
      )
    )
    .addRow(
      $.row('Queriers')
      .addPanel(
        $.panel('Queries / sec (query-frontend)') +
        $.queryPanel('sum(rate(cortex_request_duration_seconds_count{cluster=~"$cluster",job="$blocks_namespace/query-frontend",route!="metrics"}[$__rate_interval]))', 'blocks') +
        $.queryPanel('sum(rate(cortex_request_duration_seconds_count{cluster=~"$cluster",job="$chunks_namespace/query-frontend",route!="metrics"}[$__rate_interval]))', 'chunks')
      )
      .addPanel(
        $.panel('Queries / sec (query-tee)') +
        $.queryPanel('sum(rate(cortex_querytee_request_duration_seconds_count{cluster=~"$cluster",backend=~".*\\\\.$blocks_namespace\\\\..*"}[$__rate_interval]))', 'blocks') +
        $.queryPanel('sum(rate(cortex_querytee_request_duration_seconds_count{cluster=~"$cluster",backend=~".*\\\\.$chunks_namespace\\\\..*"}[$__rate_interval]))', 'chunks')
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.panel('Latency 99th') +
        $.queryPanel('histogram_quantile(0.99, sum by(backend, le) (rate(cortex_querytee_request_duration_seconds_bucket{cluster=~"$cluster",backend=~".*\\\\.$blocks_namespace\\\\..*"}[$__rate_interval])))', 'blocks') +
        $.queryPanel('histogram_quantile(0.99, sum by(backend, le) (rate(cortex_querytee_request_duration_seconds_bucket{cluster=~"$cluster",backend=~".*\\\\.$chunks_namespace\\\\..*"}[$__rate_interval])))', 'chunks') +
        { yaxes: $.yaxes('s') }
      )
      .addPanel(
        $.panel('Latency average') +
        $.queryPanel('sum by(backend) (rate(cortex_querytee_request_duration_seconds_sum{cluster=~"$cluster",backend=~".*\\\\.$blocks_namespace\\\\..*"}[$__rate_interval])) / sum by(backend) (rate(cortex_querytee_request_duration_seconds_count{cluster=~"$cluster",backend=~".*\\\\.$blocks_namespace\\\\..*"}[$__rate_interval]))', 'blocks') +
        $.queryPanel('sum by(backend) (rate(cortex_querytee_request_duration_seconds_sum{cluster=~"$cluster",backend=~".*\\\\.$chunks_namespace\\\\..*"}[$__rate_interval])) / sum by(backend) (rate(cortex_querytee_request_duration_seconds_count{cluster=~"$cluster",backend=~".*\\\\.$chunks_namespace\\\\..*"}[$__rate_interval]))', 'chunks') +
        { yaxes: $.yaxes('s') }
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.panel('CPU') +
        $.queryPanel('sum(rate(container_cpu_usage_seconds_total{cluster=~"$cluster",namespace="$blocks_namespace",container="querier"}[$__rate_interval]))', 'blocks') +
        $.queryPanel('sum(rate(container_cpu_usage_seconds_total{cluster=~"$cluster",namespace="$chunks_namespace",container="querier"}[$__rate_interval]))', 'chunks')
      )
      .addPanel(
        $.panel('Memory') +
        $.queryPanel('sum(container_memory_working_set_bytes{cluster=~"$cluster",namespace="$blocks_namespace",container="querier"})', 'blocks - working set') +
        $.queryPanel('sum(container_memory_working_set_bytes{cluster=~"$cluster",namespace="$chunks_namespace",container="querier"})', 'chunks - working set') +
        $.queryPanel('sum(go_memstats_heap_inuse_bytes{cluster=~"$cluster",job=~"$blocks_namespace/querier"})', 'blocks - heap inuse') +
        $.queryPanel('sum(go_memstats_heap_inuse_bytes{cluster=~"$cluster",job=~"$chunks_namespace/querier"})', 'chunks - heap inuse') +
        { yaxes: $.yaxes('bytes') }
      )
    ),
}
