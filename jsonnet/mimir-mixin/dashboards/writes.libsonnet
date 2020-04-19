local utils = import 'mixin-utils/utils.libsonnet';

(import 'dashboard-utils.libsonnet') {
  'cortex-writes.json':
    $.dashboard('Cortex / Writes')
    .addMultiTemplate('cluster', 'kube_pod_container_info{image=~".*cortex.*"}', 'cluster')
    .addMultiTemplate('namespace', 'kube_pod_container_info{image=~".*cortex.*"}', 'namespace')
    .addRow(
      ($.row('Headlines') +
       {
         height: '100px',
         showTitle: false,
       })
      .addPanel(
        $.panel('Samples / s') +
        $.statPanel('sum(cluster_namespace:cortex_distributor_received_samples:rate5m{cluster=~"$cluster", namespace=~"$namespace"})', format='reqps')
      )
      .addPanel(
        $.panel('Active Series') +
        $.statPanel(|||
          sum(cortex_ingester_memory_series{cluster=~"$cluster", job=~"($namespace)/ingester"}
          / on(namespace) group_left
          max by (namespace) (cortex_distributor_replication_factor{cluster=~"$cluster", job=~"($namespace)/distributor"}))
        |||, format='short')
      )
      .addPanel(
        $.panel('QPS') +
        $.statPanel('sum(rate(cortex_request_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/cortex-gw", route="api_prom_push"}[5m]))', format='reqps')
      )
    )
    .addRow(
      $.row('Gateway')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_request_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/cortex-gw", route="api_prom_push"}')
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_request_duration_seconds', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/cortex-gw'), utils.selector.eq('route', 'api_prom_push')])
      )
    )
    .addRow(
      $.row('Distributor')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_request_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/distributor"}')
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_request_duration_seconds', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/distributor')])
      )
    )
    .addRow(
      $.row('Etcd (HA Dedupe)')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_kv_request_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/distributor"}')
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_kv_request_duration_seconds', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/distributor')])
      )
    )
    .addRow(
      $.row('Ingester')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_request_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/ingester",route="/cortex.Ingester/Push"}')
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_request_duration_seconds', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/ingester'), utils.selector.eq('route', '/cortex.Ingester/Push')])
      )
    )
    .addRow(
      $.row('Consul (Ring)')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_kv_request_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/ingester"}')
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_kv_request_duration_seconds', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/ingester')])
      )
    )
    .addRowIf(
      std.setMember('chunks', $._config.storage_engine),
      $.row('Memcached')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_memcache_request_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/ingester",method="Memcache.Put"}')
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_memcache_request_duration_seconds', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/ingester'), utils.selector.eq('method', 'Memcache.Put')])
      )
    )
    .addRowIf(
      $._config.storage_backend == 'cassandra',
      $.row('Cassandra')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_cassandra_request_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/ingester", operation="INSERT"}')
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_cassandra_request_duration_seconds', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/ingester'), utils.selector.eq('operation', 'INSERT')])
      )
    )
    .addRowIf(
      // only show BigTable if chunks panels are enabled
      $._config.storage_backend == 'gcp' && std.setMember('chunks', $._config.storage_engine),
      $.row('BigTable')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_bigtable_request_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/ingester", operation="/google.bigtable.v2.Bigtable/MutateRows"}')
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_bigtable_request_duration_seconds', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/ingester'), utils.selector.eq('operation', '/google.bigtable.v2.Bigtable/MutateRows')])
      )
    )
    .addRowIf(
      $._config.storage_backend == 'dynamodb',
      $.row('DynamoDB')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_dynamo_request_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/ingester", operation="DynamoDB.BatchWriteItem"}')
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_dynamo_request_duration_seconds', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/ingester'), utils.selector.eq('operation', 'DynamoDB.BatchWriteItem')])
      )
    )
    .addRowIf(
      $._config.gcs_enabled,
      $.row('GCS')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_gcs_request_duration_seconds_count{cluster=~"$cluster", job=~"($namespace)/querier", operation="POST"}')
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_gcs_request_duration_seconds', [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/querier'), utils.selector.eq('operation', 'POST')])
      )
    )
    .addRowIf(
      std.setMember('tsdb', $._config.storage_engine),
      $.row('Blocks Shipper')
      .addPanel(
        $.successFailurePanel(
          'Uploaded blocks / sec',
          'sum(rate(cortex_ingester_shipper_uploads_total{cluster=~"$cluster"}[$__interval])) - sum(rate(cortex_ingester_shipper_upload_failures_total{cluster=~"$cluster"}[$__interval]))',
          'sum(rate(cortex_ingester_shipper_upload_failures_total{cluster=~"$cluster"}[$__interval]))'
        ),
      )
    )
    .addRowIf(
      std.setMember('tsdb', $._config.storage_engine),
      $.objectStorePanels1('Blocks Object Store Stats (Ingester)', 'cortex_ingester'),
    )
    .addRowIf(
      std.setMember('tsdb', $._config.storage_engine),
      $.objectStorePanels2('', 'cortex_ingester'),
    ),
}
