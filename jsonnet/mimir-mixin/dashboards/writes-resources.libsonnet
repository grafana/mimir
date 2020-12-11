local utils = import 'mixin-utils/utils.libsonnet';

(import 'dashboard-utils.libsonnet') {
  'cortex-writes-resources.json':
    ($.dashboard('Cortex / Writes Resources') + { uid: 'c0464f0d8bd026f776c9006b0591bb0b' })
    .addClusterSelectorTemplates()
    .addRow(
      $.row('Gateway')
      .addPanel(
        $.containerCPUUsagePanel('CPU', 'cortex-gw'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel('Memory (workingset)', 'cortex-gw'),
      )
      .addPanel(
        $.goHeapInUsePanel('Memory (go heap inuse)', 'cortex-gw'),
      )
    )
    .addRow(
      $.row('Distributor')
      .addPanel(
        $.containerCPUUsagePanel('CPU', 'distributor'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel('Memory (workingset)', 'distributor'),
      )
      .addPanel(
        $.goHeapInUsePanel('Memory (go heap inuse)', 'distributor'),
      )
    )
    .addRow(
      $.row('Ingester')
      .addPanel(
        $.panel('In-memory series') +
        $.queryPanel('sum by(instance) (cortex_ingester_memory_series{%s})' % $.jobMatcher($._config.job_names.ingester), '{{instance}}'),
      )
      .addPanel(
        $.containerCPUUsagePanel('CPU', 'ingester'),
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.containerMemoryWorkingSetPanel('Memory (workingset)', 'ingester'),
      )
      .addPanel(
        $.goHeapInUsePanel('Memory (go heap inuse)', 'ingester'),
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.panel('Disk Writes') +
        $.queryPanel('sum by(instance, device) (rate(node_disk_written_bytes_total[$__rate_interval])) + %s' % $.filterNodeDiskContainer('ingester'), '{{pod}} - {{device}}') +
        $.stack +
        { yaxes: $.yaxes('Bps') },
      )
      .addPanel(
        $.panel('Disk Reads') +
        $.queryPanel('sum by(instance, device) (rate(node_disk_read_bytes_total[$__rate_interval])) + %s' % $.filterNodeDiskContainer('ingester'), '{{pod}} - {{device}}') +
        $.stack +
        { yaxes: $.yaxes('Bps') },
      )
      .addPanel(
        $.panel('Disk Space Utilization') +
        $.queryPanel('max by(persistentvolumeclaim) (kubelet_volume_stats_used_bytes{%s} / kubelet_volume_stats_capacity_bytes{%s}) and count by(persistentvolumeclaim) (kube_persistentvolumeclaim_labels{%s,label_name="ingester"})' % [$.namespaceMatcher(), $.namespaceMatcher(), $.namespaceMatcher()], '{{persistentvolumeclaim}}') +
        { yaxes: $.yaxes('percentunit') },
      )
    )
    + {
      templating+: {
        list: [
          // Do not allow to include all clusters/namespaces otherwise this dashboard
          // risks to explode because it shows resources per pod.
          l + (if (l.name == 'cluster' || l.name == 'namespace') then { includeAll: false } else {})
          for l in super.list
        ],
      },
    },
}
