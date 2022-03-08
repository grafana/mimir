local utils = import 'mixin-utils/utils.libsonnet';

(import 'dashboard-utils.libsonnet') {
  'mimir-compactor-resources.json':
    ($.dashboard('Compactor resources') + { uid: 'df9added6f1f4332f95848cca48ebd99' })
    .addClusterSelectorTemplates()
    .addRow(
      $.row('CPU and memory')
      .addPanel(
        $.containerCPUUsagePanel('CPU', 'compactor'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel('Memory (workingset)', 'compactor'),
      )
      .addPanel(
        $.goHeapInUsePanel('Memory (go heap inuse)', $._config.job_names.compactor),
      )
    )
    .addRow(
      $.row('Network')
      .addPanel(
        $.containerNetworkReceiveBytesPanel($._config.instance_names.compactor),
      )
      .addPanel(
        $.containerNetworkTransmitBytesPanel($._config.instance_names.compactor),
      )
    )
    .addRow(
      $.row('Disk')
      .addPanel(
        $.containerDiskWritesPanel('Disk writes', 'compactor'),
      )
      .addPanel(
        $.containerDiskReadsPanel('Disk reads', 'compactor'),
      )
      .addPanel(
        $.containerDiskSpaceUtilization('Disk space utilization', 'compactor'),
      )
    ) + {
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
