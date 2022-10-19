local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-compactor-resources.json';

(import 'dashboard-utils.libsonnet') {
  [filename]:
    ($.dashboard('Compactor resources') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates()
    .addRow(
      $.row('CPU and memory')
      .addPanel(
        $.containerCPUUsagePanel('CPU', 'compactor'),
      )
      .addPanel(
        $.goHeapInUsePanel('Memory (go heap inuse)', $._config.job_names.compactor),
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.containerMemoryRSSPanel('Memory (RSS)', 'compactor'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel('Memory (workingset)', 'compactor'),
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
          // Do not allow to include all namespaces otherwise this dashboard
          // risks to explode because it shows resources per pod.
          l + (if (l.name == 'namespace') then { includeAll: false } else {})
          for l in super.list
        ],
      },
    },
}
