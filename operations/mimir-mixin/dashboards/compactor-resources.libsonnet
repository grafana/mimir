local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-compactor-resources.json';

(import 'dashboard-utils.libsonnet') {
  [filename]:
    ($.dashboard('Compactor resources') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates()
    .addRow(
      $.row('CPU and memory')
      .addPanel(
        $.containerCPUUsagePanelByComponent('compactor'),
      )
      .addPanel(
        $.containerGoHeapInUsePanelByComponent('compactor'),
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.containerMemoryRSSPanelByComponent('compactor'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanelByComponent('compactor'),
      )
    )
    .addRow(
      $.row('Network')
      .addPanel(
        $.containerNetworkReceiveBytesPanelByComponent('compactor'),
      )
      .addPanel(
        $.containerNetworkTransmitBytesPanelByComponent('compactor'),
      )
    )
    .addRow(
      $.row('Disk')
      .addPanel(
        $.containerDiskWritesPanelByComponent('compactor'),
      )
      .addPanel(
        $.containerDiskReadsPanelByComponent('compactor'),
      )
      .addPanel(
        $.containerDiskSpaceUtilizationPanelByComponent('compactor'),
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
