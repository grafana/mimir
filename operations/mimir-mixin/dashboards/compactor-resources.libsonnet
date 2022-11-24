local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-compactor-resources.json';

(import 'dashboard-utils.libsonnet') {
  [filename]:
    ($.dashboard('Compactor resources') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates()
    .addRow(
      $.row('CPU and memory')
      .addPanel(
        $.containerCPUUsagePanel($._config.instance_names.compactor, $._config.container_names.compactor),
      )
      .addPanel(
        $.containerGoHeapInUsePanel($._config.instance_names.compactor, $._config.container_names.compactor),
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.containerMemoryRSSPanel($._config.instance_names.compactor, $._config.container_names.compactor),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel($._config.instance_names.compactor, $._config.container_names.compactor),
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
        $.containerDiskWritesPanel($._config.instance_names.compactor, $._config.container_names.compactor),
      )
      .addPanel(
        $.containerDiskReadsPanel($._config.instance_names.compactor, $._config.container_names.compactor),
      )
      .addPanel(
        $.containerDiskSpaceUtilization($._config.instance_names.compactor, $._config.container_names.compactor),
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
