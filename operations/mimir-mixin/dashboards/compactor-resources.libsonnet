local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-compactor-resources.json';

(import 'dashboard-utils.libsonnet') {
  // This dashboard focuses only on compactor. When Mimir is deployed in read-write mode, the compactor
  // runs as part of the backend, so we show the backend resources instead.
  local compactorInstanceName = $._config.instance_names.compactor_or_backend,
  local compactorContainerName = $._config.container_names.compactor_or_backend,

  [filename]:
    ($.dashboard('Compactor resources') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates()
    .addRow(
      $.row('CPU and memory')
      .addPanel(
        $.containerCPUUsagePanel(compactorInstanceName, compactorContainerName),
      )
      .addPanel(
        $.containerGoHeapInUsePanel(compactorInstanceName, compactorContainerName),
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.containerMemoryRSSPanel(compactorInstanceName, compactorContainerName),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel(compactorInstanceName, compactorContainerName),
      )
    )
    .addRow(
      $.row('Network')
      .addPanel(
        $.containerNetworkReceiveBytesPanel(compactorInstanceName),
      )
      .addPanel(
        $.containerNetworkTransmitBytesPanel(compactorInstanceName),
      )
    )
    .addRow(
      $.row('Disk')
      .addPanel(
        $.containerDiskWritesPanel(compactorInstanceName, compactorContainerName),
      )
      .addPanel(
        $.containerDiskReadsPanel(compactorInstanceName, compactorContainerName),
      )
      .addPanel(
        $.containerDiskSpaceUtilization(compactorInstanceName, compactorContainerName),
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
