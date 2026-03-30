local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-compactor-resources.json';

(import 'dashboard-utils.libsonnet') {
  [filename]:
    assert std.md5(filename) == '09a5c49e9cdb2f2b24c6d184574a07fd' : 'UID of the dashboard has changed, please update references to dashboard.';
    local compactor_scheduler_enabled = $._config.compactor_scheduler_enabled;
    ($.dashboard('Compactor resources') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates()
    .addRow(
      $.row(if compactor_scheduler_enabled then 'CPU and memory (compactor)' else 'CPU and memory')
      .addPanel(
        $.containerCPUUsagePanelByComponent('compactor'),
      )
      .addPanel(
        $.containerGoHeapInUsePanelByComponent('compactor'),
      )
      .addPanel(
        $.containerMemoryRSSPanelByComponent('compactor'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanelByComponent('compactor'),
      )
      .splitIntoLines([2, 2])
    )
    .addRow(
      $.row(if compactor_scheduler_enabled then 'Network (compactor)' else 'Network')
      .addPanel(
        $.containerNetworkReceiveBytesPanelByComponent('compactor', if compactor_scheduler_enabled then 'compactor_scheduler' else ''),
      )
      .addPanel(
        $.containerNetworkTransmitBytesPanelByComponent('compactor', if compactor_scheduler_enabled then 'compactor_scheduler' else ''),
      )
    )
    .addRow(
      $.row(if compactor_scheduler_enabled then 'Disk (compactor)' else 'Disk')
      .addPanel(
        $.containerDiskWritesPanelByComponent('compactor'),
      )
      .addPanel(
        $.containerDiskReadsPanelByComponent('compactor'),
      )
      .addPanel(
        $.containerDiskSpaceUtilizationPanelByComponent('compactor', if compactor_scheduler_enabled then 'compactor_scheduler' else ''),
      )
    )
    .addRowIf(
      compactor_scheduler_enabled,
      ($.row('CPU and memory (scheduler)') + { collapse: true })
      .addPanel(
        $.containerCPUUsagePanelByComponent('compactor_scheduler'),
      )
      .addPanel(
        $.containerGoHeapInUsePanelByComponent('compactor_scheduler'),
      )
      .addPanel(
        $.containerMemoryRSSPanelByComponent('compactor_scheduler'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanelByComponent('compactor_scheduler'),
      )
      .splitIntoLines([2, 2])
    )
    .addRowIf(
      compactor_scheduler_enabled,
      ($.row('Network (scheduler)') + { collapse: true })
      .addPanel(
        $.containerNetworkReceiveBytesPanelByComponent('compactor_scheduler'),
      )
      .addPanel(
        $.containerNetworkTransmitBytesPanelByComponent('compactor_scheduler'),
      )
    )
    .addRowIf(
      compactor_scheduler_enabled,
      ($.row('Disk (scheduler)') + { collapse: true })
      .addPanel(
        $.containerDiskWritesPanelByComponent('compactor_scheduler'),
      )
      .addPanel(
        $.containerDiskReadsPanelByComponent('compactor_scheduler'),
      )
      .addPanel(
        $.containerDiskSpaceUtilizationPanelByComponent('compactor_scheduler'),
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
