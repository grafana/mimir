local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-alertmanager-resources.json';

(import 'dashboard-utils.libsonnet') {
  [filename]:
    assert std.md5(filename) == 'a6883fb22799ac74479c7db872451092' : 'UID of the dashboard has changed, please update references to dashboard.';
    ($.dashboard('Alertmanager resources') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates(false)
    .addRowIf(
      $._config.gateway_enabled,
      $.row('Gateway')
      .addPanel(
        $.containerCPUUsagePanelByComponent('gateway'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanelByComponent('gateway'),
      )
      .addPanel(
        $.containerGoHeapInUsePanelByComponent('gateway'),
      )
    )
    .addRow(
      $.row('Alertmanager')
      .addPanel(
        $.containerCPUUsagePanelByComponent('alertmanager'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanelByComponent('alertmanager'),
      )
      .addPanel(
        $.containerGoHeapInUsePanelByComponent('alertmanager'),
      )
    )
    .addRowIf(
      $._config.alertmanager_im_enabled,
      $.row('Instance mapper')
      .addPanel(
        $.containerCPUUsagePanelByComponent('alertmanager_im'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanelByComponent('alertmanager_im'),
      )
      .addPanel(
        $.containerGoHeapInUsePanelByComponent('alertmanager_im'),
      )
    )
    .addRow(
      $.row('Network')
      .addPanel(
        $.containerNetworkReceiveBytesPanelByComponent('alertmanager'),
      )
      .addPanel(
        $.containerNetworkTransmitBytesPanelByComponent('alertmanager'),
      )
    )
    .addRow(
      $.row('Disk')
      .addPanel(
        $.containerDiskWritesPanelByComponent('alertmanager'),
      )
      .addPanel(
        $.containerDiskReadsPanelByComponent('alertmanager'),
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.containerDiskSpaceUtilizationPanelByComponent('alertmanager'),
      )
    ),
}
