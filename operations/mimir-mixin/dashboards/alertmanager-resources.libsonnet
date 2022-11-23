local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-alertmanager-resources.json';

(import 'dashboard-utils.libsonnet') {
  [filename]:
    ($.dashboard('Alertmanager resources') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates(false)
    .addRowIf(
      $._config.gateway_enabled,
      $.row('Gateway')
      .addPanel(
        $.containerCPUUsagePanel($._config.instance_names.gateway, $._config.container_names.gateway),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel($._config.instance_names.gateway, $._config.container_names.gateway),
      )
      .addPanel(
        $.containerGoHeapInUsePanel($._config.instance_names.gateway, $._config.container_names.gateway),
      )
    )
    .addRow(
      $.row('Alertmanager')
      .addPanel(
        $.containerCPUUsagePanel($._config.instance_names.alertmanager, $._config.container_names.alertmanager),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel($._config.instance_names.alertmanager, $._config.container_names.alertmanager),
      )
      .addPanel(
        $.containerGoHeapInUsePanel($._config.instance_names.alertmanager, $._config.container_names.alertmanager),
      )
    )
    .addRowIf(
      $._config.alertmanager_im_enabled,
      $.row('Instance mapper')
      .addPanel(
        $.containerCPUUsagePanel($._config.instance_names.alertmanager_im, $._config.container_names.alertmanager_im),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel($._config.instance_names.alertmanager_im, $._config.container_names.alertmanager_im),
      )
      .addPanel(
        $.containerGoHeapInUsePanel($._config.instance_names.alertmanager_im, $._config.container_names.alertmanager_im),
      )
    )
    .addRow(
      $.row('Network')
      .addPanel(
        $.containerNetworkReceiveBytesPanel($._config.instance_names.alertmanager),
      )
      .addPanel(
        $.containerNetworkTransmitBytesPanel($._config.instance_names.alertmanager),
      )
    )
    .addRow(
      $.row('Disk')
      .addPanel(
        $.containerDiskWritesPanel($._config.instance_names.alertmanager, $._config.container_names.alertmanager),
      )
      .addPanel(
        $.containerDiskReadsPanel($._config.instance_names.alertmanager, $._config.container_names.alertmanager),
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.containerDiskSpaceUtilization($._config.instance_names.alertmanager, $._config.container_names.alertmanager),
      )
    ),
}
