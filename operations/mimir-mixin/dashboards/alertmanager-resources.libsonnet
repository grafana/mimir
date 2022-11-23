local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-alertmanager-resources.json';

(import 'dashboard-utils.libsonnet') {
  // This dashboard focuses only on compactor. When Mimir is deployed in read-write mode, the compactor
  // runs as part of the backend, so we show the backend resources instead.
  local alertmanagerInstanceName = $._config.instance_names.alertmanager_or_backend,
  local alertmanagerContainerName = $._config.container_names.alertmanager_or_backend,

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
        $.containerCPUUsagePanel(alertmanagerInstanceName, alertmanagerContainerName),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel(alertmanagerInstanceName, alertmanagerContainerName),
      )
      .addPanel(
        $.containerGoHeapInUsePanel(alertmanagerInstanceName, alertmanagerContainerName),
      )
    )
    .addRowIf(
      $._config.alertmanager_im_enabled,
      $.row('Instance mapper')
      .addPanel(
        $.containerCPUUsagePanel($._config.instance_names.alertmanager_im, $._config.container_name.alertmanager_im),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel($._config.instance_names.alertmanager_im, $._config.container_name.alertmanager_im),
      )
      .addPanel(
        $.containerGoHeapInUsePanel($._config.instance_names.alertmanager_im, $._config.container_name.alertmanager_im),
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
        $.containerDiskWritesPanel(alertmanagerInstanceName, alertmanagerContainerName),
      )
      .addPanel(
        $.containerDiskReadsPanel(alertmanagerInstanceName, alertmanagerContainerName),
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.containerDiskSpaceUtilization(alertmanagerInstanceName, alertmanagerContainerName),
      )
    ),
}
