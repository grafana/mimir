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
        $.containerCPUUsagePanel($._config.job_names.gateway, $._config.job_names.gateway),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel($._config.job_names.gateway, $._config.job_names.gateway),
      )
      .addPanel(
        $.goHeapInUsePanel($._config.job_names.gateway),
      )
    )
    .addRow(
      $.row('Alertmanager')
      .addPanel(
        $.containerCPUUsagePanel('alertmanager', 'alertmanager'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel('alertmanager', 'alertmanager'),
      )
      .addPanel(
        $.goHeapInUsePanel('alertmanager'),
      )
    )
    .addRowIf(
      $._config.alertmanager_im_enabled,
      $.row('Instance mapper')
      .addPanel(
        $.containerCPUUsagePanel('alertmanager-im', 'alertmanager-im'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel('alertmanager-im', 'alertmanager-im'),
      )
      .addPanel(
        $.goHeapInUsePanel('alertmanager-im'),
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
        $.containerDiskWritesPanel('Writes', 'alertmanager', 'alertmanager'),
      )
      .addPanel(
        $.containerDiskReadsPanel('Reads', 'alertmanager', 'alertmanager'),
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.containerDiskSpaceUtilization('Disk space utilization', 'alertmanager', 'alertmanager'),
      )
    ),
}
