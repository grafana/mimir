local utils = import 'mixin-utils/utils.libsonnet';

(import 'dashboard-utils.libsonnet') {
  'mimir-alertmanager-resources.json':
    ($.dashboard('Alertmanager resources') + { uid: '68b66aed90ccab448009089544a8d6c6' })
    .addClusterSelectorTemplates(false)
    .addRow(
      $.row('Gateway')
      .addPanel(
        $.containerCPUUsagePanel('CPU', $._config.job_names.gateway),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel('Memory (workingset)', $._config.job_names.gateway),
      )
      .addPanel(
        $.goHeapInUsePanel('Memory (go heap inuse)', $._config.job_names.gateway),
      )
    )
    .addRow(
      $.row('Alertmanager')
      .addPanel(
        $.containerCPUUsagePanel('CPU', 'alertmanager'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel('Memory (workingset)', 'alertmanager'),
      )
      .addPanel(
        $.goHeapInUsePanel('Memory (go heap inuse)', 'alertmanager'),
      )
    )
    .addRow(
      $.row('Instance mapper')
      .addPanel(
        $.containerCPUUsagePanel('CPU', 'alertmanager-im'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel('Memory (workingset)', 'alertmanager-im'),
      )
      .addPanel(
        $.goHeapInUsePanel('Memory (go heap inuse)', 'alertmanager-im'),
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
        $.containerDiskWritesPanel('Writes', 'alertmanager'),
      )
      .addPanel(
        $.containerDiskReadsPanel('Reads', 'alertmanager'),
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.containerDiskSpaceUtilization('Disk space utilization', 'alertmanager'),
      )
    ),
}
