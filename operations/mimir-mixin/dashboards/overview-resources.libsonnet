local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-overview-resources.json';

(import 'dashboard-utils.libsonnet') +
(import 'dashboard-queries.libsonnet') {
  [filename]:
    ($.dashboard('Overview resources') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates(false)

    .addRowIf(
      $._config.gateway_enabled,
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
      $.row('Writes')
      .addPanel(
        $.panel('CPU') +
        $.queryPanel($.resourceUtilizationQuery('cpu', $._config.container_names.write), '{{%s}}' % $._config.per_instance_label),
      )
      .addPanel(
        $.panel('Memory (workingset)') +
        $.queryPanel($.resourceUtilizationQuery('memory_working', $._config.container_names.write), '{{%s}}' % $._config.per_instance_label) +
        { yaxes: $.yaxes('bytes') },
      )
      .addPanel(
        $.goHeapInUsePanel('Memory (go heap inuse)', $._config.job_names.write),
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.containerDiskWritesPanel('Disk writes', $._config.container_names.write)
      )
      .addPanel(
        $.containerDiskReadsPanel('Disk reads', $._config.container_names.write)
      )
      .addPanel(
        $.containerDiskSpaceUtilization('Disk space utilization', $._config.container_names.write),
      )
    )

    .addRow(
      $.row('Reads')
      .addPanel(
        $.panel('CPU') +
        $.queryPanel($.resourceUtilizationQuery('cpu', $._config.container_names.read), '{{%s}}' % $._config.per_instance_label),
      )
      .addPanel(
        $.panel('Memory (workingset)') +
        $.queryPanel($.resourceUtilizationQuery('memory_working', $._config.container_names.read), '{{%s}}' % $._config.per_instance_label) +
        { yaxes: $.yaxes('bytes') },
      )
      .addPanel(
        $.goHeapInUsePanel('Memory (go heap inuse)', $._config.job_names.read),
      )
    )

    .addRow(
      $.row('Backend')
      .addPanel(
        $.panel('CPU') +
        $.queryPanel($.resourceUtilizationQuery('cpu', $._config.container_names.backend), '{{%s}}' % $._config.per_instance_label),
      )
      .addPanel(
        $.panel('Memory (workingset)') +
        $.queryPanel($.resourceUtilizationQuery('memory_working', $._config.container_names.backend), '{{%s}}' % $._config.per_instance_label) +
        { yaxes: $.yaxes('bytes') },
      )
      .addPanel(
        $.goHeapInUsePanel('Memory (go heap inuse)', $._config.job_names.backend),
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.containerDiskWritesPanel('Disk writes', $._config.container_names.backend)
      )
      .addPanel(
        $.containerDiskReadsPanel('Disk reads', $._config.container_names.backend)
      )
      .addPanel(
        $.containerDiskSpaceUtilization('Disk space utilization', $._config.container_names.backend),
      )
    ),
}
