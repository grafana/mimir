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
      $.row('Writes')
      .addPanel(
        $.panel('CPU') +
        $.queryPanel($.resourceUtilizationQuery('cpu', $._config.instance_names.write, $._config.container_names.write), '{{%s}}' % $._config.per_instance_label),
      )
      .addPanel(
        $.panel('Memory (workingset)') +
        $.queryPanel($.resourceUtilizationQuery('memory_working', $._config.instance_names.write, $._config.container_names.write), '{{%s}}' % $._config.per_instance_label) +
        { yaxes: $.yaxes('bytes') },
      )
      .addPanel(
        $.containerGoHeapInUsePanelByComponent('write'),
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.containerDiskWritesPanelByComponent('write')
      )
      .addPanel(
        $.containerDiskReadsPanelByComponent('write')
      )
      .addPanel(
        $.containerDiskSpaceUtilizationPanelByComponent('write'),
      )
    )

    .addRow(
      $.row('Reads')
      .addPanel(
        $.panel('CPU') +
        $.queryPanel($.resourceUtilizationQuery('cpu', $._config.instance_names.read, $._config.container_names.read), '{{%s}}' % $._config.per_instance_label),
      )
      .addPanel(
        $.panel('Memory (workingset)') +
        $.queryPanel($.resourceUtilizationQuery('memory_working', $._config.instance_names.read, $._config.container_names.read), '{{%s}}' % $._config.per_instance_label) +
        { yaxes: $.yaxes('bytes') },
      )
      .addPanel(
        $.containerGoHeapInUsePanelByComponent('read'),
      )
    )

    .addRow(
      $.row('Backend')
      .addPanel(
        $.panel('CPU') +
        $.queryPanel($.resourceUtilizationQuery('cpu', $._config.instance_names.backend, $._config.container_names.backend), '{{%s}}' % $._config.per_instance_label),
      )
      .addPanel(
        $.panel('Memory (workingset)') +
        $.queryPanel($.resourceUtilizationQuery('memory_working', $._config.instance_names.backend, $._config.container_names.backend), '{{%s}}' % $._config.per_instance_label) +
        { yaxes: $.yaxes('bytes') },
      )
      .addPanel(
        $.containerGoHeapInUsePanelByComponent('backend'),
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.containerDiskWritesPanelByComponent('backend')
      )
      .addPanel(
        $.containerDiskReadsPanelByComponent('backend')
      )
      .addPanel(
        $.containerDiskSpaceUtilizationPanelByComponent('backend'),
      )
    ),
}
