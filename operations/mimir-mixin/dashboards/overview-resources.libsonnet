local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-overview-resources.json';

(import 'dashboard-utils.libsonnet') +
(import 'dashboard-queries.libsonnet') {
  [filename]:
    assert std.md5(filename) == 'a9b92d3c4d1af325d872a9e9a7083d71' : 'UID of the dashboard has changed, please update references to dashboard.';
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
        $.timeseriesPanel('CPU') +
        $.queryPanel($.resourceUtilizationQuery('cpu', $._config.instance_names.write, $._config.container_names.write), '{{%s}}' % $._config.per_instance_label),
      )
      .addPanel(
        $.timeseriesPanel('Memory (workingset)') +
        $.queryPanel($.resourceUtilizationQuery('memory_working', $._config.instance_names.write, $._config.container_names.write), '{{%s}}' % $._config.per_instance_label) +
        { fieldConfig+: { defaults+: { unit: 'bytes' } } },
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
        $.timeseriesPanel('CPU') +
        $.queryPanel($.resourceUtilizationQuery('cpu', $._config.instance_names.read, $._config.container_names.read), '{{%s}}' % $._config.per_instance_label),
      )
      .addPanel(
        $.timeseriesPanel('Memory (workingset)') +
        $.queryPanel($.resourceUtilizationQuery('memory_working', $._config.instance_names.read, $._config.container_names.read), '{{%s}}' % $._config.per_instance_label) +
        { fieldConfig+: { defaults+: { unit: 'bytes' } } },
      )
      .addPanel(
        $.containerGoHeapInUsePanelByComponent('read'),
      )
    )

    .addRow(
      $.row('Backend')
      .addPanel(
        $.timeseriesPanel('CPU') +
        $.queryPanel($.resourceUtilizationQuery('cpu', $._config.instance_names.backend, $._config.container_names.backend), '{{%s}}' % $._config.per_instance_label),
      )
      .addPanel(
        $.timeseriesPanel('Memory (workingset)') +
        $.queryPanel($.resourceUtilizationQuery('memory_working', $._config.instance_names.backend, $._config.container_names.backend), '{{%s}}' % $._config.per_instance_label) +
        { fieldConfig+: { defaults+: { unit: 'bytes' } } },
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
