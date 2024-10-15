local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-writes-resources.json';

(import 'dashboard-utils.libsonnet') {
  [filename]:
    assert std.md5(filename) == 'bc9160e50b52e89e0e49c840fea3d379' : 'UID of the dashboard has changed, please update references to dashboard.';
    ($.dashboard('Writes resources') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates(false)
    .addRow(
      $.row('Summary')
      .addPanel(
        $.timeseriesPanel('CPU') +
        $.queryPanel($.resourceUtilizationQuery('cpu', $._config.instance_names.write, $._config.container_names.write), '{{%s}}' % $._config.per_instance_label) +
        $.stack,
      )
      .addPanel(
        $.timeseriesPanel('Memory (workingset)') +
        $.queryPanel($.resourceUtilizationQuery('memory_working', $._config.instance_names.write, $._config.container_names.write), '{{%s}}' % $._config.per_instance_label) +
        $.stack +
        { fieldConfig+: { defaults+: { unit: 'bytes' } } },
      )
      .addPanel(
        $.containerGoHeapInUsePanelByComponent('write') +
        $.stack,
      )
    )
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
      $.row('Distributor')
      .addPanel(
        $.containerCPUUsagePanelByComponent('distributor'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanelByComponent('distributor'),
      )
      .addPanel(
        $.containerGoHeapInUsePanelByComponent('distributor'),
      )
    )
    .addRow(
      $.row('Ingester')
      .addPanel(
        $.timeseriesPanel('In-memory series') +
        $.queryPanel(
          'sum by(%s) (cortex_ingester_memory_series{%s})' % [$._config.per_instance_label, $.jobMatcher($._config.job_names.ingester)],
          '{{%s}}' % $._config.per_instance_label
        ) +
        $.showAllTooltip +
        {
          fieldConfig+: {
            defaults+: {
              custom+: {
                fillOpacity: 0,
              },
            },
          },
        },
      )
      .addPanel(
        $.containerCPUUsagePanelByComponent('ingester'),
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.containerMemoryRSSPanelByComponent('ingester'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanelByComponent('ingester'),
      )
      .addPanel(
        $.containerGoHeapInUsePanelByComponent('ingester'),
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.containerDiskWritesPanelByComponent('ingester')
      )
      .addPanel(
        $.containerDiskReadsPanelByComponent('ingester')
      )
      .addPanel(
        $.containerDiskSpaceUtilizationPanelByComponent('ingester'),
      )
    )
    + {
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
