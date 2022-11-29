local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-writes-resources.json';

(import 'dashboard-utils.libsonnet') {
  [filename]:
    ($.dashboard('Writes resources') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates(false)
    .addRow(
      $.row('Summary')
      .addPanel(
        $.panel('CPU') +
        $.queryPanel($.resourceUtilizationQuery('cpu', $._config.instance_names.write, $._config.container_names.write), '{{%s}}' % $._config.per_instance_label) +
        $.stack,
      )
      .addPanel(
        $.panel('Memory (workingset)') +
        $.queryPanel($.resourceUtilizationQuery('memory_working', $._config.instance_names.write, $._config.container_names.write), '{{%s}}' % $._config.per_instance_label) +
        $.stack +
        { yaxes: $.yaxes('bytes') },
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
        $.panel('In-memory series') +
        $.queryPanel(
          'sum by(%s) (cortex_ingester_memory_series{%s})' % [$._config.per_instance_label, $.jobMatcher($._config.job_names.ingester)],
          '{{%s}}' % $._config.per_instance_label
        ) +
        {
          tooltip: { sort: 2 },  // Sort descending.
          fill: 0,
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
