local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-reads-resources.json';

(import 'dashboard-utils.libsonnet') {
  [filename]:
    ($.dashboard('Reads resources') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates(false)
    .addRow(
      $.row('Summary')
      .addPanel(
        $.panel('CPU') +
        $.queryPanel($.resourceUtilizationQuery('cpu', $._config.instance_names.read, $._config.container_names.read), '{{%s}}' % $._config.per_instance_label) +
        $.stack,
      )
      .addPanel(
        $.panel('Memory (workingset)') +
        $.queryPanel($.resourceUtilizationQuery('memory_working', $._config.instance_names.read, $._config.container_names.read), '{{%s}}' % $._config.per_instance_label) +
        $.stack +
        { yaxes: $.yaxes('bytes') },
      )
      .addPanel(
        $.containerGoHeapInUsePanelByComponent('read') +
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
      $.row('Query-frontend')
      .addPanel(
        $.containerCPUUsagePanelByComponent('query_frontend'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanelByComponent('query_frontend'),
      )
      .addPanel(
        $.containerGoHeapInUsePanelByComponent('query_frontend'),
      )
    )
    .addRow(
      $.row('Query-scheduler')
      .addPanel(
        $.containerCPUUsagePanelByComponent('query_scheduler'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanelByComponent('query_scheduler'),
      )
      .addPanel(
        $.containerGoHeapInUsePanelByComponent('query_scheduler'),
      )
    )
    .addRow(
      $.row('Querier')
      .addPanel(
        $.containerCPUUsagePanelByComponent('querier'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanelByComponent('querier'),
      )
      .addPanel(
        $.containerGoHeapInUsePanelByComponent('querier'),
      )
    )
    .addRow(
      $.row('Ingester')
      .addPanel(
        $.containerCPUUsagePanelByComponent('ingester'),
      )
      .addPanel(
        $.containerGoHeapInUsePanelByComponent('ingester'),
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
    )
    .addRow(
      $.row('Ruler')
      .addPanel(
        $.panel('Rules') +
        $.queryPanel(
          'sum by(%s) (cortex_prometheus_rule_group_rules{%s})' % [$._config.per_instance_label, $.jobMatcher($._config.job_names.ruler)],
          '{{%s}}' % $._config.per_instance_label
        ),
      )
      .addPanel(
        $.containerCPUUsagePanelByComponent('ruler'),
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.containerMemoryWorkingSetPanelByComponent('ruler'),
      )
      .addPanel(
        $.containerGoHeapInUsePanelByComponent('ruler'),
      )
    )
    .addRow(
      $.row('Store-gateway')
      .addPanel(
        $.containerCPUUsagePanelByComponent('store_gateway'),
      )
      .addPanel(
        $.containerGoHeapInUsePanelByComponent('store_gateway'),
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.containerMemoryRSSPanelByComponent('store_gateway'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanelByComponent('store_gateway'),
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.containerDiskWritesPanelByComponent('store_gateway'),
      )
      .addPanel(
        $.containerDiskReadsPanelByComponent('store_gateway'),
      )
      .addPanel(
        $.containerDiskSpaceUtilizationPanelByComponent('store_gateway'),
      )
    ) + {
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
