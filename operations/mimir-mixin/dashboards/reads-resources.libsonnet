local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-reads-resources.json';

(import 'dashboard-utils.libsonnet') {
  [filename]:
    assert std.md5(filename) == 'cc86fd5aa9301c6528986572ad974db9' : 'UID of the dashboard has changed, please update references to dashboard.';
    ($.dashboard('Reads resources') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates(false)
    .addRow(
      $.row('Summary')
      .addPanel(
        $.timeseriesPanel('CPU') +
        $.queryPanel($.resourceUtilizationQuery('cpu', $._config.instance_names.read, $._config.container_names.read), '{{%s}}' % $._config.per_instance_label) +
        $.showAllTooltip +
        $.stack,
      )
      .addPanel(
        $.timeseriesPanel('Memory (workingset)') +
        $.queryPanel($.resourceUtilizationQuery('memory_working', $._config.instance_names.read, $._config.container_names.read), '{{%s}}' % $._config.per_instance_label) +
        $.stack +
        $.showAllTooltip +
        { fieldConfig+: { defaults+: { unit: 'bytes' } } },
      )
      .addPanel(
        $.containerGoHeapInUsePanelByComponent('read') +
        $.stack,
      )
      .addPanel(
        $.containerEphemeralStoragePanelByComponent('read') +
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
      .addPanel(
        $.containerEphemeralStoragePanelByComponent('gateway'),
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
      .addPanel(
        $.containerEphemeralStoragePanelByComponent('query_frontend'),
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
      .addPanel(
        $.containerEphemeralStoragePanelByComponent('query_scheduler'),
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
      .addPanel(
        $.containerEphemeralStoragePanelByComponent('querier'),
      )
    )
    .addRow(
      $.row('Ingester')
      .addPanel(
        $.containerCPUUsagePanelByComponent('ingester'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanelByComponent('ingester'),
      )
      .addPanel(
        $.containerGoHeapInUsePanelByComponent('ingester'),
      )
      .addPanel(
        $.containerEphemeralStoragePanelByComponent('ingester'),
      )
    )
    .addRow(
      $.row('Store-gateway')
      .addPanel(
        $.containerCPUUsagePanelByComponent('store_gateway'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanelByComponent('store_gateway'),
      )
      .addPanel(
        $.containerGoHeapInUsePanelByComponent('store_gateway'),
      )
      .addPanel(
        $.containerEphemeralStoragePanelByComponent('store_gateway'),
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
    )
    .addRow(
      $.row('Ruler')
      .addPanel(
        $.containerCPUUsagePanelByComponent('ruler'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanelByComponent('ruler'),
      )
      .addPanel(
        $.containerGoHeapInUsePanelByComponent('ruler'),
      )
      .addPanel(
        $.containerEphemeralStoragePanelByComponent('ruler'),
      )
      .addPanel(
        $.timeseriesPanel('Rules') +
        $.queryPanel(
          'sum by(%s) (cortex_prometheus_rule_group_rules{%s})' % [$._config.per_instance_label, $.jobMatcher($._config.job_names.ruler)],
          '{{%s}}' % $._config.per_instance_label
        ) +
        // Use a stack to quickly see all the rules loaded across all ruler pods,
        // and don't get misled when the per-pod rules decrease due to a ruler scale out (more replicas added).
        $.stack +
        { fieldConfig+: { defaults+: { custom+: { fillOpacity: 20, lineWidth: 1 } } } },
      )
      .splitIntoLines([4, 1])  // Puts "rules" panel below the rest of the resources
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
