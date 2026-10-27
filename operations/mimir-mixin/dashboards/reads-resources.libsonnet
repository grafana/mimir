local filename = 'mimir-reads-resources.json';

{
  local lib =
    (import 'dashboard-utils.libsonnet') +
    (import 'dashboard-queries.libsonnet') +
    { _config:: $._config + $._group_config },

  // Resource panels of a component: CPU, memory (workingset), Go heap and ephemeral storage.
  local componentRow(title, componentName) =
    lib.gridRow(title, [
      lib.containerCPUUsagePanelByComponent(componentName),
      lib.containerMemoryWorkingSetPanelByComponent(componentName),
      lib.containerGoHeapInUsePanelByComponent(componentName),
      lib.containerEphemeralStoragePanelByComponent(componentName),
    ]),

  // Rows of the Reads resources dashboard, also shown in the Resources tab of the Reads dashboard.
  grafanaPanels+:: {
    readsResourcesContent:
      [
        lib.gridRow('Summary', [
          lib.timeseriesPanel('CPU') +
          lib.queryPanel(lib.resourceUtilizationQuery('cpu', lib._config.instance_names.read, lib._config.container_names.read), '{{%s}}' % lib._config.per_instance_label) +
          lib.showAllTooltip +
          lib.stack,
          lib.timeseriesPanel('Memory (workingset)') +
          lib.queryPanel(lib.resourceUtilizationQuery('memory_working', lib._config.instance_names.read, lib._config.container_names.read), '{{%s}}' % lib._config.per_instance_label) +
          lib.stack +
          lib.showAllTooltip +
          { fieldConfig+: { defaults+: { unit: 'bytes' } } },
          lib.containerGoHeapInUsePanelByComponent('read') +
          lib.stack,
          lib.containerEphemeralStoragePanelByComponent('read') +
          lib.stack,
        ]),
      ]
      + (if lib._config.gateway_enabled then [componentRow('Gateway', 'gateway')] else [])
      + [
        componentRow('Query-frontend', 'query_frontend'),
        componentRow('Query-scheduler', 'query_scheduler'),
        componentRow('Querier', 'querier'),
        componentRow('Ingester', 'ingester'),
        componentRow('Store-gateway', 'store_gateway'),
        lib.gridRow('', [
          lib.containerDiskWritesPanelByComponent('store_gateway'),
          lib.containerDiskReadsPanelByComponent('store_gateway'),
          lib.containerDiskSpaceUtilizationPanelByComponent('store_gateway'),
        ]),
        lib.gridRow('Ruler', [
          lib.containerCPUUsagePanelByComponent('ruler'),
          lib.containerMemoryWorkingSetPanelByComponent('ruler'),
          lib.containerGoHeapInUsePanelByComponent('ruler'),
          lib.containerEphemeralStoragePanelByComponent('ruler'),
          lib.timeseriesPanel('Rules') +
          lib.queryPanel(
            'sum by(%s) (cortex_prometheus_rule_group_rules{%s})' % [lib._config.per_instance_label, lib.jobMatcher(lib._config.job_names.ruler)],
            '{{%s}}' % lib._config.per_instance_label
          ) +
          // Use a stack to quickly see all the rules loaded across all ruler pods,
          // and don't get misled when the per-pod rules decrease due to a ruler scale out (more replicas added).
          lib.stack +
          { fieldConfig+: { defaults+: { custom+: { fillOpacity: 20, lineWidth: 1 } } } },
        ], panelsPerLine=[4, 1]),  // Puts "rules" panel below the rest of the resources
      ],
  },

  grafanaDashboards+: if !lib._config.resources_dashboards_enabled then {} else {
    [filename]:
      assert std.md5(filename) == 'cc86fd5aa9301c6528986572ad974db9' : 'UID of the dashboard has changed, please update references to dashboard.';
      (lib.dashboard('Reads resources') + { uid: std.md5(filename) })
      .addClusterSelectorTemplates(false)
      .addGridPanels(lib.gridLayout($.grafanaPanels.readsResourcesContent))
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
  },
}
