local filename = 'mimir-reads-resources.json';

{
  local lib =
    (import 'dashboard-utils.libsonnet') +
    (import 'dashboard-queries.libsonnet') +
    { _config:: $._config + $._group_config },

  // Rows of the Reads resources dashboard, also shown in the Resources tab of the Reads dashboard.
  grafanaPanels+:: {
    readsResourcesContent:
      [
        lib.row('Summary')
        .addPanel(
          lib.timeseriesPanel('CPU') +
          lib.queryPanel(lib.resourceUtilizationQuery('cpu', lib._config.instance_names.read, lib._config.container_names.read), '{{%s}}' % lib._config.per_instance_label) +
          lib.showAllTooltip +
          lib.stack,
        )
        .addPanel(
          lib.timeseriesPanel('Memory (workingset)') +
          lib.queryPanel(lib.resourceUtilizationQuery('memory_working', lib._config.instance_names.read, lib._config.container_names.read), '{{%s}}' % lib._config.per_instance_label) +
          lib.stack +
          lib.showAllTooltip +
          { fieldConfig+: { defaults+: { unit: 'bytes' } } },
        )
        .addPanel(
          lib.containerGoHeapInUsePanelByComponent('read') +
          lib.stack,
        )
        .addPanel(
          lib.containerEphemeralStoragePanelByComponent('read') +
          lib.stack,
        ),
      ]
      + (if lib._config.gateway_enabled then [
           lib.row('Gateway')
           .addPanel(
             lib.containerCPUUsagePanelByComponent('gateway'),
           )
           .addPanel(
             lib.containerMemoryWorkingSetPanelByComponent('gateway'),
           )
           .addPanel(
             lib.containerGoHeapInUsePanelByComponent('gateway'),
           )
           .addPanel(
             lib.containerEphemeralStoragePanelByComponent('gateway'),
           ),
         ] else [])
      + [
        lib.row('Query-frontend')
        .addPanel(
          lib.containerCPUUsagePanelByComponent('query_frontend'),
        )
        .addPanel(
          lib.containerMemoryWorkingSetPanelByComponent('query_frontend'),
        )
        .addPanel(
          lib.containerGoHeapInUsePanelByComponent('query_frontend'),
        )
        .addPanel(
          lib.containerEphemeralStoragePanelByComponent('query_frontend'),
        ),
      ]
      + [
        lib.row('Query-scheduler')
        .addPanel(
          lib.containerCPUUsagePanelByComponent('query_scheduler'),
        )
        .addPanel(
          lib.containerMemoryWorkingSetPanelByComponent('query_scheduler'),
        )
        .addPanel(
          lib.containerGoHeapInUsePanelByComponent('query_scheduler'),
        )
        .addPanel(
          lib.containerEphemeralStoragePanelByComponent('query_scheduler'),
        ),
      ]
      + [
        lib.row('Querier')
        .addPanel(
          lib.containerCPUUsagePanelByComponent('querier'),
        )
        .addPanel(
          lib.containerMemoryWorkingSetPanelByComponent('querier'),
        )
        .addPanel(
          lib.containerGoHeapInUsePanelByComponent('querier'),
        )
        .addPanel(
          lib.containerEphemeralStoragePanelByComponent('querier'),
        ),
      ]
      + [
        lib.row('Ingester')
        .addPanel(
          lib.containerCPUUsagePanelByComponent('ingester'),
        )
        .addPanel(
          lib.containerMemoryWorkingSetPanelByComponent('ingester'),
        )
        .addPanel(
          lib.containerGoHeapInUsePanelByComponent('ingester'),
        )
        .addPanel(
          lib.containerEphemeralStoragePanelByComponent('ingester'),
        ),
      ]
      + [
        lib.row('Store-gateway')
        .addPanel(
          lib.containerCPUUsagePanelByComponent('store_gateway'),
        )
        .addPanel(
          lib.containerMemoryWorkingSetPanelByComponent('store_gateway'),
        )
        .addPanel(
          lib.containerGoHeapInUsePanelByComponent('store_gateway'),
        )
        .addPanel(
          lib.containerEphemeralStoragePanelByComponent('store_gateway'),
        ),
      ]
      + [
        lib.row('')
        .addPanel(
          lib.containerDiskWritesPanelByComponent('store_gateway'),
        )
        .addPanel(
          lib.containerDiskReadsPanelByComponent('store_gateway'),
        )
        .addPanel(
          lib.containerDiskSpaceUtilizationPanelByComponent('store_gateway'),
        ),
      ]
      + [
        lib.row('Ruler')
        .addPanel(
          lib.containerCPUUsagePanelByComponent('ruler'),
        )
        .addPanel(
          lib.containerMemoryWorkingSetPanelByComponent('ruler'),
        )
        .addPanel(
          lib.containerGoHeapInUsePanelByComponent('ruler'),
        )
        .addPanel(
          lib.containerEphemeralStoragePanelByComponent('ruler'),
        )
        .addPanel(
          lib.timeseriesPanel('Rules') +
          lib.queryPanel(
            'sum by(%s) (cortex_prometheus_rule_group_rules{%s})' % [lib._config.per_instance_label, lib.jobMatcher(lib._config.job_names.ruler)],
            '{{%s}}' % lib._config.per_instance_label
          ) +
          // Use a stack to quickly see all the rules loaded across all ruler pods,
          // and don't get misled when the per-pod rules decrease due to a ruler scale out (more replicas added).
          lib.stack +
          { fieldConfig+: { defaults+: { custom+: { fillOpacity: 20, lineWidth: 1 } } } },
        )
        .splitIntoLines([4, 1]),  // Puts "rules" panel below the rest of the resources
      ],
  },

  grafanaDashboards+: if !lib._config.resources_dashboards_enabled then {} else {
    [filename]:
      assert std.md5(filename) == 'cc86fd5aa9301c6528986572ad974db9' : 'UID of the dashboard has changed, please update references to dashboard.';
      (lib.dashboard('Reads resources') + { uid: std.md5(filename) })
      .addClusterSelectorTemplates(false)
      .addRows($.grafanaPanels.readsResourcesContent)
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
