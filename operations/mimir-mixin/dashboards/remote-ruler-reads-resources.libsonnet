local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-remote-ruler-reads-resources.json';

(import 'dashboard-utils.libsonnet') {
  [filename]:
    ($.dashboard('Remote ruler reads resources') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates(false)
    .addRow(
      $.row('Query-frontend (dedicated to ruler)')
      .addPanel(
        $.containerCPUUsagePanelByComponent('ruler_query_frontend'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanelByComponent('ruler_query_frontend'),
      )
      .addPanel(
        $.containerGoHeapInUsePanelByComponent('ruler_query_frontend'),
      )
    )
    .addRow(
      $.row('Query-scheduler (dedicated to ruler)')
      .addPanel(
        $.containerCPUUsagePanelByComponent('ruler_query_scheduler'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanelByComponent('ruler_query_scheduler'),
      )
      .addPanel(
        $.containerGoHeapInUsePanelByComponent('ruler_query_scheduler'),
      )
    )
    .addRow(
      $.row('Querier (dedicated to ruler)')
      .addPanel(
        $.containerCPUUsagePanelByComponent('ruler_querier'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanelByComponent('ruler_querier'),
      )
      .addPanel(
        $.containerGoHeapInUsePanelByComponent('ruler_querier'),
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
