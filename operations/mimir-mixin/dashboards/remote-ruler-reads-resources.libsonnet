local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-remote-ruler-reads-resources.json';

(import 'dashboard-utils.libsonnet') {
  [filename]:
    assert std.md5(filename) == '1940f6ef765a506a171faa2056c956c3' : 'UID of the dashboard has changed, please update references to dashboard.';
    ($.dashboard('Remote ruler reads resources') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates(false)
    .addRow(
      $.row('Ruler-query-frontend')
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
      $.row('Ruler-query-scheduler')
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
      $.row('Ruler-querier')
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
