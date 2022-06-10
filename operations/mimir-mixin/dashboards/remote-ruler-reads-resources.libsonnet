local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-remote-ruler-reads-resources.json';

(import 'dashboard-utils.libsonnet') {
  [filename]:
    ($.dashboard('Remote ruler reads resources') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates(false)
    .addRow(
      $.row('Query-frontend (dedicated to ruler)')
      .addPanel(
        $.containerCPUUsagePanel('CPU', 'ruler-query-frontend'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel('Memory (workingset)', 'ruler-query-frontend'),
      )
      .addPanel(
        $.goHeapInUsePanel('Memory (go heap inuse)', $._config.job_names.ruler_query_frontend),
      )
    )
    .addRow(
      $.row('Query-scheduler (dedicated to ruler)')
      .addPanel(
        $.containerCPUUsagePanel('CPU', 'ruler-query-scheduler'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel('Memory (workingset)', 'ruler-query-scheduler'),
      )
      .addPanel(
        $.goHeapInUsePanel('Memory (go heap inuse)', $._config.job_names.ruler_query_scheduler),
      )
    )
    .addRow(
      $.row('Querier (dedicated to ruler)')
      .addPanel(
        $.containerCPUUsagePanel('CPU', 'ruler-querier'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel('Memory (workingset)', 'ruler-querier'),
      )
      .addPanel(
        $.goHeapInUsePanel('Memory (go heap inuse)', $._config.job_names.ruler_querier),
      )
    ) + {
      templating+: {
        list: [
          // Do not allow to include all clusters/namespaces otherwise this dashboard
          // risks to explode because it shows resources per pod.
          l + (if (l.name == 'cluster' || l.name == 'namespace') then { includeAll: false } else {})
          for l in super.list
        ],
      },
    },
}
