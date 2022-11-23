local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-remote-ruler-reads-resources.json';

(import 'dashboard-utils.libsonnet') {
  [filename]:
    ($.dashboard('Remote ruler reads resources') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates(false)
    .addRow(
      $.row('Query-frontend (dedicated to ruler)')
      .addPanel(
        $.containerCPUUsagePanel('ruler-query-frontend', 'ruler-query-frontend'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel('ruler-query-frontend', 'ruler-query-frontend'),
      )
      .addPanel(
        $.goHeapInUsePanel($._config.job_names.ruler_query_frontend),
      )
    )
    .addRow(
      $.row('Query-scheduler (dedicated to ruler)')
      .addPanel(
        $.containerCPUUsagePanel('ruler-query-scheduler', 'ruler-query-scheduler'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel('ruler-query-scheduler', 'ruler-query-scheduler'),
      )
      .addPanel(
        $.goHeapInUsePanel($._config.job_names.ruler_query_scheduler),
      )
    )
    .addRow(
      $.row('Querier (dedicated to ruler)')
      .addPanel(
        $.containerCPUUsagePanel('ruler-querier', 'ruler-querier'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel('ruler-querier', 'ruler-querier'),
      )
      .addPanel(
        $.goHeapInUsePanel($._config.job_names.ruler_querier),
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
