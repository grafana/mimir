local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-remote-ruler-reads-resources.json';

(import 'dashboard-utils.libsonnet') {
  [filename]:
    ($.dashboard('Remote ruler reads resources') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates(false)
    .addRow(
      $.row('Query-frontend (dedicated to ruler)')
      .addPanel(
        $.containerCPUUsagePanel($._config.instance_names.ruler_query_frontend, $._config.container_names.ruler_query_frontend),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel($._config.instance_names.ruler_query_frontend, $._config.container_names.ruler_query_frontend),
      )
      .addPanel(
        $.containerGoHeapInUsePanel($._config.instance_names.ruler_query_frontend, $._config.container_names.ruler_query_frontend),
      )
    )
    .addRow(
      $.row('Query-scheduler (dedicated to ruler)')
      .addPanel(
        $.containerCPUUsagePanel($._config.instance_names.ruler_query_scheduler, $._config.container_names.ruler_query_scheduler),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel($._config.instance_names.ruler_query_scheduler, $._config.container_names.ruler_query_scheduler),
      )
      .addPanel(
        $.containerGoHeapInUsePanel($._config.instance_names.ruler_query_scheduler, $._config.container_names.ruler_query_scheduler),
      )
    )
    .addRow(
      $.row('Querier (dedicated to ruler)')
      .addPanel(
        $.containerCPUUsagePanel($._config.instance_names.ruler_querier, $._config.container_names.ruler_querier),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel($._config.instance_names.ruler_querier, $._config.container_names.ruler_querier),
      )
      .addPanel(
        $.containerGoHeapInUsePanel($._config.instance_names.ruler_querier, $._config.container_names.ruler_querier),
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
