local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-reads-resources.json';

(import 'dashboard-utils.libsonnet') {
  [filename]:
    ($.dashboard('Reads resources') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates(false)
    .addRowIf(
      $._config.gateway_enabled,
      $.row('Gateway')
      .addPanel(
        $.containerCPUUsagePanel('CPU', $._config.job_names.gateway),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel('Memory (workingset)', $._config.job_names.gateway),
      )
      .addPanel(
        $.goHeapInUsePanel('Memory (go heap inuse)', $._config.job_names.gateway),
      )
    )
    .addRow(
      $.row('Query-frontend')
      .addPanel(
        $.containerCPUUsagePanel('CPU', 'query-frontend'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel('Memory (workingset)', 'query-frontend'),
      )
      .addPanel(
        $.goHeapInUsePanel('Memory (go heap inuse)', $._config.job_names.query_frontend),
      )
    )
    .addRow(
      $.row('Query-scheduler')
      .addPanel(
        $.containerCPUUsagePanel('CPU', 'query-scheduler'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel('Memory (workingset)', 'query-scheduler'),
      )
      .addPanel(
        $.goHeapInUsePanel('Memory (go heap inuse)', $._config.job_names.query_scheduler),
      )
    )
    .addRow(
      $.row('Querier')
      .addPanel(
        $.containerCPUUsagePanel('CPU', 'querier'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel('Memory (workingset)', 'querier'),
      )
      .addPanel(
        $.goHeapInUsePanel('Memory (go heap inuse)', $._config.job_names.querier),
      )
    )
    .addRow(
      $.row('Ingester')
      .addPanel(
        $.containerCPUUsagePanel('CPU', 'ingester'),
      )
      .addPanel(
        $.goHeapInUsePanel('Memory (go heap inuse)', $._config.job_names.ingester),
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.containerMemoryRSSPanel('Memory (RSS)', 'ingester'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel('Memory (workingset)', 'ingester'),
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
        $.containerCPUUsagePanel('CPU', 'ruler'),
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.containerMemoryWorkingSetPanel('Memory (workingset)', 'ruler'),
      )
      .addPanel(
        $.goHeapInUsePanel('Memory (go heap inuse)', $._config.job_names.ruler),
      )
    )
    .addRow(
      $.row('Store-gateway')
      .addPanel(
        $.containerCPUUsagePanel('CPU', 'store-gateway'),
      )
      .addPanel(
        $.goHeapInUsePanel('Memory (go heap inuse)', $._config.job_names.store_gateway),
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.containerMemoryRSSPanel('Memory (RSS)', 'store-gateway'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel('Memory (workingset)', 'store-gateway'),
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.containerDiskWritesPanel('Disk writes', 'store-gateway'),
      )
      .addPanel(
        $.containerDiskReadsPanel('Disk reads', 'store-gateway'),
      )
      .addPanel(
        $.containerDiskSpaceUtilization('Disk space utilization', 'store-gateway'),
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
