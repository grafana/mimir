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
        $.containerCPUUsagePanel($._config.job_names.gateway, $._config.job_names.gateway),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel($._config.job_names.gateway, $._config.job_names.gateway),
      )
      .addPanel(
        $.goHeapInUsePanel($._config.job_names.gateway),
      )
    )
    .addRow(
      $.row('Query-frontend')
      .addPanel(
        $.containerCPUUsagePanel('query-frontend', 'query-frontend'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel('query-frontend', 'query-frontend'),
      )
      .addPanel(
        $.goHeapInUsePanel($._config.job_names.query_frontend),
      )
    )
    .addRow(
      $.row('Query-scheduler')
      .addPanel(
        $.containerCPUUsagePanel('query-scheduler', 'query-scheduler'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel('query-scheduler', 'query-scheduler'),
      )
      .addPanel(
        $.goHeapInUsePanel($._config.job_names.query_scheduler),
      )
    )
    .addRow(
      $.row('Querier')
      .addPanel(
        $.containerCPUUsagePanel('querier', 'querier'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel('querier', 'querier'),
      )
      .addPanel(
        $.goHeapInUsePanel($._config.job_names.querier),
      )
    )
    .addRow(
      $.row('Ingester')
      .addPanel(
        $.containerCPUUsagePanel('ingester', 'ingester'),
      )
      .addPanel(
        $.goHeapInUsePanel($._config.job_names.ingester),
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.containerMemoryRSSPanel('ingester', 'ingester'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel('ingester', 'ingester'),
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
        $.containerCPUUsagePanel('ruler', 'ruler'),
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.containerMemoryWorkingSetPanel('ruler', 'ruler'),
      )
      .addPanel(
        $.goHeapInUsePanel($._config.job_names.ruler),
      )
    )
    .addRow(
      $.row('Store-gateway')
      .addPanel(
        $.containerCPUUsagePanel('store-gateway', 'store-gateway'),
      )
      .addPanel(
        $.goHeapInUsePanel($._config.job_names.store_gateway),
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.containerMemoryRSSPanel('store-gateway', 'store-gateway'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel('store-gateway', 'store-gateway'),
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.containerDiskWritesPanel('Disk writes', 'store-gateway', 'store-gateway'),
      )
      .addPanel(
        $.containerDiskReadsPanel('Disk reads', 'store-gateway', 'store-gateway'),
      )
      .addPanel(
        $.containerDiskSpaceUtilization('Disk space utilization', 'store-gateway', 'store-gateway'),
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
