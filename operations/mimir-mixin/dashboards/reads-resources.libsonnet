local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-reads-resources.json';

(import 'dashboard-utils.libsonnet') {
  [filename]:
    ($.dashboard('Reads resources') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates(false)
    .addRow(
      $.row('Summary')
      .addPanel(
        $.panel('CPU') +
        $.queryPanel($.resourceUtilizationQuery('cpu', $._config.instance_names.read, $._config.container_names.read), '{{%s}}' % $._config.per_instance_label) +
        $.stack,
      )
      .addPanel(
        $.panel('Memory (workingset)') +
        $.queryPanel($.resourceUtilizationQuery('memory_working', $._config.instance_names.read, $._config.container_names.read), '{{%s}}' % $._config.per_instance_label) +
        $.stack +
        { yaxes: $.yaxes('bytes') },
      )
      .addPanel(
        $.containerGoHeapInUsePanel($._config.instance_names.read, $._config.container_names.read) +
        $.stack,
      )
    )
    .addRowIf(
      $._config.gateway_enabled,
      $.row('Gateway')
      .addPanel(
        $.containerCPUUsagePanel($._config.instance_names.gateway, $._config.container_names.gateway),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel($._config.instance_names.gateway, $._config.container_names.gateway),
      )
      .addPanel(
        $.containerGoHeapInUsePanel($._config.instance_names.gateway, $._config.container_names.gateway),
      )
    )
    .addRow(
      $.row('Query-frontend')
      .addPanel(
        $.containerCPUUsagePanel($._config.instance_names.query_frontend, $._config.container_names.query_frontend),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel($._config.instance_names.query_frontend, $._config.container_names.query_frontend),
      )
      .addPanel(
        $.containerGoHeapInUsePanel($._config.instance_names.query_frontend, $._config.container_names.query_frontend),
      )
    )
    .addRow(
      $.row('Query-scheduler')
      .addPanel(
        $.containerCPUUsagePanel($._config.instance_names.query_scheduler, $._config.container_names.query_scheduler),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel($._config.instance_names.query_scheduler, $._config.container_names.query_scheduler),
      )
      .addPanel(
        $.containerGoHeapInUsePanel($._config.instance_names.query_scheduler, $._config.container_names.query_scheduler),
      )
    )
    .addRow(
      $.row('Querier')
      .addPanel(
        $.containerCPUUsagePanel($._config.instance_names.querier, $._config.container_names.querier),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel($._config.instance_names.querier, $._config.container_names.querier),
      )
      .addPanel(
        $.containerGoHeapInUsePanel($._config.instance_names.querier, $._config.container_names.querier),
      )
    )
    .addRow(
      $.row('Ingester')
      .addPanel(
        $.containerCPUUsagePanel($._config.instance_names.ingester, $._config.container_names.ingester),
      )
      .addPanel(
        $.containerGoHeapInUsePanel($._config.instance_names.ingester, $._config.container_names.ingester),
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.containerMemoryRSSPanel($._config.instance_names.ingester, $._config.container_names.ingester),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel($._config.instance_names.ingester, $._config.container_names.ingester),
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
        $.containerCPUUsagePanel($._config.instance_names.ruler, $._config.container_names.ruler),
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.containerMemoryWorkingSetPanel($._config.instance_names.ruler, $._config.container_names.ruler),
      )
      .addPanel(
        $.containerGoHeapInUsePanel($._config.instance_names.ruler, $._config.container_names.ruler),
      )
    )
    .addRow(
      $.row('Store-gateway')
      .addPanel(
        $.containerCPUUsagePanel($._config.instance_names.store_gateway, $._config.container_names.store_gateway),
      )
      .addPanel(
        $.containerGoHeapInUsePanel($._config.instance_names.store_gateway, $._config.container_names.store_gateway),
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.containerMemoryRSSPanel($._config.instance_names.store_gateway, $._config.container_names.store_gateway),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel($._config.instance_names.store_gateway, $._config.container_names.store_gateway),
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.containerDiskWritesPanel($._config.instance_names.store_gateway, $._config.container_names.store_gateway),
      )
      .addPanel(
        $.containerDiskReadsPanel($._config.instance_names.store_gateway, $._config.container_names.store_gateway),
      )
      .addPanel(
        $.containerDiskSpaceUtilization($._config.instance_names.store_gateway, $._config.container_names.store_gateway),
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
