local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-writes-resources.json';

(import 'dashboard-utils.libsonnet') {
  [filename]:
    ($.dashboard('Writes resources') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates(false)
    .addRow(
      $.row('Summary')
      .addPanel(
        $.panel('CPU') +
        $.queryPanel($.resourceUtilizationQuery('cpu', $._config.instance_names.write, $._config.container_names.write), '{{%s}}' % $._config.per_instance_label) +
        $.stack,
      )
      .addPanel(
        $.panel('Memory (workingset)') +
        $.queryPanel($.resourceUtilizationQuery('memory_working', $._config.instance_names.write, $._config.container_names.write), '{{%s}}' % $._config.per_instance_label) +
        $.stack +
        { yaxes: $.yaxes('bytes') },
      )
      .addPanel(
        $.containerGoHeapInUsePanel($._config.instance_names.write, $._config.container_names.write) +
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
      $.row('Distributor')
      .addPanel(
        $.containerCPUUsagePanel($._config.instance_names.distributor, $._config.container_names.distributor),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel($._config.instance_names.distributor, $._config.container_names.distributor),
      )
      .addPanel(
        $.containerGoHeapInUsePanel($._config.instance_names.distributor, $._config.container_names.distributor),
      )
    )
    .addRow(
      $.row('Ingester')
      .addPanel(
        $.panel('In-memory series') +
        $.queryPanel(
          'sum by(%s) (cortex_ingester_memory_series{%s})' % [$._config.per_instance_label, $.jobMatcher($._config.job_names.ingester)],
          '{{%s}}' % $._config.per_instance_label
        ) +
        {
          tooltip: { sort: 2 },  // Sort descending.
          fill: 0,
        },
      )
      .addPanel(
        $.containerCPUUsagePanel($._config.instance_names.ingester, $._config.container_names.ingester),
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
      .addPanel(
        $.containerGoHeapInUsePanel($._config.instance_names.ingester, $._config.container_names.ingester),
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.containerDiskWritesPanel($._config.instance_names.ingester, $._config.container_names.ingester)
      )
      .addPanel(
        $.containerDiskReadsPanel($._config.instance_names.ingester, $._config.container_names.ingester)
      )
      .addPanel(
        $.containerDiskSpaceUtilization($._config.instance_names.ingester, $._config.container_names.ingester),
      )
    )
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
}
