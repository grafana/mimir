local utils = import 'mixin-utils/utils.libsonnet';

(import 'dashboard-utils.libsonnet') {
  'mimir-writes-resources.json':
    ($.dashboard('Writes resources') + { uid: 'c0464f0d8bd026f776c9006b0591bb0b' })
    .addClusterSelectorTemplates(false)
    .addRow(
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
      $.row('Distributor')
      .addPanel(
        $.containerCPUUsagePanel('CPU', 'distributor'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel('Memory (workingset)', 'distributor'),
      )
      .addPanel(
        $.goHeapInUsePanel('Memory (go heap inuse)', $._config.job_names.distributor),
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
        },
      )
      .addPanel(
        $.containerCPUUsagePanel('CPU', 'ingester'),
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.containerMemoryWorkingSetPanel('Memory (workingset)', 'ingester'),
      )
      .addPanel(
        $.goHeapInUsePanel('Memory (go heap inuse)', $._config.job_names.ingester),
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.containerDiskWritesPanel('Disk writes', 'ingester')
      )
      .addPanel(
        $.containerDiskReadsPanel('Disk reads', 'ingester')
      )
      .addPanel(
        $.containerDiskSpaceUtilization('Disk space utilization', 'ingester'),
      )
    )
    + {
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
