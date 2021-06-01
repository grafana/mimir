local utils = import 'mixin-utils/utils.libsonnet';

(import 'dashboard-utils.libsonnet') {
  'alertmanager-resources.json':
    ($.dashboard('Cortex / Alertmanager Resources') + { uid: '68b66aed90ccab448009089544a8d6c6' })
    .addClusterSelectorTemplates(false)
    .addRow(
      $.row('Gateway')
      .addPanel(
        $.containerCPUUsagePanel('CPU', 'cortex-gw'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel('Memory (workingset)', 'cortex-gw'),
      )
      .addPanel(
        $.goHeapInUsePanel('Memory (go heap inuse)', $._config.job_names.gateway),
      )
    )
    .addRow(
      $.row('Alertmanager')
      .addPanel(
        $.containerCPUUsagePanel('CPU', 'alertmanager'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel('Memory (workingset)', 'alertmanager'),
      )
      .addPanel(
        $.goHeapInUsePanel('Memory (go heap inuse)', 'alertmanager'),
      )
    )
    .addRow(
      $.row('Instance Mapper')
      .addPanel(
        $.containerCPUUsagePanel('CPU', 'alertmanager-im'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel('Memory (workingset)', 'alertmanager-im'),
      )
      .addPanel(
        $.goHeapInUsePanel('Memory (go heap inuse)', 'alertmanager-im'),
      )
    )
    .addRow(
      $.row('Network')
      .addPanel(
        $.containerNetworkReceiveBytesPanel($._config.instance_names.alertmanager),
      )
      .addPanel(
        $.containerNetworkTransmitBytesPanel($._config.instance_names.alertmanager),
      )
    )
    .addRow(
      $.row('Disk')
      .addPanel(
        $.panel('Writes') +
        $.queryPanel(
          'sum by(%s, %s, device) (rate(node_disk_written_bytes_total[$__rate_interval])) + %s' % [$._config.per_node_label, $._config.per_instance_label, $.filterNodeDiskContainer('alertmanager')],
          '{{%s}} - {{device}}' % $._config.per_instance_label
        ) +
        $.stack +
        { yaxes: $.yaxes('Bps') },
      )
      .addPanel(
        $.panel('Reads') +
        $.queryPanel(
          'sum by(%s, %s, device) (rate(node_disk_read_bytes_total[$__rate_interval])) + %s' % [$._config.per_node_label, $._config.per_instance_label, $.filterNodeDiskContainer('alertmanager')],
          '{{%s}} - {{device}}' % $._config.per_instance_label
        ) +
        $.stack +
        { yaxes: $.yaxes('Bps') },
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.panel('Disk Space Utilization') +
        $.queryPanel('max by(persistentvolumeclaim) (kubelet_volume_stats_used_bytes{%s} / kubelet_volume_stats_capacity_bytes{%s}) and count by(persistentvolumeclaim) (kube_persistentvolumeclaim_labels{%s,label_name="alertmanager"})' % [$.namespaceMatcher(), $.namespaceMatcher(), $.namespaceMatcher()], '{{persistentvolumeclaim}}') +
        { yaxes: $.yaxes('percentunit') },
      )
    ),
}
