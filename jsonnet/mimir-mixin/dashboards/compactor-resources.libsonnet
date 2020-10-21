local utils = import 'mixin-utils/utils.libsonnet';

(import 'dashboard-utils.libsonnet') {
  'cortex-compactor-resources.json':
    local filterNodeDiskByCompactor = |||
      ignoring(pod) group_right() (label_replace(count by(pod, instance, device) (container_fs_writes_bytes_total{%s,container="compactor",device!~".*sda.*"}), "device", "$1", "device", "/dev/(.*)") * 0)
    ||| % $.namespaceMatcher();

    ($.dashboard('Cortex / Compactor Resources') + { uid: 'df9added6f1f4332f95848cca48ebd99' })
    .addClusterSelectorTemplates()
    .addRow(
      $.row('CPU and Memory')
      .addPanel(
        $.containerCPUUsagePanel('CPU', 'compactor'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanel('Memory (workingset)', 'compactor'),
      )
      .addPanel(
        $.goHeapInUsePanel('Memory (go heap inuse)', 'compactor'),
      )
    )
    .addRow(
      $.row('Network')
      .addPanel(
        $.panel('Receive Bandwidth') +
        $.queryPanel('sum by(pod) (rate(container_network_receive_bytes_total{%s,pod=~"compactor.*"}[$__interval]))' % $.namespaceMatcher(), '{{pod}}') +
        $.stack +
        { yaxes: $.yaxes('Bps') },
      )
      .addPanel(
        $.panel('Transmit Bandwidth') +
        $.queryPanel('sum by(pod) (rate(container_network_transmit_bytes_total{%s,pod=~"compactor.*"}[$__interval]))' % $.namespaceMatcher(), '{{pod}}') +
        $.stack +
        { yaxes: $.yaxes('Bps') },
      )
    )
    .addRow(
      $.row('Disk')
      .addPanel(
        $.panel('Writes') +
        $.queryPanel('sum by(instance, device) (rate(node_disk_written_bytes_total[$__interval])) + %s' % filterNodeDiskByCompactor, '{{pod}} - {{device}}') +
        $.stack +
        { yaxes: $.yaxes('Bps') },
      )
      .addPanel(
        $.panel('Reads') +
        $.queryPanel('sum by(instance, device) (rate(node_disk_read_bytes_total[$__interval])) + %s' % filterNodeDiskByCompactor, '{{pod}} - {{device}}') +
        $.stack +
        { yaxes: $.yaxes('Bps') },
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.panel('Disk Space Utilization') +
        $.queryPanel('max by(persistentvolumeclaim) (kubelet_volume_stats_used_bytes{%s} / kubelet_volume_stats_capacity_bytes{%s}) and count by(persistentvolumeclaim) (kube_persistentvolumeclaim_labels{%s,label_name="compactor"})' % [$.namespaceMatcher(), $.namespaceMatcher(), $.namespaceMatcher()], '{{persistentvolumeclaim}}') +
        { yaxes: $.yaxes('percentunit') },
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
