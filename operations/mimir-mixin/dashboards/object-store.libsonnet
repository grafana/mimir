local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-object-store.json';

(import 'dashboard-utils.libsonnet') {
  [filename]:
    ($.dashboard('Object Store') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates()
    .addRow(
      $.row('Components')
      .addPanel(
        $.panel('RPS / component') +
        $.queryPanel('sum by(component) (rate(thanos_objstore_bucket_operations_total{%s}[$__rate_interval]))' % $.namespaceMatcher(), '{{component}}') +
        $.stack +
        { yaxes: $.yaxes('reqps') },
      )
      .addPanel(
        $.timeseriesPanel('Error rate / component') +
        $.queryPanel('sum by(component) (rate(thanos_objstore_bucket_operation_failures_total{%s}[$__rate_interval])) / sum by(component) (rate(thanos_objstore_bucket_operations_total{%s}[$__rate_interval])) >= 0' % [$.namespaceMatcher(), $.namespaceMatcher()], '{{component}}') +
        { fieldConfig: { defaults: { noValue: '0', unit: 'percentunit', min: 0, max: 1 } } }
      )
    )
    .addRow(
      $.row('Operations')
      .addPanel(
        $.panel('RPS / operation') +
        $.queryPanel('sum by(operation) (rate(thanos_objstore_bucket_operations_total{%s}[$__rate_interval]))' % $.namespaceMatcher(), '{{operation}}') +
        $.stack +
        { yaxes: $.yaxes('reqps') },
      )
      .addPanel(
        $.timeseriesPanel('Error rate / operation') +
        $.queryPanel('sum by(operation) (rate(thanos_objstore_bucket_operation_failures_total{%s}[$__rate_interval])) / sum by(operation) (rate(thanos_objstore_bucket_operations_total{%s}[$__rate_interval])) >= 0' % [$.namespaceMatcher(), $.namespaceMatcher()], '{{operation}}') +
        { fieldConfig: { defaults: { noValue: '0', unit: 'percentunit', min: 0, max: 1 } } }
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.panel('Op: Get') +
        $.latencyPanel('thanos_objstore_bucket_operation_duration_seconds', '{%s,operation="get"}' % $.namespaceMatcher()),
      )
      .addPanel(
        $.panel('Op: GetRange') +
        $.latencyPanel('thanos_objstore_bucket_operation_duration_seconds', '{%s,operation="get_range"}' % $.namespaceMatcher()),
      )
      .addPanel(
        $.panel('Op: Exists') +
        $.latencyPanel('thanos_objstore_bucket_operation_duration_seconds', '{%s,operation="exists"}' % $.namespaceMatcher()),
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.panel('Op: Attributes') +
        $.latencyPanel('thanos_objstore_bucket_operation_duration_seconds', '{%s,operation="attributes"}' % $.namespaceMatcher()),
      )
      .addPanel(
        $.panel('Op: Upload') +
        $.latencyPanel('thanos_objstore_bucket_operation_duration_seconds', '{%s,operation="upload"}' % $.namespaceMatcher()),
      )
      .addPanel(
        $.panel('Op: Delete') +
        $.latencyPanel('thanos_objstore_bucket_operation_duration_seconds', '{%s,operation="delete"}' % $.namespaceMatcher()),
      )
    ),
}
