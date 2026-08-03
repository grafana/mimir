local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-object-store.json';

(import 'dashboard-utils.libsonnet') {
  // The bucket variable lists the buckets of the selected read compartments, plus the
  // non-compartmentalized ones which are always included.
  // ${read_compartment:pipe} avoids the capture group that the default multi-value
  // interpolation adds, which Grafana would otherwise use as the variable value.
  local bucketVariableRegex = '/^(?:(?!.*-rc-[0-9]+$).*|.*(?:${read_compartment:pipe}))$/',

  local addBucketVariable(dashboard) =
    if $._config.compartments_enabled then
      dashboard.addMultiTemplate(
        'bucket',
        'thanos_objstore_bucket_operations_total{%s}' % $.namespaceMatcher(),
        'bucket',
        allValue=null,
        regex=bucketVariableRegex,
      )
    else
      dashboard,

  local bucketSelector = if $._config.compartments_enabled then ', bucket=~"$bucket|"' else '',

  local rateBy(label, metric) =
    'sum by(%s) (rate(%s{%s%s}[$__rate_interval]))' % [label, metric, $.namespaceMatcher(), bucketSelector],

  local operationSelector(operation) = '%s,operation="%s"%s' % [$.namespaceMatcher(), operation, bucketSelector],

  [filename]:
    assert std.md5(filename) == 'e1324ee2a434f4158c00a9ee279d3292' : 'UID of the dashboard has changed, please update references to dashboard.';
    addBucketVariable(
      ($.dashboard('Object Store') + { uid: std.md5(filename) })
      .addClusterSelectorTemplates()
      .addShowNativeLatencyVariable($.latencyVariableDefault())
    )
    .addRow(
      $.row('Components')
      .addPanel(
        $.timeseriesPanel('RPS / component') +
        $.queryPanel(rateBy('component', 'thanos_objstore_bucket_operations_total'), '{{component}}') +
        $.stack +
        { fieldConfig+: { defaults+: { unit: 'reqps' } } },
      )
      .addPanel(
        $.timeseriesPanel('Error rate / component') +
        $.queryPanel(
          '%s / %s >= 0' % [
            rateBy('component', 'thanos_objstore_bucket_operation_failures_total'),
            rateBy('component', 'thanos_objstore_bucket_operations_total'),
          ],
          '{{component}}',
        ) +
        { fieldConfig: { defaults: { noValue: '0', unit: 'percentunit', min: 0, max: 1 } } }
      )
    )
    .addRow(
      $.row('Operations')
      .addPanel(
        $.timeseriesPanel('RPS / operation') +
        $.queryPanel(rateBy('operation', 'thanos_objstore_bucket_operations_total'), '{{operation}}') +
        $.stack +
        { fieldConfig+: { defaults+: { unit: 'reqps' } } },
      )
      .addPanel(
        $.timeseriesPanel('Error rate / operation') +
        $.queryPanel(
          '%s / %s >= 0' % [
            rateBy('operation', 'thanos_objstore_bucket_operation_failures_total'),
            rateBy('operation', 'thanos_objstore_bucket_operations_total'),
          ],
          '{{operation}}',
        ) +
        { fieldConfig: { defaults: { noValue: '0', unit: 'percentunit', min: 0, max: 1 } } }
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.timeseriesPanel('Op: Get') +
        $.ncLatencyPanel('thanos_objstore_bucket_operation_duration_seconds', operationSelector('get')),
      )
      .addPanel(
        $.timeseriesPanel('Op: GetRange') +
        $.ncLatencyPanel('thanos_objstore_bucket_operation_duration_seconds', operationSelector('get_range')),
      )
      .addPanel(
        $.timeseriesPanel('Op: Exists') +
        $.ncLatencyPanel('thanos_objstore_bucket_operation_duration_seconds', operationSelector('exists')),
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.timeseriesPanel('Op: Attributes') +
        $.ncLatencyPanel('thanos_objstore_bucket_operation_duration_seconds', operationSelector('attributes')),
      )
      .addPanel(
        $.timeseriesPanel('Op: Upload') +
        $.ncLatencyPanel('thanos_objstore_bucket_operation_duration_seconds', operationSelector('upload')),
      )
      .addPanel(
        $.timeseriesPanel('Op: Delete') +
        $.ncLatencyPanel('thanos_objstore_bucket_operation_duration_seconds', operationSelector('delete')),
      )
    ),
}
