local utils = import 'mixin-utils/utils.libsonnet';

(import 'grafana-builder/grafana.libsonnet') {

  _config:: error 'must provide _config',

  // Override the dashboard constructor to add:
  // - default tags,
  // - some links that propagate the selectred cluster.
  dashboard(title)::
    super.dashboard(title) + {
      tags: $._config.tags,

      addRowIf(condition, row)::
        if condition
        then self.addRow(row)
        else self,

      addClusterSelectorTemplates()::
        local d = self {
          links: [
            {
              asDropdown: true,
              icon: 'external link',
              includeVars: true,
              keepTime: true,
              tags: $._config.tags,
              targetBlank: false,
              title: 'Cortex Dashboards',
              type: 'dashboards',
            },
          ],
        };

        if $._config.singleBinary
        then d.addMultiTemplate('job', 'cortex_build_info', 'job')
        else d
             .addMultiTemplate('cluster', 'cortex_build_info', 'cluster')
             .addMultiTemplate('namespace', 'cortex_build_info', 'namespace'),
    },

  // The ,ixin allow specialism of the job selector depending on if its a single binary
  // deployment or a namespaced one.
  jobMatcher(job)::
    if $._config.singleBinary
    then 'job=~"$job"'
    else 'cluster=~"$cluster", job=~"($namespace)/%s"' % job,

  namespaceMatcher()::
    if $._config.singleBinary
    then 'job=~"$job"'
    else 'cluster=~"$cluster", namespace=~"$namespace"',

  jobSelector(job)::
    if $._config.singleBinary
    then [utils.selector.noop('cluster'), utils.selector.re('job', '$job')]
    else [utils.selector.re('cluster', '$cluster'), utils.selector.re('job', '($namespace)/%s' % job)],

  qpsPanel(selector)::
    super.qpsPanel(selector) + {
      targets: [
        target {
          interval: '1m',
        }
        for target in super.targets
      ],
    },

  latencyPanel(metricName, selector, multiplier='1e3')::
    super.latencyPanel(metricName, selector, multiplier) + {
      targets: [
        target {
          interval: '1m',
        }
        for target in super.targets
      ],
    },

  successFailurePanel(title, successMetric, failureMetric)::
    $.panel(title) +
    $.queryPanel([successMetric, failureMetric], ['successful', 'failed']) +
    $.stack + {
      aliasColors: {
        successful: '#7EB26D',
        failed: '#E24D42',
      },
    },

  objectStorePanels1(title, metricPrefix)::
    local opsTotal = '%s_thanos_objstore_bucket_operations_total' % [metricPrefix];
    local opsTotalFailures = '%s_thanos_objstore_bucket_operation_failures_total' % [metricPrefix];
    local operationDuration = '%s_thanos_objstore_bucket_operation_duration_seconds' % [metricPrefix];
    local interval = '$__interval';
    super.row(title)
    .addPanel(
      // We use 'up{cluster=~"$cluster", job="($namespace)/.+"}' to add 0 if there are no failed operations.
      self.successFailurePanel(
        'Operations/sec',
        'sum(rate(%s{cluster=~"$cluster"}[%s])) - sum(rate(%s{cluster=~"$cluster"}[%s]) or (up{cluster=~"$cluster", job="($namespace)/.+"}*0))' % [opsTotal, interval, opsTotalFailures, interval],
        'sum(rate(%s{cluster=~"$cluster"}[%s]) or (up{cluster=~"$cluster", job="($namespace)/.+"}*0))' % [opsTotalFailures, interval]
      )
    )
    .addPanel(
      $.panel('Op: ObjectSize') +
      $.latencyPanel(operationDuration, '{cluster=~"$cluster", operation="objectsize"}'),
    )
    .addPanel(
      // Cortex (Thanos) doesn't track timing for 'iter', so we use ops/sec instead.
      $.panel('Op: Iter') +
      $.queryPanel('sum(rate(%s{cluster=~"$cluster", operation="iter"}[$__interval]))' % [opsTotal], 'ops/sec')
    )
    .addPanel(
      $.panel('Op: Exists') +
      $.latencyPanel(operationDuration, '{cluster=~"$cluster", operation="exists"}'),
    ),

  // Second row of Object Store stats
  objectStorePanels2(title, metricPrefix)::
    local operationDuration = '%s_thanos_objstore_bucket_operation_duration_seconds' % [metricPrefix];
    super.row(title)
    .addPanel(
      $.panel('Op: Get') +
      $.latencyPanel(operationDuration, '{cluster=~"$cluster", operation="get"}'),
    )
    .addPanel(
      $.panel('Op: GetRange') +
      $.latencyPanel(operationDuration, '{cluster=~"$cluster", operation="get_range"}'),
    )
    .addPanel(
      $.panel('Op: Upload') +
      $.latencyPanel(operationDuration, '{cluster=~"$cluster", operation="upload"}'),
    )
    .addPanel(
      $.panel('Op: Delete') +
      $.latencyPanel(operationDuration, '{cluster=~"$cluster", operation="delete"}'),
    ),
}
