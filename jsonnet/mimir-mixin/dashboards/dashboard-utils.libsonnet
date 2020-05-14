local utils = import 'mixin-utils/utils.libsonnet';

(import 'grafana-builder/grafana.libsonnet') {

  _config:: error 'must provide _config',

  // Override the dashboard constructor to add:
  // - default tags,
  // - some links that propagate the selectred cluster.
  dashboard(title)::
    super.dashboard(title) + {
      addRowIf(condition, row)::
        if condition
        then self.addRow(row)
        else self,

      addClusterSelectorTemplates()::
        local d = self {
          tags: $._config.tags,
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

  queryPanel(queries, legends, legendLink=null)::
    super.queryPanel(queries, legends, legendLink) + {
      targets: [
        target {
          interval: '1m',
        }
        for target in super.targets
      ],
    },

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

  // Displays started, completed and failed rate.
  startedCompletedFailedPanel(title, startedMetric, completedMetric, failedMetric)::
    $.panel(title) +
    $.queryPanel([startedMetric, completedMetric, failedMetric], ['started', 'completed', 'failed']) +
    $.stack + {
      aliasColors: {
        started: '#34CCEB',
        completed: '#7EB26D',
        failed: '#E24D42',
      },
    },

  containerCPUUsagePanel(title, containerName)::
    $.panel(title) +
    $.queryPanel([
      'sum by(pod_name) (rate(container_cpu_usage_seconds_total{%s,container_name="%s"}[$__interval]))' % [$.namespaceMatcher(), containerName],
      'min(container_spec_cpu_quota{%s,container_name="%s"} / container_spec_cpu_period{%s,container_name="%s"})' % [$.namespaceMatcher(), containerName, $.namespaceMatcher(), containerName],
    ], ['{{pod_name}}', 'limit']) +
    {
      seriesOverrides: [
        {
          alias: 'limit',
          color: '#E02F44',
          fill: 0,
        },
      ],
    },

  containerMemoryWorkingSetPanel(title, containerName)::
    $.panel(title) +
    $.queryPanel([
      'sum by(pod_name) (container_memory_working_set_bytes{%s,container_name="%s"})' % [$.namespaceMatcher(), containerName],
      'min(container_spec_memory_limit_bytes{%s,container_name="%s"} > 0)' % [$.namespaceMatcher(), containerName],
    ], ['{{pod_name}}', 'limit']) +
    {
      seriesOverrides: [
        {
          alias: 'limit',
          color: '#E02F44',
          fill: 0,
        },
      ],
      yaxes: $.yaxes('bytes'),
    },

  goHeapInUsePanel(title, jobName)::
    $.panel(title) +
    $.queryPanel('sum by(instance) (go_memstats_heap_inuse_bytes{%s})' % $.jobMatcher(jobName), '{{instance}}') +
    { yaxes: $.yaxes('bytes') },

  // Switches a panel from lines (default) to bars.
  bars:: {
    bars: true,
    lines: false,
  },

  textPanel(title, content, options={}):: {
    content: content,
    datasource: null,
    description: '',
    mode: 'markdown',
    title: title,
    transparent: true,
    type: 'text',
  } + options,

  objectStorePanels1(title, component)::
    super.row(title)
    .addPanel(
      $.panel('Operations / sec') +
      $.queryPanel('sum by(operation) (rate(thanos_objstore_bucket_operations_total{%s,component="%s"}[$__interval]))' % [$.namespaceMatcher(), component], '{{operation}}') +
      $.stack +
      { yaxes: $.yaxes('rps') },
    )
    .addPanel(
      $.panel('Error rate') +
      $.queryPanel('sum by(operation) (rate(thanos_objstore_bucket_operation_failures_total{%s,component="%s"}[$__interval])) / sum by(operation) (rate(thanos_objstore_bucket_operations_total{%s,component="%s"}[$__interval]))' % [$.namespaceMatcher(), component, $.namespaceMatcher(), component], '{{operation}}') +
      { yaxes: $.yaxes('percentunit') },
    )
    .addPanel(
      $.panel('Op: ObjectSize') +
      $.latencyPanel('thanos_objstore_bucket_operation_duration_seconds', '{%s,component="%s",operation="objectsize"}' % [$.namespaceMatcher(), component]),
    )
    .addPanel(
      $.panel('Op: Exists') +
      $.latencyPanel('thanos_objstore_bucket_operation_duration_seconds', '{%s,component="%s",operation="exists"}' % [$.namespaceMatcher(), component]),
    ),

  // Second row of Object Store stats
  objectStorePanels2(title, component)::
    super.row(title)
    .addPanel(
      $.panel('Op: Get') +
      $.latencyPanel('thanos_objstore_bucket_operation_duration_seconds', '{%s,component="%s",operation="get"}' % [$.namespaceMatcher(), component]),
    )
    .addPanel(
      $.panel('Op: GetRange') +
      $.latencyPanel('thanos_objstore_bucket_operation_duration_seconds', '{%s,component="%s",operation="get_range"}' % [$.namespaceMatcher(), component]),
    )
    .addPanel(
      $.panel('Op: Upload') +
      $.latencyPanel('thanos_objstore_bucket_operation_duration_seconds', '{%s,component="%s",operation="upload"}' % [$.namespaceMatcher(), component]),
    )
    .addPanel(
      $.panel('Op: Delete') +
      $.latencyPanel('thanos_objstore_bucket_operation_duration_seconds', '{%s,component="%s",operation="delete"}' % [$.namespaceMatcher(), component]),
    ),
}
