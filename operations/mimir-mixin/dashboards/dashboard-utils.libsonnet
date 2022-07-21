local utils = import 'mixin-utils/utils.libsonnet';

(import 'grafana-builder/grafana.libsonnet') {
  local resourceRequestStyle = { alias: 'request', color: '#FFC000', fill: 0, dashes: true, dashLength: 5 },
  local resourceLimitStyle = { alias: 'limit', color: '#E02F44', fill: 0, dashes: true, dashLength: 5 },

  local resourceRequestColor = '#FFC000',
  local resourceLimitColor = '#E02F44',

  _config:: error 'must provide _config',

  row(title)::
    super.row(title) + {
      addPanelIf(condition, panel)::
        if condition
        then self.addPanel(panel)
        else self,
    },

  // Override the dashboard constructor to add:
  // - default tags,
  // - some links that propagate the selectred cluster.
  dashboard(title)::
    // Prefix the dashboard title with "<product> /" unless configured otherwise.
    super.dashboard(
      title='%(prefix)s%(title)s' % { prefix: $._config.dashboard_prefix, title: title },
      datasource=$._config.dashboard_datasource,
      datasource_regex=$._config.datasource_regex
    ) + {
      __requires: [
        {
          id: 'grafana',
          name: 'Grafana',
          type: 'grafana',
          version: '8.0.0',
        },
      ],

      addRowIf(condition, row)::
        if condition
        then self.addRow(row)
        else self,

      addRowsIf(condition, rows)::
        if condition
        then
          local reduceRows(dashboard, remainingRows) =
            if (std.length(remainingRows) == 0)
            then dashboard
            else
              reduceRows(
                dashboard.addRow(remainingRows[0]),
                std.slice(remainingRows, 1, std.length(remainingRows), 1)
              )
          ;
          reduceRows(self, rows)
        else self,

      addRows(rows)::
        self.addRowsIf(true, rows),

      addClusterSelectorTemplates(multi=true)::
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
              title: '%(product)s dashboards' % $._config,
              type: 'dashboards',
            },
          ],
        };

        if multi then
          if $._config.singleBinary
          then d.addMultiTemplate('job', 'cortex_build_info', 'job')
          else d
               .addMultiTemplate('cluster', 'cortex_build_info', '%s' % $._config.per_cluster_label)
               .addMultiTemplate('namespace', 'cortex_build_info{%s=~"$cluster"}' % $._config.per_cluster_label, 'namespace')
        else
          if $._config.singleBinary
          then d.addTemplate('job', 'cortex_build_info', 'job')
          else d
               .addTemplate('cluster', 'cortex_build_info', '%s' % $._config.per_cluster_label)
               .addTemplate('namespace', 'cortex_build_info{%s=~"$cluster"}' % $._config.per_cluster_label, 'namespace'),

      addActiveUserSelectorTemplates()::
        self.addTemplate('user', 'cortex_ingester_active_series{%s=~"$cluster", namespace=~"$namespace"}' % $._config.per_cluster_label, 'user'),

      addCustomTemplate(name, values, defaultIndex=0):: self {
        templating+: {
          list+: [
            {
              name: name,
              options: [
                {
                  selected: v == values[defaultIndex],
                  text: v,
                  value: v,
                }
                for v in values
              ],
              current: {
                selected: true,
                text: values[defaultIndex],
                value: values[defaultIndex],
              },
              type: 'custom',
              hide: 0,
              includeAll: false,
              multi: false,
            },
          ],
        },
      },
    },

  // The mixin allow specialism of the job selector depending on if its a single binary
  // deployment or a namespaced one.
  jobMatcher(job)::
    if $._config.singleBinary
    then 'job=~"$job"'
    else '%s=~"$cluster", job=~"($namespace)/(%s)"' % [$._config.per_cluster_label, job],

  namespaceMatcher()::
    if $._config.singleBinary
    then 'job=~"$job"'
    else '%s=~"$cluster", namespace=~"$namespace"' % $._config.per_cluster_label,

  jobSelector(job)::
    if $._config.singleBinary
    then [utils.selector.noop('%s' % $._config.per_cluster_label), utils.selector.re('job', '$job')]
    else [utils.selector.re('%s' % $._config.per_cluster_label, '$cluster'), utils.selector.re('job', '($namespace)/(%s)' % job)],

  panel(title)::
    super.panel(title) + {
      tooltip+: {
        shared: false,
        sort: 0,
      },
    },

  queryPanel(queries, legends, legendLink=null)::
    super.queryPanel(queries, legends, legendLink) + {
      targets: [
        target {
          interval: '15s',
        }
        for target in super.targets
      ],
    },

  // hiddenLegendQueryPanel is a standard query panel designed to handle a large number of series.  it hides the legend, doesn't fill the series and
  //  sorts the tooltip descending
  hiddenLegendQueryPanel(queries, legends, legendLink=null)::
    $.queryPanel(queries, legends, legendLink) +
    {
      legend: { show: false },
      fill: 0,
      tooltip: { sort: 2 },
    },

  qpsPanel(selector)::
    super.qpsPanel(selector) + {
      targets: [
        target {
          interval: '15s',
        }
        for target in super.targets
      ],
    },

  latencyPanel(metricName, selector, multiplier='1e3')::
    super.latencyPanel(metricName, selector, multiplier) + {
      targets: [
        target {
          interval: '15s',
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
      'sum by(%s) (rate(container_cpu_usage_seconds_total{%s,container=~"%s"}[$__rate_interval]))' % [$._config.per_instance_label, $.namespaceMatcher(), containerName],
      'min(container_spec_cpu_quota{%s,container=~"%s"} / container_spec_cpu_period{%s,container=~"%s"})' % [$.namespaceMatcher(), containerName, $.namespaceMatcher(), containerName],
      'min(kube_pod_container_resource_requests{%s,container=~"%s",resource="cpu"})' % [$.namespaceMatcher(), containerName],
    ], ['{{%s}}' % $._config.per_instance_label, 'limit', 'request']) +
    {
      seriesOverrides: [
        resourceRequestStyle,
        resourceLimitStyle,
      ],
      tooltip: { sort: 2 },  // Sort descending.
      fill: 0,
    },

  containerMemoryWorkingSetPanel(title, containerName)::
    $.panel(title) +
    $.queryPanel([
      // We use "max" instead of "sum" otherwise during a rolling update of a statefulset we will end up
      // summing the memory of the old instance/pod (whose metric will be stale for 5m) to the new instance/pod.
      'max by(%s) (container_memory_working_set_bytes{%s,container=~"%s"})' % [$._config.per_instance_label, $.namespaceMatcher(), containerName],
      'min(container_spec_memory_limit_bytes{%s,container=~"%s"} > 0)' % [$.namespaceMatcher(), containerName],
      'min(kube_pod_container_resource_requests{%s,container=~"%s",resource="memory"})' % [$.namespaceMatcher(), containerName],
    ], ['{{%s}}' % $._config.per_instance_label, 'limit', 'request']) +
    {
      seriesOverrides: [
        resourceRequestStyle,
        resourceLimitStyle,
      ],
      yaxes: $.yaxes('bytes'),
      tooltip: { sort: 2 },  // Sort descending.
      fill: 0,
    },

  containerMemoryRSSPanel(title, containerName)::
    $.panel(title) +
    $.queryPanel([
      // We use "max" instead of "sum" otherwise during a rolling update of a statefulset we will end up
      // summing the memory of the old instance/pod (whose metric will be stale for 5m) to the new instance/pod.
      'max by(%s) (container_memory_rss{%s,container=~"%s"})' % [$._config.per_instance_label, $.namespaceMatcher(), containerName],
      'min(container_spec_memory_limit_bytes{%s,container=~"%s"} > 0)' % [$.namespaceMatcher(), containerName],
      'min(kube_pod_container_resource_requests{%s,container=~"%s",resource="memory"})' % [$.namespaceMatcher(), containerName],
    ], ['{{%s}}' % $._config.per_instance_label, 'limit', 'request']) +
    {
      seriesOverrides: [
        resourceRequestStyle,
        resourceLimitStyle,
      ],
      yaxes: $.yaxes('bytes'),
      tooltip: { sort: 2 },  // Sort descending.
      fill: 0,
    },

  containerNetworkPanel(title, metric, instanceName)::
    $.panel(title) +
    $.queryPanel(
      'sum by(%(instance)s) (rate(%(metric)s{%(namespace)s,%(instance)s=~"%(instanceName)s"}[$__rate_interval]))' % {
        namespace: $.namespaceMatcher(),
        metric: metric,
        instance: $._config.per_instance_label,
        instanceName: instanceName,
      }, '{{%s}}' % $._config.per_instance_label
    ) +
    $.stack +
    { yaxes: $.yaxes('Bps') },

  containerNetworkReceiveBytesPanel(instanceName)::
    $.containerNetworkPanel('Receive bandwidth', 'container_network_receive_bytes_total', instanceName),

  containerNetworkTransmitBytesPanel(instanceName)::
    $.containerNetworkPanel('Transmit bandwidth', 'container_network_transmit_bytes_total', instanceName),

  containerDiskWritesPanel(title, containerName)::
    $.panel(title) +
    $.queryPanel(
      |||
        sum by(%s, %s, device) (
          rate(
            node_disk_written_bytes_total[$__rate_interval]
          )
        )
        +
        %s
      ||| % [
        $._config.per_node_label,
        $._config.per_instance_label,
        $.filterNodeDiskContainer(containerName),
      ],
      '{{%s}} - {{device}}' % $._config.per_instance_label
    ) +
    $.stack +
    { yaxes: $.yaxes('Bps') },

  containerDiskReadsPanel(title, containerName)::
    $.panel(title) +
    $.queryPanel(
      |||
        sum by(%s, %s, device) (
          rate(
            node_disk_read_bytes_total[$__rate_interval]
          )
        ) + %s
      ||| % [
        $._config.per_node_label,
        $._config.per_instance_label,
        $.filterNodeDiskContainer(containerName),
      ],
      '{{%s}} - {{device}}' % $._config.per_instance_label
    ) +
    $.stack +
    { yaxes: $.yaxes('Bps') },

  containerDiskSpaceUtilization(title, containerName)::
    $.panel(title) +
    $.queryPanel(
      |||
        max by(persistentvolumeclaim) (
          kubelet_volume_stats_used_bytes{%(namespace)s} /
          kubelet_volume_stats_capacity_bytes{%(namespace)s}
        )
        and
        count by(persistentvolumeclaim) (
          kube_persistentvolumeclaim_labels{
            %(namespace)s,
            %(label)s
          }
        )
      ||| % {
        namespace: $.namespaceMatcher(),
        label: $.containerLabelMatcher(containerName),
      }, '{{persistentvolumeclaim}}'
    ) +
    {
      yaxes: $.yaxes('percentunit'),
      fill: 0,
    },

  containerLabelMatcher(containerName)::
    if containerName == 'ingester' then 'label_name=~"ingester.*"'
    else if containerName == 'store-gateway' then 'label_name=~"store-gateway.*"'
    else 'label_name="%s"' % containerName,

  jobNetworkingRow(title, name)::
    local vars = $._config {
      job_matcher: $.jobMatcher($._config.job_names[name]),
    };

    super.row(title)
    .addPanel($.containerNetworkReceiveBytesPanel($._config.instance_names[name]))
    .addPanel($.containerNetworkTransmitBytesPanel($._config.instance_names[name]))
    .addPanel(
      $.panel('Inflight requests (per pod)') +
      $.queryPanel([
        'avg(cortex_inflight_requests{%(job_matcher)s})' % vars,
        'max(cortex_inflight_requests{%(job_matcher)s})' % vars,
      ], ['avg', 'highest']) +
      { fill: 0 }
    )
    .addPanel(
      $.panel('TCP connections (per pod)') +
      $.queryPanel([
        'avg(sum by(%(per_instance_label)s) (cortex_tcp_connections{%(job_matcher)s}))' % vars,
        'max(sum by(%(per_instance_label)s) (cortex_tcp_connections{%(job_matcher)s}))' % vars,
        'min(cortex_tcp_connections_limit{%(job_matcher)s})' % vars,
      ], ['avg', 'highest', 'limit']) +
      { fill: 0 }
    ),

  kvStoreRow(title, jobName, kvName)::
    super.row(title)
    .addPanel(
      $.panel('Requests / sec') +
      $.qpsPanel('cortex_kv_request_duration_seconds_count{%s, kv_name=~"%s"}' % [$.jobMatcher($._config.job_names[jobName]), kvName])
    )
    .addPanel(
      $.panel('Latency') +
      $.latencyPanel('cortex_kv_request_duration_seconds', '{%s, kv_name=~"%s"}' % [$.jobMatcher($._config.job_names[jobName]), kvName])
    ),

  goHeapInUsePanel(title, jobName)::
    $.panel(title) +
    $.queryPanel(
      'sum by(%s) (go_memstats_heap_inuse_bytes{%s})' % [$._config.per_instance_label, $.jobMatcher(jobName)],
      '{{%s}}' % $._config.per_instance_label
    ) +
    {
      yaxes: $.yaxes('bytes'),
      tooltip: { sort: 2 },  // Sort descending.
      fill: 0,
    },

  newStatPanel(queries, legends='', unit='percentunit', decimals=1, thresholds=[], instant=false, novalue='')::
    super.queryPanel(queries, legends) + {
      type: 'stat',
      targets: [
        target {
          instant: instant,
          interval: '',

          // Reset defaults from queryPanel().
          format: null,
          intervalFactor: null,
          step: null,
        }
        for target in super.targets
      ],
      fieldConfig: {
        defaults: {
          color: { mode: 'thresholds' },
          decimals: decimals,
          thresholds: {
            mode: 'absolute',
            steps: thresholds,
          },
          noValue: novalue,
          unit: unit,
        },
        overrides: [],
      },
    },

  barGauge(queries, legends='', thresholds=[], unit='short', min=null, max=null)::
    super.queryPanel(queries, legends) + {
      type: 'bargauge',
      targets: [
        target {
          // Reset defaults from queryPanel().
          format: null,
          intervalFactor: null,
          step: null,
        }
        for target in super.targets
      ],
      fieldConfig: {
        defaults: {
          color: { mode: 'thresholds' },
          mappings: [],
          max: max,
          min: min,
          thresholds: {
            mode: 'absolute',
            steps: thresholds,
          },
          unit: unit,
        },
      },
      options: {
        displayMode: 'basic',
        orientation: 'horizontal',
        reduceOptions: {
          calcs: ['lastNotNull'],
          fields: '',
          values: false,
        },
      },
    },

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

  getObjectStoreRows(title, component):: [
    super.row(title)
    .addPanel(
      $.panel('Operations / sec') +
      $.queryPanel('sum by(operation) (rate(thanos_objstore_bucket_operations_total{%s,component="%s"}[$__rate_interval]))' % [$.namespaceMatcher(), component], '{{operation}}') +
      $.stack +
      { yaxes: $.yaxes('rps') },
    )
    .addPanel(
      $.panel('Error rate') +
      $.queryPanel('sum by(operation) (rate(thanos_objstore_bucket_operation_failures_total{%s,component="%s"}[$__rate_interval])) / sum by(operation) (rate(thanos_objstore_bucket_operations_total{%s,component="%s"}[$__rate_interval]))' % [$.namespaceMatcher(), component, $.namespaceMatcher(), component], '{{operation}}') +
      { yaxes: $.yaxes('percentunit') },
    )
    .addPanel(
      $.panel('Latency of op: Attributes') +
      $.latencyPanel('thanos_objstore_bucket_operation_duration_seconds', '{%s,component="%s",operation="attributes"}' % [$.namespaceMatcher(), component]),
    )
    .addPanel(
      $.panel('Latency of op: Exists') +
      $.latencyPanel('thanos_objstore_bucket_operation_duration_seconds', '{%s,component="%s",operation="exists"}' % [$.namespaceMatcher(), component]),
    ),
    $.row('')
    .addPanel(
      $.panel('Latency of op: Get') +
      $.latencyPanel('thanos_objstore_bucket_operation_duration_seconds', '{%s,component="%s",operation="get"}' % [$.namespaceMatcher(), component]),
    )
    .addPanel(
      $.panel('Latency of op: GetRange') +
      $.latencyPanel('thanos_objstore_bucket_operation_duration_seconds', '{%s,component="%s",operation="get_range"}' % [$.namespaceMatcher(), component]),
    )
    .addPanel(
      $.panel('Latency of op: Upload') +
      $.latencyPanel('thanos_objstore_bucket_operation_duration_seconds', '{%s,component="%s",operation="upload"}' % [$.namespaceMatcher(), component]),
    )
    .addPanel(
      $.panel('Latency of op: Delete') +
      $.latencyPanel('thanos_objstore_bucket_operation_duration_seconds', '{%s,component="%s",operation="delete"}' % [$.namespaceMatcher(), component]),
    ),
  ],

  thanosMemcachedCache(title, jobName, component, cacheName)::
    local config = {
      jobMatcher: $.jobMatcher(jobName),
      component: component,
      cacheName: cacheName,
    };
    super.row(title)
    .addPanel(
      $.panel('Requests / sec') +
      $.queryPanel(
        |||
          sum by(operation) (
            rate(
              thanos_memcached_operations_total{
                %(jobMatcher)s,
                component="%(component)s",
                name="%(cacheName)s"
              }[$__rate_interval]
            )
          )
        ||| % config,
        '{{operation}}'
      ) +
      $.stack +
      { yaxes: $.yaxes('ops') }
    )
    .addPanel(
      $.panel('Latency (getmulti)') +
      $.latencyPanel(
        'thanos_memcached_operation_duration_seconds',
        |||
          {
            %(jobMatcher)s,
            operation="getmulti",
            component="%(component)s",
            name="%(cacheName)s"
          }
        ||| % config
      )
    )
    .addPanel(
      $.panel('Hit ratio') +
      $.queryPanel(
        |||
          sum(
            rate(
              thanos_cache_memcached_hits_total{
                %(jobMatcher)s,
                component="%(component)s",
                name="%(cacheName)s"
              }[$__rate_interval]
            )
          )
          /
          sum(
            rate(
              thanos_cache_memcached_requests_total{
                %(jobMatcher)s,
                component="%(component)s",
                name="%(cacheName)s"
              }[$__rate_interval]
            )
          )
        ||| % config,
        'items'
      ) +
      { yaxes: $.yaxes('percentunit') }
    ),

  filterNodeDiskContainer(containerName)::
    |||
      ignoring(%s) group_right() (
        label_replace(
          count by(
            %s,
            %s,
            device
          )
          (
            container_fs_writes_bytes_total{
              %s,
              container="%s",
              device!~".*sda.*"
            }
          ),
          "device",
          "$1",
          "device",
          "/dev/(.*)"
        ) * 0
      )
    ||| % [
      $._config.per_instance_label,
      $._config.per_node_label,
      $._config.per_instance_label,
      $.namespaceMatcher(),
      containerName,
    ],

  filterKedaMetricByHPA(query, hpa_name)::
    |||
      %(query)s +
      on(metric) group_left
      label_replace(
          kube_horizontalpodautoscaler_spec_target_metric{%(namespace)s, horizontalpodautoscaler="%(hpa_name)s"}
          * 0, "metric", "$1", "metric_name", "(.+)"
      )
    ||| % {
      query: query,
      hpa_name: hpa_name,
      namespace: $.namespaceMatcher(),
    },

  // panelAxisPlacement allows to place a series on the right axis.
  // This function supports the old Graph panel.
  panelAxisPlacement(seriesName, placement)::
    if placement != 'right' then {} else {
      seriesOverrides+: [
        {
          alias: seriesName,
          yaxis: 2,
        },
      ],
      // Ensure all Y-axis are displayed (default is that right axis is hidden).
      yaxes: std.map(function(entry) entry {
        show: true,
      }, super.yaxes),
    },

  panelDescription(title, description):: {
    description: |||
      ### %s
      %s
    ||| % [title, description],
  },

  // Panel query override functions
  overrideFieldByName(fieldName, overrideProperties):: {
    matcher: {
      id: 'byName',
      options: fieldName,
    },
    properties: overrideProperties,
  },

  overrideProperty(id, value):: { id: id, value: value },

  // Panel query value mapping functions
  mappingRange(from, to, result):: {
    type: 'range',
    options: {
      from: from,
      to: to,
      result: result,
    },
  },

  mappingSpecial(match, result):: {
    type: 'special',
    options: {
      match: match,
      result: result,
    },
  },

  // Panel query transformation functions

  transformation(id, options={}):: { id: id, options: options },

  transformationCalculateField(alias, left, operator, right, replaceFields=false)::
    $.transformation('calculateField', {
      alias: alias,
      binary: {
        left: left,
        operator: operator,
        right: right,
      },
      mode: 'binary',
      replaceFields: replaceFields,
    }),

}
