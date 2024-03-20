local utils = import 'mixin-utils/utils.libsonnet';

(import 'grafana-builder/grafana.libsonnet') {
  local resourceRequestColor = '#FFC000',
  local resourceLimitColor = '#E02F44',
  local successColor = '#7EB26D',
  local warningColor = '#EAB839',
  local errorColor = '#E24D42',

  // Colors palette picked from Grafana UI, excluding red-ish colors which we want to keep reserved for errors / failures.
  local nonErrorColorsPalette = ['#429D48', '#F1C731', '#2A66CF', '#9E44C1', '#FFAB57', '#C79424', '#84D586', '#A1C4FC', '#C788DE'],

  local resourceRequestStyle = $.overrideFieldByName('request', [
    $.overrideProperty('color', { mode: 'fixed', fixedColor: resourceRequestColor }),
    $.overrideProperty('custom.fillOpacity', 0),
    $.overrideProperty('custom.lineStyle', { fill: 'dash' }),
  ]),
  local resourceLimitStyle = $.overrideFieldByName('limit', [
    $.overrideProperty('color', { mode: 'fixed', fixedColor: resourceLimitColor }),
    $.overrideProperty('custom.fillOpacity', 0),
    $.overrideProperty('custom.lineStyle', { fill: 'dash' }),
  ]),

  local sortAscending = 1,

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
      graphTooltip: $._config.graph_tooltip,
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
          then d.addMultiTemplate('job', $._config.dashboard_variables.job_query, $._config.per_job_label, sort=sortAscending)
          else d
               .addMultiTemplate('cluster', $._config.dashboard_variables.cluster_query, '%s' % $._config.per_cluster_label, sort=sortAscending)
               .addMultiTemplate('namespace', $._config.dashboard_variables.namespace_query, '%s' % $._config.per_namespace_label, sort=sortAscending)
        else
          if $._config.singleBinary
          then d.addTemplate('job', $._config.dashboard_variables.job_query, $._config.per_job_label, sort=sortAscending)
          else d
               .addTemplate('cluster', $._config.dashboard_variables.cluster_query, '%s' % $._config.per_cluster_label, allValue='.*', includeAll=true, sort=sortAscending)
               .addTemplate('namespace', $._config.dashboard_variables.namespace_query, '%s' % $._config.per_namespace_label, sort=sortAscending),

      addActiveUserSelectorTemplates()::
        self.addTemplate('user', 'cortex_ingester_active_series{%s=~"$cluster", %s=~"$namespace"}' % [$._config.per_cluster_label, $._config.per_namespace_label], 'user', sort=sortAscending),

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

  // Returns the URL of a given dashboard, keeping the current time range and variables.
  dashboardURL(filename)::
    // Grafana uses a <base> HTML set to the path defined in GF_SERVER_ROOT_URL.
    // This means that if we create relative links (starting with ".") the browser
    // will append the base to it, effectively honoring the GF_SERVER_ROOT_URL.
    //
    // IMPORTANT: due to an issue with Grafana, this URL works only when opened in a
    // new browser tab (e.g. link with target="_blank").
    './d/%(uid)s/%(filename)s?${__url_time_range}&${__all_variables}' % {
      uid: std.md5(filename),
      filename: std.strReplace(filename, '.json', ''),
    },

  // The mixin allow specialism of the job selector depending on if its a single binary
  // deployment or a namespaced one.
  jobMatcher(job)::
    if $._config.singleBinary
    then '%s=~"$job"' % $._config.per_job_label
    else '%s=~"$cluster", %s=~"%s(%s)"' % [$._config.per_cluster_label, $._config.per_job_label, $._config.job_prefix, formatJobForQuery(job)],

  local formatJobForQuery(job) =
    if std.isArray(job) then '(%s)' % std.join('|', job)
    else if std.isString(job) then job
    else error 'expected job "%s" to be a string or an array, but it is type "%s"' % [job, std.type(job)],

  namespaceMatcher()::
    if $._config.singleBinary
    then '%s=~"$job"' % $._config.per_job_label
    else '%s=~"$cluster", %s=~"$namespace"' % [$._config.per_cluster_label, $._config.per_namespace_label],

  jobSelector(job)::
    if $._config.singleBinary
    then [utils.selector.noop('%s' % $._config.per_cluster_label), utils.selector.re($._config.per_job_label, '$job')]
    else [utils.selector.re('%s' % $._config.per_cluster_label, '$cluster'), utils.selector.re($._config.per_job_label, '($namespace)/(%s)' % formatJobForQuery(job))],

  recordingRulePrefix(selectors)::
    std.join('_', [matcher.label for matcher in selectors]),

  panel(title)::
    super.panel(title) + {
      tooltip+: {
        shared: false,
        sort: 0,
      },
    },

  timeseriesPanel(title)::
    super.timeseriesPanel(title) + {
      fieldConfig+: {
        defaults+: {
          unit: 'short',
          min: 0,
        },
      },
    },

  qpsPanel(selector, statusLabelName='status_code')::
    super.qpsPanel(selector, statusLabelName) +
    $.aliasColors({
      '1xx': warningColor,
      '2xx': successColor,
      '3xx': '#6ED0E0',
      '4xx': '#EF843C',
      '5xx': errorColor,
      OK: successColor,
      success: successColor,
      'error': errorColor,
      cancel: '#A9A9A9',
    }) + {
      fieldConfig+: {
        defaults+: { unit: 'reqps' },
      },
    },

  latencyPanel(metricName, selector, multiplier='1e3')::
    super.latencyPanel(metricName, selector, multiplier) + {
      fieldConfig+: {
        defaults+: { unit: 'ms' },
      },
    },

  // hiddenLegendQueryPanel adds on to 'timeseriesPanel', not the deprecated 'panel'.
  // It is a standard query panel designed to handle a large number of series.  it hides the legend, doesn't fill the series and
  // shows all values on tooltip, descending. Also turns on exemplars, unless 4th parameter is false.
  hiddenLegendQueryPanel(queries, legends, legendLink=null, exemplars=true)::
    $.queryPanel(queries, legends, legendLink) +
    {
      options: {
        legend+: {
          showLegend: false,
          // Work round Grafana turning showLegend back on when we have
          // schemaVersion<37. https://github.com/grafana/grafana/issues/54472
          displayMode: 'hidden',
        },
        tooltip+: {
          mode: 'multi',
          sort: 'desc',
        },
      },
      fieldConfig+: {
        defaults+: {
          unit: 's',
          custom+: {
            fillOpacity: 0,
          },
        },
      },
    } + {
      targets: [
        target {
          exemplar: exemplars,
        }
        for target in super.targets
      ],
    },

  // Creates a panel like queryPanel() but if the legend contains only 1 entry,
  // than it configures the series alias color to the one used to display failures.
  failurePanel(queries, legends, legendLink=null)::
    $.queryPanel(queries, legends, legendLink) +
    // Set the failure color only if there's just 1 legend and it doesn't contain any placeholder.
    $.aliasColors(
      if (std.type(legends) == 'string' && std.length(std.findSubstr('{', legends[0])) == 0) then {
        [legends]: errorColor,
      } else {}
    ),

  successFailurePanel(successMetric, failureMetric)::
    $.queryPanel([successMetric, failureMetric], ['successful', 'failed']) +
    $.aliasColors({
      successful: successColor,
      failed: errorColor,
    }),

  // successFailureCustomPanel is like successFailurePanel() but allows to customize the legends
  // and have additional queries. The success and failure queries MUST be the first and second
  // queries respectively.
  successFailureCustomPanel(queries, legends)::
    $.queryPanel(queries, legends) +
    $.aliasColors({
      [legends[0]]: successColor,
      [legends[1]]: errorColor,
    }),

  // Displays started, completed and failed rate.
  startedCompletedFailedPanel(title, startedMetric, completedMetric, failedMetric)::
    $.timeseriesPanel(title) +
    $.queryPanel([startedMetric, completedMetric, failedMetric], ['started', 'completed', 'failed']) +
    $.stack +
    $.aliasColors({
      started: '#34CCEB',
      completed: successColor,
      failed: errorColor,
    }),

  resourceUtilizationAndLimitLegend(resourceName)::
    if $._config.deployment_type == 'kubernetes'
    then [resourceName, 'limit', 'request']
    // limit and request does not makes sense when running on baremetal
    else [resourceName],

  // The provided instanceName should be a regexp from $._config.instance_names, while
  // the provided containerName should be a regexp from $._config.container_names.
  resourceUtilizationQuery(metric, instanceName, containerName)::
    $._config.resources_panel_queries[$._config.deployment_type]['%s_usage' % metric] % {
      instanceLabel: $._config.per_instance_label,
      namespace: $.namespaceMatcher(),
      instanceName: instanceName,
      containerName: containerName,
    },

  // The provided instanceName should be a regexp from $._config.instance_names, while
  // the provided containerName should be a regexp from $._config.container_names.
  resourceUtilizationAndLimitQueries(metric, instanceName, containerName)::
    if $._config.deployment_type == 'kubernetes'
    then [
      $.resourceUtilizationQuery(metric, instanceName, containerName),
      $._config.resources_panel_queries[$._config.deployment_type]['%s_limit' % metric] % {
        namespace: $.namespaceMatcher(),
        containerName: containerName,
      },
      $._config.resources_panel_queries[$._config.deployment_type]['%s_request' % metric] % {
        namespace: $.namespaceMatcher(),
        containerName: containerName,
      },
    ]
    else [
      $.resourceUtilizationQuery(metric, instanceName, containerName),
    ],

  // The provided instanceName should be a regexp from $._config.instance_names, while
  // the provided containerName should be a regexp from $._config.container_names.
  containerCPUUsagePanel(instanceName, containerName)::
    $.timeseriesPanel('CPU') +
    $.queryPanel($.resourceUtilizationAndLimitQueries('cpu', instanceName, containerName), $.resourceUtilizationAndLimitLegend('{{%s}}' % $._config.per_instance_label)) +
    {
      fieldConfig+: {
        overrides+: [
          resourceRequestStyle,
          resourceLimitStyle,
        ],
        defaults+: {
          unit: 'short',
          custom+: {
            fillOpacity: 0,
          },
        },
      },
      options+: {
        tooltip: {
          mode: 'multi',
          sort: 'desc',
        },
      },
    },

  // The provided componentName should be the name of a component among the ones defined in $._config.instance_names.
  containerCPUUsagePanelByComponent(componentName)::
    $.containerCPUUsagePanel($._config.instance_names[componentName], $._config.container_names[componentName]),

  // The provided instanceName should be a regexp from $._config.instance_names, while
  // the provided containerName should be a regexp from $._config.container_names.
  containerMemoryWorkingSetPanel(instanceName, containerName)::
    $.timeseriesPanel('Memory (workingset)') +
    $.queryPanel($.resourceUtilizationAndLimitQueries('memory_working', instanceName, containerName), $.resourceUtilizationAndLimitLegend('{{%s}}' % $._config.per_instance_label)) +
    {
      fieldConfig+: {
        overrides+: [
          resourceRequestStyle,
          resourceLimitStyle,
        ],
        defaults+: {
          unit: 'bytes',
          custom+: {
            fillOpacity: 0,
          },
        },
      },
      options+: {
        tooltip: {
          mode: 'multi',
          sort: 'desc',
        },
      },
    },

  // The provided componentName should be the name of a component among the ones defined in $._config.instance_names.
  containerMemoryWorkingSetPanelByComponent(componentName)::
    $.containerMemoryWorkingSetPanel($._config.instance_names[componentName], $._config.container_names[componentName]),

  // The provided instanceName should be a regexp from $._config.instance_names, while
  // the provided containerName should be a regexp from $._config.container_names.
  containerMemoryRSSPanel(instanceName, containerName)::
    $.timeseriesPanel('Memory (RSS)') +
    $.queryPanel($.resourceUtilizationAndLimitQueries('memory_rss', instanceName, containerName), $.resourceUtilizationAndLimitLegend('{{%s}}' % $._config.per_instance_label)) +
    {
      fieldConfig+: {
        overrides+: [
          resourceRequestStyle,
          resourceLimitStyle,
        ],
        defaults+: {
          unit: 'bytes',
          custom+: {
            fillOpacity: 0,
          },
        },
      },
      options+: {
        tooltip: {
          mode: 'multi',
          sort: 'desc',
        },
      },
    },

  // The provided componentName should be the name of a component among the ones defined in $._config.instance_names.
  containerMemoryRSSPanelByComponent(componentName)::
    $.containerMemoryRSSPanel($._config.instance_names[componentName], $._config.container_names[componentName]),

  // The provided instanceName should be a regexp from $._config.instance_names, while
  // the provided containerName should be a regexp from $._config.container_names.
  containerGoHeapInUsePanel(instanceName, containerName)::
    $.timeseriesPanel('Memory (go heap inuse)') +
    $.queryPanel($.resourceUtilizationQuery('memory_go_heap', instanceName, containerName), '{{%s}}' % $._config.per_instance_label) +
    {
      fieldConfig+: {
        defaults+: {
          unit: 'bytes',
          custom+: {
            fillOpacity: 0,
          },
        },
      },
      options+: {
        tooltip: {
          mode: 'multi',
          sort: 'desc',
        },
      },
    },

  // The provided componentName should be the name of a component among the ones defined in $._config.instance_names.
  containerGoHeapInUsePanelByComponent(componentName)::
    $.containerGoHeapInUsePanel($._config.instance_names[componentName], $._config.container_names[componentName]),

  containerNetworkBytesPanel(title, metric, instanceName)::
    $.timeseriesPanel(title) +
    $.queryPanel(
      $._config.resources_panel_queries[$._config.deployment_type][metric] % {
        namespaceMatcher: $.namespaceMatcher(),
        instanceLabel: $._config.per_instance_label,
        instanceName: instanceName,
      }, '{{%s}}' % $._config.per_instance_label
    ) +
    $.stack +
    { fieldConfig+: { defaults+: { unit: 'Bps' } } },

  // The provided componentName should be the name of a component among the ones defined in $._config.instance_names.
  containerNetworkReceiveBytesPanelByComponent(componentName)::
    $.containerNetworkBytesPanel('Receive bandwidth', 'network_receive_bytes', $._config.instance_names[componentName]),

  // The provided componentName should be the name of a component among the ones defined in $._config.instance_names.
  containerNetworkTransmitBytesPanelByComponent(componentName)::
    $.containerNetworkBytesPanel('Transmit bandwidth', 'network_transmit_bytes', $._config.instance_names[componentName]),

  // The provided instanceName should be a regexp from $._config.instance_names, while
  // the provided containerName should be a regexp from $._config.container_names.
  containerDiskWritesPanel(instanceName, containerName)::
    $.timeseriesPanel('Disk writes') +
    $.queryPanel(
      $._config.resources_panel_queries[$._config.deployment_type].disk_writes % {
        namespace: $.namespaceMatcher(),
        nodeLabel: $._config.per_node_label,
        instanceLabel: $._config.per_instance_label,
        instanceName: instanceName,
        filterNodeDiskContainer: $.filterNodeDiskContainer(containerName),
      },
      '{{%s}} - {{device}}' % $._config.per_instance_label
    ) +
    $.stack +
    { fieldConfig+: { defaults+: { unit: 'Bps' } } },

  // The provided componentName should be the name of a component among the ones defined in $._config.instance_names.
  containerDiskWritesPanelByComponent(componentName)::
    $.containerDiskWritesPanel($._config.instance_names[componentName], $._config.container_names[componentName]),

  // The provided instanceName should be a regexp from $._config.instance_names, while
  // the provided containerName should be a regexp from $._config.container_names.
  containerDiskReadsPanel(instanceName, containerName)::
    $.timeseriesPanel('Disk reads') +
    $.queryPanel(
      $._config.resources_panel_queries[$._config.deployment_type].disk_reads % {
        namespace: $.namespaceMatcher(),
        nodeLabel: $._config.per_node_label,
        instanceLabel: $._config.per_instance_label,
        filterNodeDiskContainer: $.filterNodeDiskContainer(containerName),
        instanceName: instanceName,
      },
      '{{%s}} - {{device}}' % $._config.per_instance_label
    ) +
    $.stack +
    { fieldConfig+: { defaults+: { unit: 'Bps' } } },

  // The provided componentName should be the name of a component among the ones defined in $._config.instance_names.
  containerDiskReadsPanelByComponent(componentName)::
    $.containerDiskReadsPanel($._config.instance_names[componentName], $._config.container_names[componentName]),

  // The provided instanceName should be a regexp from $._config.instance_names, while
  // the provided containerName should be a regexp from $._config.container_names.
  containerDiskSpaceUtilizationPanel(instanceName, containerName)::
    local label = if $._config.deployment_type == 'kubernetes' then '{{persistentvolumeclaim}}' else '{{instance}}';
    $.timeseriesPanel('Disk space utilization') +
    $.queryPanel(
      $._config.resources_panel_queries[$._config.deployment_type].disk_utilization % {
        namespaceMatcher: $.namespaceMatcher(),
        containerMatcher: $.containerLabelNameMatcher(containerName),
        instanceLabel: $._config.per_instance_label,
        instanceName: instanceName,
        instanceDataDir: $._config.instance_data_mountpoint,
      }, label
    ) +
    {
      fieldConfig+: {
        defaults+: { unit: 'percentunit' },
        custom+: {
          fillOpacity: 0,
        },
      },
    },

  // The provided componentName should be the name of a component among the ones defined in $._config.instance_names.
  containerDiskSpaceUtilizationPanelByComponent(componentName)::
    $.containerDiskSpaceUtilizationPanel($._config.instance_names[componentName], $._config.container_names[componentName]),

  // The provided containerName should be a regexp from $._config.container_names.
  containerLabelNameMatcher(containerName)::
    // Check only the prefix so that a multi-zone deployment matches too.
    'label_name=~"(%s).*"' % containerName,

  // The provided componentName should be the name of a component among the ones defined in $._config.instance_names.
  containerNetworkingRowByComponent(title, componentName)::
    // Match series using namespace + instance instead of the job so that we can
    // select only specific deployments (e.g. "distributor in microservices mode").
    local vars = $._config {
      instanceLabel: $._config.per_instance_label,
      instanceName: $._config.instance_names[componentName],
      namespaceMatcher: $.namespaceMatcher(),
    };

    super.row(title)
    .addPanel($.containerNetworkReceiveBytesPanelByComponent(componentName))
    .addPanel($.containerNetworkTransmitBytesPanelByComponent(componentName))
    .addPanel(
      $.timeseriesPanel('Inflight requests (per pod)') +
      $.queryPanel([
        'avg(cortex_inflight_requests{%(namespaceMatcher)s,%(instanceLabel)s=~"%(instanceName)s"})' % vars,
        'max(cortex_inflight_requests{%(namespaceMatcher)s,%(instanceLabel)s=~"%(instanceName)s"})' % vars,
      ], ['avg', 'highest']) +
      {
        fieldConfig+: {
          defaults+: { unit: 'short' },
          custom+: {
            fillOpacity: 0,
          },
        },
      },
    )
    .addPanel(
      $.timeseriesPanel('TCP connections (per pod)') +
      $.queryPanel([
        'avg(sum by(%(per_instance_label)s) (cortex_tcp_connections{%(namespaceMatcher)s,%(instanceLabel)s=~"%(instanceName)s"}))' % vars,
        'max(sum by(%(per_instance_label)s) (cortex_tcp_connections{%(namespaceMatcher)s,%(instanceLabel)s=~"%(instanceName)s"}))' % vars,
        'min(cortex_tcp_connections_limit{%(namespaceMatcher)s,%(instanceLabel)s=~"%(instanceName)s"})' % vars,
      ], ['avg', 'highest', 'limit']) +
      {
        fieldConfig+: {
          defaults+: { unit: 'short' },
          custom+: {
            fillOpacity: 0,
          },
        },
      },
    ),

  kvStoreRow(title, jobName, kvName)::
    super.row(title)
    .addPanel(
      $.timeseriesPanel('Requests / sec') +
      $.qpsPanel('cortex_kv_request_duration_seconds_count{%s, kv_name=~"%s"}' % [$.jobMatcher($._config.job_names[jobName]), kvName])
    )
    .addPanel(
      $.timeseriesPanel('Latency') +
      $.latencyPanel('cortex_kv_request_duration_seconds', '{%s, kv_name=~"%s"}' % [$.jobMatcher($._config.job_names[jobName]), kvName])
    ),

  cpuAndMemoryBasedAutoScalingRow(componentTitle)::
    local component = std.asciiLower(componentTitle);
    local field = std.strReplace(component, '-', '_');
    super.row('%s - autoscaling' % [componentTitle])
    .addPanel(
      local title = 'Replicas';
      $.timeseriesPanel(title) +
      $.queryPanel(
        [
          |||
            max by (scaletargetref_name) (
              kube_horizontalpodautoscaler_spec_max_replicas{%(namespace_matcher)s, horizontalpodautoscaler=~"%(hpa_name)s"}
              # Add the scaletargetref_name label for readability
              + on (%(cluster_labels)s, horizontalpodautoscaler) group_left (scaletargetref_name)
                0*kube_horizontalpodautoscaler_info{%(namespace_matcher)s, horizontalpodautoscaler=~"%(hpa_name)s"}
            )
          ||| % {
            namespace_matcher: $.namespaceMatcher(),
            hpa_name: $._config.autoscaling[field].hpa_name,
            cluster_labels: std.join(', ', $._config.cluster_labels),
          },
          |||
            max by (scaletargetref_name) (
              kube_horizontalpodautoscaler_status_current_replicas{%(namespace_matcher)s, horizontalpodautoscaler=~"%(hpa_name)s"}
              # HPA doesn't go to 0 replicas, so we multiply by 0 if the HPA is not active
              * on (%(cluster_labels)s, horizontalpodautoscaler)
                kube_horizontalpodautoscaler_status_condition{%(namespace_matcher)s, horizontalpodautoscaler=~"%(hpa_name)s", condition="ScalingActive", status="true"}
              # Add the scaletargetref_name label for readability
              + on (%(cluster_labels)s, horizontalpodautoscaler) group_left (scaletargetref_name)
                0*kube_horizontalpodautoscaler_info{%(namespace_matcher)s, horizontalpodautoscaler=~"%(hpa_name)s"}
            )
          ||| % {
            namespace_matcher: $.namespaceMatcher(),
            hpa_name: $._config.autoscaling[field].hpa_name,
            cluster_labels: std.join(', ', $._config.cluster_labels),
          },
          |||
            max by (scaletargetref_name) (
              kube_horizontalpodautoscaler_spec_min_replicas{%(namespace_matcher)s, horizontalpodautoscaler=~"%(hpa_name)s"}
              # Add the scaletargetref_name label for readability
              + on (%(cluster_labels)s, horizontalpodautoscaler) group_left (scaletargetref_name)
                0*kube_horizontalpodautoscaler_info{%(namespace_matcher)s, horizontalpodautoscaler=~"%(hpa_name)s"}
            )
          ||| % {
            namespace_matcher: $.namespaceMatcher(),
            hpa_name: $._config.autoscaling[field].hpa_name,
            cluster_labels: std.join(', ', $._config.cluster_labels),
          },
        ],
        [
          'Max {{ scaletargetref_name }}',
          'Current {{ scaletargetref_name }}',
          'Min {{ scaletargetref_name }}',
        ],
      ) +
      $.panelDescription(
        title,
        |||
          The maximum and current number of %s replicas.
          Note: The current number of replicas can still show 1 replica even when scaled to 0.
          Because HPA never reports 0 replicas, the query will report 0 only if the HPA is not active.
        ||| % [component]
      ) +
      {
        fieldConfig+: {
          overrides: [
            $.overrideField('byRegexp', '/Max .+/', [
              $.overrideProperty('custom.fillOpacity', 0),
              $.overrideProperty('custom.lineStyle', { fill: 'dash' }),
            ]),
            $.overrideField('byRegexp', '/Current .+/', [
              $.overrideProperty('custom.fillOpacity', 0),
            ]),
            $.overrideField('byRegexp', '/Min .+/', [
              $.overrideProperty('custom.fillOpacity', 0),
              $.overrideProperty('custom.lineStyle', { fill: 'dash' }),
            ]),
          ],
        },
      },
    )
    .addPanel(
      local title = 'Scaling metric (CPU): Desired replicas';
      $.timeseriesPanel(title) +
      $.queryPanel(
        [
          |||
            sum by (scaler) (
              label_replace(
                keda_scaler_metrics_value{%(cluster_label)s=~"$cluster", exported_namespace=~"$namespace", scaler=~".*cpu.*"},
                "namespace", "$1", "exported_namespace", "(.*)"
              )
              /
              on(%(aggregation_labels)s, scaledObject, metric) group_left label_replace(
                label_replace(
                  kube_horizontalpodautoscaler_spec_target_metric{%(namespace)s, horizontalpodautoscaler=~"%(hpa_name)s"},
                  "metric", "$1", "metric_name", "(.+)"
                ),
                "scaledObject", "$1", "horizontalpodautoscaler", "%(hpa_prefix)s(.*)"
              )
            )
          ||| % {
            aggregation_labels: $._config.alert_aggregation_labels,
            cluster_label: $._config.per_cluster_label,
            hpa_prefix: $._config.autoscaling_hpa_prefix,
            hpa_name: $._config.autoscaling[field].hpa_name,
            namespace: $.namespaceMatcher(),
          },
        ], [
          '{{ scaler }}',
        ]
      ) +
      $.panelDescription(
        title,
        |||
          This panel shows the scaling metric exposed by KEDA divided by the target/threshold used.
          It should represent the desired number of replicas, ignoring the min/max constraints applied later.
        |||
      ),
    )
    .addPanel(
      local title = 'Scaling metric (memory): Desired replicas';
      $.timeseriesPanel(title) +
      $.queryPanel(
        [
          |||
            sum by (scaler) (
              label_replace(
                keda_scaler_metrics_value{%(cluster_label)s=~"$cluster", exported_namespace=~"$namespace", scaler=~".*memory.*"},
                "namespace", "$1", "exported_namespace", "(.*)"
              )
              /
              on(%(aggregation_labels)s, scaledObject, metric) group_left label_replace(
                label_replace(
                  kube_horizontalpodautoscaler_spec_target_metric{%(namespace)s, horizontalpodautoscaler=~"%(hpa_name)s"},
                  "metric", "$1", "metric_name", "(.+)"
                ),
                "scaledObject", "$1", "horizontalpodautoscaler", "%(hpa_prefix)s(.*)"
              )
            )
          ||| % {
            aggregation_labels: $._config.alert_aggregation_labels,
            cluster_label: $._config.per_cluster_label,
            hpa_prefix: $._config.autoscaling_hpa_prefix,
            hpa_name: $._config.autoscaling[field].hpa_name,
            namespace: $.namespaceMatcher(),
          },
        ], [
          '{{ scaler }}',
        ]
      ) +
      $.panelDescription(
        title,
        |||
          This panel shows the scaling metric exposed by KEDA divided by the target/threshold used.
          It should represent the desired number of replicas, ignoring the min/max constraints applied later.
        |||
      ),
    )
    .addPanel(
      local title = 'Autoscaler failures rate';
      $.timeseriesPanel(title) +
      $.queryPanel(
        $.filterKedaScalerErrorsByHPA($._config.autoscaling[field].hpa_name),
        '{{scaler}} failures'
      ) +
      $.panelDescription(
        title,
        |||
          The rate of failures in the KEDA custom metrics API server. Whenever an error occurs, the KEDA custom
          metrics server is unable to query the scaling metric from Prometheus so the autoscaler woudln't work properly.
        |||
      ),
    ),

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

  barChart(queries, legends='', thresholds=[], unit='short', min=null, max=null)::
    super.queryPanel(queries, legends) + {
      type: 'barchart',
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
          custom: {
            lineWidth: 1,
            fillOpacity: 80,
            gradientMode: 'none',
            axisPlacement: 'auto',
            axisLabel: '',
            axisColorMode: 'text',
            scaleDistribution: {
              type: 'linear',
            },
            axisCenteredZero: false,
            hideFrom: {
              tooltip: false,
              viz: false,
              legend: false,
            },
            thresholdsStyle: {
              mode: 'off',
            },
          },
        },
      },
      options: {
        orientation: 'auto',
        xTickLabelRotation: 0,
        xTickLabelSpacing: 0,
        showValue: 'auto',
        stacking: 'none',
        groupWidth: 0.7,
        barWidth: 0.97,
        barRadius: 0,
        fullHighlight: false,
        tooltip: {
          mode: 'single',
          sort: 'none',
        },
        legend: {
          showLegend: true,
          displayMode: 'list',
          placement: 'bottom',
          calcs: [],
        },
      },
    },

  // Enables stacking of timeseries on top of each.
  // It overrites the "stack" mixin from jsonnet-lib/grafana-builder, to make it compatible with timeseriesPanel.
  stack:: {
    fieldConfig+: {
      defaults+: {
        custom+: {
          lineWidth: 0,
          fillOpacity: 100,
          stacking+: {
            mode: 'normal',
          },
        },
      },
    },
  },

  // Switches a panel from lines (default) to bars.
  bars:: {
    fieldConfig+: {
      defaults+: {
        custom+: {
          drawStyle: 'bars',
        },
      },
    },
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

  alertListPanel(title, nameFilter='', labelsFilter=''):: {
    type: 'alertlist',
    title: title,
    options: {
      maxItems: 100,
      sortOrder: 3,  // Sort by importance.
      dashboardAlerts: false,
      alertName: nameFilter,
      alertInstanceLabelFilter: labelsFilter,
      stateFilter: {
        firing: true,
        pending: false,
        noData: false,
        normal: false,
        'error': true,
      },
    },
  },

  stateTimelinePanel(title, queries, legends):: {
    local queriesArray = if std.type(queries) == 'string' then [queries] else queries,
    local legendsArray = if std.type(legends) == 'string' then [legends] else legends,

    local queriesAndLegends =
      if std.length(legendsArray) == std.length(queriesArray) then
        std.makeArray(std.length(queriesArray), function(x) { query: queriesArray[x], legend: legendsArray[x] })
      else
        error 'length of queries is not equal to length of legends',

    type: 'state-timeline',
    title: title,
    targets: [
      {
        datasource: { uid: '$datasource' },
        expr: entry.query,
        legendFormat: entry.legend,
        range: true,
        instant: false,
        exemplar: false,
      }
      for entry in queriesAndLegends
    ],
    options: {
      // Never show the value over the bar in order to have a clean UI.
      showValue: 'never',
    },
    fieldConfig: {
      defaults: {
        color: {
          mode: 'thresholds',
        },
        thresholds: {
          mode: 'absolute',
          steps: [
            { color: successColor, value: null },
            { color: warningColor, value: 0.01 },  // 1%
            { color: errorColor, value: 0.05 },  // 5%
          ],
        },
      },
    },
  },

  getObjectStoreRows(title, component):: [
    super.row(title)
    .addPanel(
      $.timeseriesPanel('Operations / sec') +
      $.queryPanel('sum by(operation) (rate(thanos_objstore_bucket_operations_total{%s,component="%s"}[$__rate_interval]))' % [$.namespaceMatcher(), component], '{{operation}}') +
      $.stack +
      { fieldConfig+: { defaults+: { unit: 'reqps' } } }
    )
    .addPanel(
      $.timeseriesPanel('Error rate') +
      $.queryPanel('sum by(operation) (rate(thanos_objstore_bucket_operation_failures_total{%s,component="%s"}[$__rate_interval])) / sum by(operation) (rate(thanos_objstore_bucket_operations_total{%s,component="%s"}[$__rate_interval])) >= 0' % [$.namespaceMatcher(), component, $.namespaceMatcher(), component], '{{operation}}') +
      { fieldConfig: { defaults: { noValue: '0', unit: 'percentunit', min: 0, max: 1 } } }
    )
    .addPanel(
      $.timeseriesPanel('Latency of op: Attributes') +
      $.latencyPanel('thanos_objstore_bucket_operation_duration_seconds', '{%s,component="%s",operation="attributes"}' % [$.namespaceMatcher(), component]),
    )
    .addPanel(
      $.timeseriesPanel('Latency of op: Exists') +
      $.latencyPanel('thanos_objstore_bucket_operation_duration_seconds', '{%s,component="%s",operation="exists"}' % [$.namespaceMatcher(), component]),
    ),
    $.row('')
    .addPanel(
      $.timeseriesPanel('Latency of op: Get') +
      $.latencyPanel('thanos_objstore_bucket_operation_duration_seconds', '{%s,component="%s",operation="get"}' % [$.namespaceMatcher(), component]),
    )
    .addPanel(
      $.timeseriesPanel('Latency of op: GetRange') +
      $.latencyPanel('thanos_objstore_bucket_operation_duration_seconds', '{%s,component="%s",operation="get_range"}' % [$.namespaceMatcher(), component]),
    )
    .addPanel(
      $.timeseriesPanel('Latency of op: Upload') +
      $.latencyPanel('thanos_objstore_bucket_operation_duration_seconds', '{%s,component="%s",operation="upload"}' % [$.namespaceMatcher(), component]),
    )
    .addPanel(
      $.timeseriesPanel('Latency of op: Delete') +
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
      $.timeseriesPanel('Requests / sec') +
      $.queryPanel(
        |||
          sum by(operation) (
            # Backwards compatibility
            rate(thanos_memcached_operations_total{
              %(jobMatcher)s,
              component="%(component)s",
              name="%(cacheName)s"
            }[$__rate_interval])
            or ignoring(backend)
            rate(thanos_cache_operations_total{
              %(jobMatcher)s,
              component="%(component)s",
              name="%(cacheName)s"
            }[$__rate_interval])
          )
        ||| % config,
        '{{operation}}'
      ) +
      $.stack +
      { fieldConfig+: { defaults+: { unit: 'ops' } } }
    )
    .addPanel(
      $.timeseriesPanel('Latency (getmulti)') +
      $.backwardsCompatibleLatencyPanel(
        'thanos_memcached_operation_duration_seconds',
        'thanos_cache_operation_duration_seconds',
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
      $.timeseriesPanel('Hit ratio') +
      $.queryPanel(
        |||
          sum(
            # Backwards compatibility
            rate(thanos_cache_memcached_hits_total{
              %(jobMatcher)s,
              component="%(component)s",
              name="%(cacheName)s"
            }[$__rate_interval])
            or
            rate(thanos_cache_hits_total{
              %(jobMatcher)s,
              component="%(component)s",
              name="%(cacheName)s"
            }[$__rate_interval])
          )
          /
          sum(
            # Backwards compatibility
            rate(thanos_cache_memcached_requests_total{
              %(jobMatcher)s,
              component="%(component)s",
              name="%(cacheName)s"
            }[$__rate_interval])
            or
            rate(thanos_cache_requests_total{
              %(jobMatcher)s,
              component="%(component)s",
              name="%(cacheName)s"
            }[$__rate_interval])
          )
        ||| % config,
        'items'
      ) +
      { fieldConfig+: { defaults+: { unit: 'percentunit' } } }
    ),

  latencyPanelLabelBreakout(
    metricName,
    selector,
    percentiles=['0.99', '0.50'],
    includeAverage=true,
    labels=[],
    labelReplaceArgSets=[{}],
    multiplier='1e3',
  )::
    local averageExprTmpl = $.wrapMultiLabelReplace(
      query='sum(rate(%s_sum%s[$__rate_interval])) by (%s) * %s / sum(rate(%s_count%s[$__rate_interval])) by (%s)',
      labelReplaceArgSets=labelReplaceArgSets,
    );
    local histogramExprTmpl = $.wrapMultiLabelReplace(
      query='histogram_quantile(%s, sum(rate(%s_bucket%s[$__rate_interval])) by (%s)) * %s',
      labelReplaceArgSets=labelReplaceArgSets,
    );
    local labelBreakouts = '%s' % std.join(', ', labels);
    local histogramLabelBreakouts = '%s' % std.join(', ', ['le'] + labels);

    local percentileTargets = [
      {
        expr: histogramExprTmpl % [percentile, metricName, selector, histogramLabelBreakouts, multiplier],
        format: 'time_series',
        legendFormat: '%sth Percentile: {{ %s }}' % [std.lstripChars(percentile, '0.'), labelBreakouts],
        refId: 'A',
      }
      for percentile in percentiles
    ];
    local averageTargets = [
      {
        expr: averageExprTmpl % [metricName, selector, labelBreakouts, multiplier, metricName, selector, labelBreakouts],
        format: 'time_series',
        legendFormat: 'Average: {{ %s }}' % [labelBreakouts],
        refId: 'C',
      },
    ];

    local targets = if includeAverage then percentileTargets + averageTargets else percentileTargets;

    {
      targets: targets,
      fieldConfig+: {
        defaults+: { unit: 'ms', noValue: 0 },
      },
    },

  // Copy/paste of latencyPanel from grafana-builder so that we can migrate between two different
  // names for the same metric. When enough time has passed and we no longer care about the old
  // metric name, this method can be removed and replaced with $.latencyPanel
  backwardsCompatibleLatencyPanel(oldMetricName, newMetricName, selector, multiplier='1e3'):: {
    targets: [
      {
        expr: |||
          histogram_quantile(0.99, sum(
            # Backwards compatibility
            rate(%s_bucket%s[$__rate_interval])
            or
            rate(%s_bucket%s[$__rate_interval])
          ) by (le)) * %s
        ||| % [oldMetricName, selector, newMetricName, selector, multiplier],
        format: 'time_series',
        intervalFactor: 2,
        legendFormat: '99th Percentile',
        refId: 'A',
      },
      {
        expr: |||
          histogram_quantile(0.50, sum(
            # Backwards compatibility
            rate(%s_bucket%s[$__rate_interval])
            or
            rate(%s_bucket%s[$__rate_interval])
          ) by (le)) * %s
        ||| % [oldMetricName, selector, newMetricName, selector, multiplier],
        format: 'time_series',
        intervalFactor: 2,
        legendFormat: '50th Percentile',
        refId: 'B',
      },
      {
        expr: |||
          sum(
            # Backwards compatibility
            rate(%s_sum%s[$__rate_interval])
            or
            rate(%s_sum%s[$__rate_interval])
          ) * %s
          /
          sum(
            # Backwards compatibility
            rate(%s_count%s[$__rate_interval])
            or
            rate(%s_count%s[$__rate_interval])
          )
        ||| % [
          oldMetricName,
          selector,
          newMetricName,
          selector,
          multiplier,
          oldMetricName,
          selector,
          newMetricName,
          selector,
        ],
        format: 'time_series',
        intervalFactor: 2,
        legendFormat: 'Average',
        refId: 'C',
      },
    ],
    fieldConfig+: {
      defaults+: { unit: 'ms', noValue: 0 },
    },
  },

  latencyRecordingRulePanel(metric, selectors, extra_selectors=[], multiplier='1e3', sum_by=[])::
    utils.latencyRecordingRulePanel(metric, selectors, extra_selectors, multiplier, sum_by) + {
      // Hide yaxes from JSON Model; it's not supported by timeseriesPanel.
      yaxes:: super.yaxes,
      fieldConfig+: {
        defaults+: {
          unit: 'ms',
          min: 0,
        },
      },
    },

  filterNodeDiskContainer(containerName)::
    |||
      ignoring(%(instanceLabel)s) group_right() (
        label_replace(
          count by(
            %(nodeLabel)s,
            %(instanceLabel)s,
            device
          )
          (
            container_fs_writes_bytes_total{
              %(namespaceMatcher)s,
              container=~"%(containerName)s",
              device!~".*sda.*"
            }
          ),
          "device",
          "$1",
          "device",
          "/dev/(.*)"
        ) * 0
      )
    ||| % {
      instanceLabel: $._config.per_instance_label,
      containerName: containerName,
      nodeLabel: $._config.per_node_label,
      namespaceMatcher: $.namespaceMatcher(),
    },

  filterKedaScalerErrorsByHPA(hpa_name)::
    |||
      sum by(%(aggregation_labels)s, scaler, metric, scaledObject) (
        label_replace(
          rate(keda_scaler_errors[$__rate_interval]),
          "namespace", "$1", "exported_namespace", "(.+)"
        )
      ) +
      on(%(aggregation_labels)s, metric, scaledObject) group_left
      label_replace(
        label_replace(
            kube_horizontalpodautoscaler_spec_target_metric{%(namespace)s, horizontalpodautoscaler=~"%(hpa_name)s"} * 0,
            "scaledObject", "$1", "horizontalpodautoscaler", "%(hpa_prefix)s(.*)"
        ),
        "metric", "$1", "metric_name", "(.+)"
      )
    ||| % {
      hpa_name: hpa_name,
      hpa_prefix: $._config.autoscaling_hpa_prefix,
      namespace: $.namespaceMatcher(),
      aggregation_labels: $._config.alert_aggregation_labels,
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

  panelSeriesNonErrorColorsPalette(legends):: {
    seriesOverrides: std.prune(std.mapWithIndex(function(idx, legend) (
      // Do not define an override if we exausted the colors in the palette.
      // Grafana will automatically choose another color.
      if idx >= std.length(nonErrorColorsPalette) then null else
        {
          alias: legend,
          color: nonErrorColorsPalette[idx],
        }
    ), legends)),
  },

  // Panel query override functions
  overrideField(matcherId, options, overrideProperties):: {
    matcher: {
      id: matcherId,
      options: options,
    },
    properties: overrideProperties,
  },

  overrideFieldByName(fieldName, overrideProperties)::
    $.overrideField('byName', fieldName, overrideProperties),

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

  wrapMultiLabelReplace(query, labelReplaceArgSets=[{}])::
    std.foldl(
      function(query, labelReplaceArgSet) $.wrapLabelReplace(query, labelReplaceArgSet),
      labelReplaceArgSets,
      query,
    ),

  wrapLabelReplace(query, labelReplaceArgSet={})::
    |||
      label_replace(%(query)s, "%(dstLabel)s", "%(replacement)s", "%(srcLabel)s", "%(regex)s")
    ||| % labelReplaceArgSet { query: query },

  lokiMetricsQueryPanel(queries, legends='', unit='short')::
    super.queryPanel(queries, legends) +
    {
      datasource: '${loki_datasource}',
      fieldConfig+: { defaults+: { unit: unit } },
    },

  // Backwards compatible helper functions

  aliasColors(colors):: {
    // aliasColors was the configuration in (deprecated) graph panel; we hide it from JSON model.
    aliasColors:: super.aliasColors,
    fieldConfig+: {
      overrides+: [
        $.overrideFieldByName(name, [
          $.overrideProperty('color', { mode: 'fixed', fixedColor: colors[name] }),
        ])
        for name in std.objectFields(colors)
      ],
    },
  },
}
