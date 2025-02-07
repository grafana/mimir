local utils = import 'mixin-utils/utils.libsonnet';

(import 'grafana-builder/grafana.libsonnet') {
  _colors:: {
    resourceRequest: '#FFC000',
    resourceLimit: '#E02F44',
    success: '#7EB26D',
    clientError: '#EF843C',
    warning: '#EAB839',
    failed: '#E24D42',  // "error" is reserved word in Jsonnet.
  },

  // Colors palette picked from Grafana UI, excluding red-ish colors which we want to keep reserved for errors / failures.
  local nonErrorColorsPalette = ['#429D48', '#F1C731', '#2A66CF', '#9E44C1', '#FFAB57', '#C79424', '#84D586', '#A1C4FC', '#C788DE', '#3F6833', '#447EBC', '#967302', '#5794F2'],

  local resourceRequestStyle = $.overrideFieldByName('request', [
    $.overrideProperty('color', { mode: 'fixed', fixedColor: $._colors.resourceRequest }),
    $.overrideProperty('custom.fillOpacity', 0),
    $.overrideProperty('custom.lineStyle', { fill: 'dash' }),
  ]),
  local resourceLimitStyle = $.overrideFieldByName('limit', [
    $.overrideProperty('color', { mode: 'fixed', fixedColor: $._colors.resourceLimit }),
    $.overrideProperty('custom.fillOpacity', 0),
    $.overrideProperty('custom.lineStyle', { fill: 'dash' }),
  ]),

  local sortAscending = 1,
  local sortNaturalAscending = 7,

  _config:: error 'must provide _config',

  row(title)::
    super.row(title) + {
      addPanelIf(condition, panel)::
        if condition
        then self.addPanel(panel)
        else self,

      // justifyPanels make sure that the panels span the whole row.
      // It is useful when the number of panels is not a divisor of 12.
      justifyPanels()::
        self + {
          local n = std.length(super.panels),
          local span = std.floor(12 / n),
          panels: [
            super.panels[i] { span: span + if i < (12 % n) then 1 else 0 }
            for i in std.range(0, n - 1)
          ],
        },
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

      refresh: '5m',

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
          then d.addMultiTemplate('job', $._config.dashboard_variables.job_query, $._config.per_job_label, sort=sortAscending, includeAll=false)
          else d
               .addMultiTemplate('cluster', $._config.dashboard_variables.cluster_query, '%s' % $._config.per_cluster_label, sort=sortAscending)
               .addMultiTemplate('namespace', $._config.dashboard_variables.namespace_query, '%s' % $._config.per_namespace_label, sort=sortAscending, includeAll=false)
        else
          if $._config.singleBinary
          then d.addTemplate('job', $._config.dashboard_variables.job_query, $._config.per_job_label, sort=sortAscending)
          else d
               .addTemplate('cluster', $._config.dashboard_variables.cluster_query, '%s' % $._config.per_cluster_label, allValue='.*', includeAll=true, sort=sortAscending)
               .addTemplate('namespace', $._config.dashboard_variables.namespace_query, '%s' % $._config.per_namespace_label, sort=sortAscending),

      addActiveUserSelectorTemplates()::
        self.addTemplate('user', 'cortex_ingester_active_series{%s=~"$cluster", %s=~"$namespace"}' % [$._config.per_cluster_label, $._config.per_namespace_label], 'user', sort=sortNaturalAscending),

      addCustomTemplate(label, name, options, defaultIndex=0):: self {
        // Escape the comma because it's used a separator in the options list.
        local escapeValue(v) = std.strReplace(v, ',', '\\,'),

        templating+: {
          list+: [{
            current: {
              selected: true,
              text: options[defaultIndex].label,
              value: escapeValue(options[defaultIndex].value),
            },
            hide: 0,
            includeAll: false,
            label: label,
            multi: false,
            name: name,
            query: std.join(',', [
              '%s : %s' % [option.label, escapeValue(option.value)]
              for option in options
            ]),
            options: [
              {
                selected: option.label == options[defaultIndex].label,
                text: option.label,
                value: escapeValue(option.value),
              }
              for option in options
            ],
            skipUrlSync: false,
            type: 'custom',
            useTags: false,
          }],
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
    else [utils.selector.re('%s' % $._config.per_cluster_label, '$cluster'), utils.selector.re($._config.per_job_label, '%s(%s)' % [$._config.job_prefix, formatJobForQuery(job)])],

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
      options+: {
        tooltip+: {
          mode: 'multi',
        },
      },
    },

  qpsPanel(selector, statusLabelName='status_code')::
    super.qpsPanel(selector, statusLabelName) +
    $.aliasColors({
      '1xx': $._colors.warning,
      '2xx': $._colors.success,
      '3xx': '#6ED0E0',
      '4xx': '#EF843C',
      '5xx': $._colors.failed,
      OK: $._colors.success,
      success: $._colors.success,
      'error': $._colors.failed,
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
    $.showAllTooltip +
    {
      options+: {
        legend+: {
          showLegend: false,
          // Work round Grafana turning showLegend back on when we have
          // schemaVersion<37. https://github.com/grafana/grafana/issues/54472
          displayMode: 'hidden',
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

  perInstanceLatencyPanelNativeHistogram(quantile, metric, selector, legends=null, instanceLabel=$._config.per_instance_label, from_recording=false)::
    local queries = [
      utils.showClassicHistogramQuery(utils.ncHistogramQuantile(quantile, metric, utils.toPrometheusSelectorNaked(selector), [instanceLabel], from_recording=from_recording)),
      utils.showNativeHistogramQuery(utils.ncHistogramQuantile(quantile, metric, utils.toPrometheusSelectorNaked(selector), [instanceLabel], from_recording=from_recording)),
    ];
    if legends == null then
      $.hiddenLegendQueryPanel(queries, ['', ''])
    else
      $.queryPanel(queries, legends),

  // Creates a panel like queryPanel() but if the legend contains only 1 entry,
  // than it configures the series alias color to the one used to display failures.
  failurePanel(queries, legends, legendLink=null)::
    $.queryPanel(queries, legends, legendLink) +
    // Set the failure color only if there's just 1 legend and it doesn't contain any placeholder.
    $.aliasColors(
      if (std.type(legends) == 'string' && std.length(std.findSubstr('{', legends[0])) == 0) then {
        [legends]: $._colors.failed,
      } else {}
    ),

  successFailurePanel(successMetric, failureMetric)::
    $.queryPanel([successMetric, failureMetric], ['successful', 'failed']) +
    $.aliasColors({
      successful: $._colors.success,
      failed: $._colors.failed,
    }),

  // successFailureCustomPanel is like successFailurePanel() but allows to customize the legends
  // and have additional queries. The success and failure queries MUST be the first and second
  // queries respectively.
  successFailureCustomPanel(queries, legends)::
    $.queryPanel(queries, legends) +
    $.aliasColors({
      [legends[0]]: $._colors.success,
      [legends[1]]: $._colors.failed,
    }),

  // Displays started, completed and failed rate.
  startedCompletedFailedPanel(title, startedMetric, completedMetric, failedMetric)::
    $.timeseriesPanel(title) +
    $.queryPanel([startedMetric, completedMetric, failedMetric], ['started', 'completed', 'failed']) +
    $.stack +
    $.aliasColors({
      started: '#34CCEB',
      completed: $._colors.success,
      failed: $._colors.failed,
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
    $.showAllTooltip +
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
    },

  // The provided componentName should be the name of a component among the ones defined in $._config.instance_names.
  containerCPUUsagePanelByComponent(componentName)::
    $.containerCPUUsagePanel($._config.instance_names[componentName], $._config.container_names[componentName]),

  // The provided instanceName should be a regexp from $._config.instance_names, while
  // the provided containerName should be a regexp from $._config.container_names.
  containerMemoryWorkingSetPanel(instanceName, containerName)::
    $.timeseriesPanel('Memory (workingset)') +
    $.queryPanel($.resourceUtilizationAndLimitQueries('memory_working', instanceName, containerName), $.resourceUtilizationAndLimitLegend('{{%s}}' % $._config.per_instance_label)) +
    $.showAllTooltip +
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
    },

  // The provided componentName should be the name of a component among the ones defined in $._config.instance_names.
  containerMemoryWorkingSetPanelByComponent(componentName)::
    $.containerMemoryWorkingSetPanel($._config.instance_names[componentName], $._config.container_names[componentName]),

  // The provided instanceName should be a regexp from $._config.instance_names, while
  // the provided containerName should be a regexp from $._config.container_names.
  containerMemoryRSSPanel(instanceName, containerName)::
    $.timeseriesPanel('Memory (RSS)') +
    $.queryPanel($.resourceUtilizationAndLimitQueries('memory_rss', instanceName, containerName), $.resourceUtilizationAndLimitLegend('{{%s}}' % $._config.per_instance_label)) +
    $.showAllTooltip +
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
    },

  // The provided componentName should be the name of a component among the ones defined in $._config.instance_names.
  containerMemoryRSSPanelByComponent(componentName)::
    $.containerMemoryRSSPanel($._config.instance_names[componentName], $._config.container_names[componentName]),

  // The provided instanceName should be a regexp from $._config.instance_names, while
  // the provided containerName should be a regexp from $._config.container_names.
  containerGoHeapInUsePanel(instanceName, containerName)::
    $.timeseriesPanel('Memory (go heap inuse)') +
    $.queryPanel($.resourceUtilizationQuery('memory_go_heap', instanceName, containerName), '{{%s}}' % $._config.per_instance_label) +
    $.showAllTooltip +
    {
      fieldConfig+: {
        defaults+: {
          unit: 'bytes',
          custom+: {
            fillOpacity: 0,
          },
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
        persistentVolumeClaimMatcher: $.containerPersistentVolumeClaimMatcher(containerName),
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
  containerPersistentVolumeClaimMatcher(containerName)::
    'persistentvolumeclaim=~".*(%s).*"' % containerName,

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
      local title = 'Ingress TCP connections (per pod)';

      $.timeseriesPanel(title) +
      $.panelDescription(
        title,
        'The number of ingress TCP connections (HTTP and gRPC protocol).'
      ) +
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

  // The provided componentName should be the name of a component among the ones defined in $._config.autoscaling.
  autoScalingActualReplicas(componentName, addlQueries=[], addlLegends=[])::
    local title = 'Replicas';
    local componentTitle = std.strReplace(componentName, '_', '-');

    $.timeseriesPanel(title) +
    $.queryPanel(
      [
        |||
          max by (scaletargetref_name) (
            kube_horizontalpodautoscaler_spec_max_replicas{%(namespace_matcher)s, horizontalpodautoscaler=~"%(hpa_name)s"}
            # Add the scaletargetref_name label for readability
            * on (%(cluster_labels)s, horizontalpodautoscaler) group_left (scaletargetref_name)
              group by (%(cluster_labels)s, horizontalpodautoscaler, scaletargetref_name) (kube_horizontalpodautoscaler_info{%(namespace_matcher)s, horizontalpodautoscaler=~"%(hpa_name)s"})
          )
        ||| % {
          namespace_matcher: $.namespaceMatcher(),
          hpa_name: $._config.autoscaling[componentName].hpa_name,
          cluster_labels: std.join(', ', $._config.cluster_labels),
        },
        |||
          max by (scaletargetref_name) (
            kube_horizontalpodautoscaler_status_current_replicas{%(namespace_matcher)s, horizontalpodautoscaler=~"%(hpa_name)s"}
            # Add the scaletargetref_name label for readability
            * on (%(cluster_labels)s, horizontalpodautoscaler) group_left (scaletargetref_name)
              group by (%(cluster_labels)s, horizontalpodautoscaler, scaletargetref_name) (kube_horizontalpodautoscaler_info{%(namespace_matcher)s, horizontalpodautoscaler=~"%(hpa_name)s"})
          )
        ||| % {
          namespace_matcher: $.namespaceMatcher(),
          hpa_name: $._config.autoscaling[componentName].hpa_name,
          cluster_labels: std.join(', ', $._config.cluster_labels),
        },
        |||
          max by (scaletargetref_name) (
            kube_horizontalpodautoscaler_spec_min_replicas{%(namespace_matcher)s, horizontalpodautoscaler=~"%(hpa_name)s"}
            # Add the scaletargetref_name label for readability
            * on (%(cluster_labels)s, horizontalpodautoscaler) group_left (scaletargetref_name)
              group by (%(cluster_labels)s, horizontalpodautoscaler, scaletargetref_name) (kube_horizontalpodautoscaler_info{%(namespace_matcher)s, horizontalpodautoscaler=~"%(hpa_name)s"})
          )
        ||| % {
          namespace_matcher: $.namespaceMatcher(),
          hpa_name: $._config.autoscaling[componentName].hpa_name,
          cluster_labels: std.join(', ', $._config.cluster_labels),
        },
      ] + addlQueries,
      [
        'Max {{ scaletargetref_name }}',
        'Current {{ scaletargetref_name }}',
        'Min {{ scaletargetref_name }}',
      ] + addlLegends,
    ) +
    $.panelDescription(
      title,
      |||
        The minimum, maximum, and current number of %s replicas.
      ||| % [componentTitle]
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

  // The provided componentName should be the name of a component among the ones defined in $._config.autoscaling.
  autoScalingDesiredReplicasByAverageValueScalingMetricPanel(componentName, scalingMetricName, scalingMetricID)::
    local title = if scalingMetricName != '' then 'Scaling metric (%s): Desired replicas' % scalingMetricName else 'Desired replicas';
    local scalerSelector = if scalingMetricID != '' then ('.*%s.*' % scalingMetricID) else '.+';

    $.timeseriesPanel(title) +
    $.queryPanel(
      [
        |||
          sum by (scaler) (
            # Using `max by ()` so that series churn doesn't break the promQL join
            max by (%(aggregation_labels)s, scaledObject, metric, scaler) (
              label_replace(
                keda_scaler_metrics_value{%(cluster_label)s=~"$cluster", exported_namespace=~"$namespace", scaler=~"%(scaler_selector)s"},
                "namespace", "$1", "exported_namespace", "(.*)"
              )
            )
            /
            on(%(aggregation_labels)s, scaledObject, metric) group_left
              # Using `max by ()` so that series churn doesn't break the promQL join
              max by (%(aggregation_labels)s, scaledObject, metric) (
                label_replace(
                  label_replace(
                    kube_horizontalpodautoscaler_spec_target_metric{%(namespace)s, horizontalpodautoscaler=~"%(hpa_name)s"},
                    "metric", "$1", "metric_name", "(.+)"
                  ),
                  "scaledObject", "$1", "horizontalpodautoscaler", "%(hpa_prefix)s(.*)"
                )
              )
          )
        ||| % {
          aggregation_labels: $._config.alert_aggregation_labels,
          cluster_label: $._config.per_cluster_label,
          hpa_prefix: $._config.autoscaling_hpa_prefix,
          hpa_name: $._config.autoscaling[componentName].hpa_name,
          namespace: $.namespaceMatcher(),
          scaler_selector: scalerSelector,
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

  // The provided componentName should be the name of a component among the ones defined in $._config.autoscaling.
  autoScalingDesiredReplicasByValueScalingMetricPanel(componentName, scalingMetricName, scalingMetricID)::
    local title = if scalingMetricName != '' then 'Scaling metric (%s): Desired replicas' % scalingMetricName else 'Desired replicas';
    local scalerSelector = if scalingMetricID != '' then ('.*%s.*' % scalingMetricID) else '.+';

    $.timeseriesPanel(title) +
    $.queryPanel(
      [
        |||
          sum by (scaler) (
            # Using `max by ()` so that series churn doesn't break the promQL join
            max by (%(aggregation_labels)s, scaledObject, metric, scaler) (
              label_replace(
                keda_scaler_metrics_value{%(cluster_label)s=~"$cluster", exported_namespace=~"$namespace", scaler=~"%(scaler_selector)s"},
                "namespace", "$1", "exported_namespace", "(.*)"
              )
            )
            /
            on(%(aggregation_labels)s, scaledObject, metric) group_left
              # Using `max by ()` so that series churn doesn't break the promQL join
              max by (%(aggregation_labels)s, scaledObject, metric) (
                label_replace(
                  label_replace(
                    kube_horizontalpodautoscaler_spec_target_metric{%(namespace)s, horizontalpodautoscaler=~"%(hpa_name)s"},
                    "metric", "$1", "metric_name", "(.+)"
                  ),
                  "scaledObject", "$1", "horizontalpodautoscaler", "%(hpa_prefix)s(.*)"
                )
              )
            *
            on(%(aggregation_labels)s, scaledObject) group_left
              # Using `max by ()` so that series churn doesn't break the promQL join
              max by (%(aggregation_labels)s, scaledObject) (
                label_replace(
                  kube_horizontalpodautoscaler_status_current_replicas{%(namespace)s, horizontalpodautoscaler=~"%(hpa_name)s"},
                  "scaledObject", "$1", "horizontalpodautoscaler", "keda-hpa-(.*)"
                )
              )
          )
        ||| % {
          aggregation_labels: $._config.alert_aggregation_labels,
          cluster_label: $._config.per_cluster_label,
          hpa_prefix: $._config.autoscaling_hpa_prefix,
          hpa_name: $._config.autoscaling[componentName].hpa_name,
          namespace: $.namespaceMatcher(),
          scaler_selector: scalerSelector,
        },
      ], [
        '{{ scaler }}',
      ]
    ) +
    $.panelDescription(
      title,
      |||
        This panel shows the scaling metric exposed by KEDA divided by the target/threshold and multiplied by the current number of replicas.
        It should represent the desired number of replicas, ignoring the min/max constraints applied later.
      |||
    ),

  // The provided componentName should be the name of a component among the ones defined in $._config.autoscaling.
  autoScalingFailuresPanel(componentName)::
    local title = 'Autoscaler failures rate';

    $.timeseriesPanel(title) +
    $.queryPanel(
      $.filterKedaScalerErrorsByHPA($._config.autoscaling[componentName].hpa_name),
      '{{scaler}} failures'
    ) +
    $.panelDescription(
      title,
      |||
        The rate of failures in the KEDA custom metrics API server. Whenever an error occurs, the KEDA custom
        metrics server is unable to query the scaling metric from Prometheus so the autoscaler wouldn't work properly.
      |||
    ),

  cpuBasedAutoScalingRow(componentTitle)::
    local componentName = std.strReplace(std.asciiLower(componentTitle), '-', '_');
    super.row('%s – autoscaling' % [componentTitle])
    .addPanel(
      $.autoScalingActualReplicas(componentName)
    )
    .addPanel(
      $.autoScalingDesiredReplicasByAverageValueScalingMetricPanel(componentName, 'CPU', 'cpu')
    )
    .addPanel(
      $.autoScalingFailuresPanel(componentName)
    ),

  cpuAndMemoryBasedAutoScalingRow(componentTitle)::
    local componentName = std.strReplace(std.asciiLower(componentTitle), '-', '_');
    super.row('%s – autoscaling' % [componentTitle])
    .addPanel(
      $.autoScalingActualReplicas(componentName)
    )
    .addPanel(
      $.autoScalingDesiredReplicasByAverageValueScalingMetricPanel(componentName, 'CPU', 'cpu')
    )
    .addPanel(
      $.autoScalingDesiredReplicasByAverageValueScalingMetricPanel(componentName, 'memory', 'memory')
    )
    .addPanel(
      $.autoScalingFailuresPanel(componentName)
    ),

  ncSumCountRateStatPanel(metric, selectors, extra_selector, thresholds=[])::
    local ncQuery = $.ncSumHistogramCountRate(metric, selectors, extra_selector);
    local queries = [
      utils.showClassicHistogramQuery(ncQuery),
      utils.showNativeHistogramQuery(ncQuery),
    ];
    $.newStatPanel(
      queries=queries,
      legends=['', ''],
      unit='percentunit',
      thresholds=thresholds,
    ),

  ncLatencyStatPanel(quantile, metric, selectors, thresholds=[])::
    local labels = std.join('_', [matcher.label for matcher in selectors]);
    local metricStr = '%(labels)s:%(metric)s' % { labels: labels, metric: metric };
    local queries = [
      utils.showClassicHistogramQuery(utils.ncHistogramQuantile(quantile, metricStr, utils.toPrometheusSelectorNaked(selectors), from_recording=true)),
      utils.showNativeHistogramQuery(utils.ncHistogramQuantile(quantile, metricStr, utils.toPrometheusSelectorNaked(selectors), from_recording=true)),
    ];
    $.newStatPanel(
      queries=queries,
      legends=['', ''],
      unit='s',
      thresholds=thresholds,
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

  tablePanel(queries, labelStyles)::
    super.tablePanel(queries, labelStyles={}) + {
      // Hides styles field, as it makes Grafana 11 use the deprecate "Table (old)" plugin.
      styles:: super.styles,
      local stylesToProps(s) =
        if std.type(s) == 'string' then [
          $.overrideProperty('displayName', s),
          $.overrideProperty('decimals', 0),
          $.overrideProperty('unit', 'short'),
        ] else [
          if std.objectHas(s, 'alias') then $.overrideProperty('displayName', s.alias),
          if std.objectHas(s, 'type') && s.type == 'hidden' then $.overrideProperty('custom.hidden', true),
          $.overrideProperty('decimals', if std.objectHas(s, 'decimals') then s.decimals else 2),
          $.overrideProperty('unit', if std.objectHas(s, 'unit') then s.unit else 'short'),
        ],
      fieldConfig+: {
        overrides+: [
          // Hide time column by default, like jsonnet-lib/grafana-builder does.
          $.overrideFieldByName('Time', [
            $.overrideProperty('displayName', 'Time'),
            $.overrideProperty('custom.hidden', true),
          ]),
        ] + [
          $.overrideFieldByName(label, std.prune(stylesToProps(labelStyles[label])))
          for label in std.objectFields(labelStyles)
        ],
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

  // Shows all series' values in the tooltip and sorts them in descending order.
  showAllTooltip:: {
    options+: {
      tooltip+: {
        mode: 'multi',
        sort: 'desc',
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
            { color: $._colors.success, value: null },
            { color: $._colors.warning, value: 0.01 },  // 1%
            { color: $._colors.failed, value: 0.05 },  // 5%
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
      $.latencyPanel(
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
            rate(thanos_cache_hits_total{
              %(jobMatcher)s,
              component="%(component)s",
              name="%(cacheName)s"
            }[$__rate_interval])
          )
          /
          sum(
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

  latencyRecordingRulePanelNativeHistogram(metric, selectors, extra_selectors=[], multiplier='1e3', sum_by=[])::
    utils.latencyRecordingRulePanelNativeHistogram(metric, selectors, extra_selectors, multiplier, sum_by) + {
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
      sum by(%(aggregation_labels)s, metric, scaledObject, scaler) (
        label_replace(
          rate(keda_scaler_errors[$__rate_interval]),
          "namespace", "$1", "exported_namespace", "(.+)"
        )
      ) +
      on(%(aggregation_labels)s, metric, scaledObject) group_left
        # Using `max by ()` so that series churn doesn't break the promQL join
        max by (%(aggregation_labels)s, metric, scaledObject) (
          label_replace(
            label_replace(
                kube_horizontalpodautoscaler_spec_target_metric{%(namespace)s, horizontalpodautoscaler=~"%(hpa_name)s"} * 0,
                "scaledObject", "$1", "horizontalpodautoscaler", "%(hpa_prefix)s(.*)"
            ),
            "metric", "$1", "metric_name", "(.+)"
          )
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

  overridesNonErrorColorsPalette(overrides):: std.mapWithIndex(function(idx, override) (
    // Do not define an override if we exausted the colors in the palette.
    // Grafana will automatically choose another color.
    if idx >= std.length(nonErrorColorsPalette) then override else
      {
        matcher: override.matcher,
        properties: override.properties + [
          {
            id: 'color',
            value: {
              fixedColor: nonErrorColorsPalette[idx],
              mode: 'fixed',
            },
          },
        ],
      }
  ), overrides),

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

  capitalize(str):: std.asciiUpper(str[0]) + str[1:],

  commonReadsDashboardsRows(
    queryFrontendJobName,
    querySchedulerJobName,
    querierJobName,
    queryRoutesRegex,
    queryPathDescription,
    rowTitlePrefix='',
    showQueryCacheRow=false,
  )::
    [
      $.row($.capitalize(rowTitlePrefix + 'query-frontend'))
      .addPanel(
        $.timeseriesPanel('Requests / sec') +
        $.qpsPanelNativeHistogram($.queries.query_frontend.requestsPerSecondMetric, utils.toPrometheusSelectorNaked($.jobSelector(queryFrontendJobName) + [utils.selector.re('route', queryRoutesRegex)]))
      )
      .addPanel(
        $.timeseriesPanel('Latency') +
        $.latencyRecordingRulePanelNativeHistogram($.queries.query_frontend.requestsPerSecondMetric, $.jobSelector(queryFrontendJobName) + [utils.selector.re('route', queryRoutesRegex)])
      )
      .addPanel(
        $.timeseriesPanel('Per %s p99 latency' % $._config.per_instance_label) +
        $.perInstanceLatencyPanelNativeHistogram('0.99', $.queries.query_frontend.requestsPerSecondMetric, $.jobSelector(queryFrontendJobName) + [utils.selector.re('route', queryRoutesRegex)])
      ),
      local description = |||
        <p>
          The query scheduler is an optional service that moves
          the internal queue from the query-frontend into a
          separate component.
          If this service is not deployed,
          these panels will show "No data."
        </p>
      |||;
      $.row($.capitalize(rowTitlePrefix + 'query-scheduler'))
      .addPanel(
        local title = 'Requests / sec';
        $.timeseriesPanel(title) +
        $.panelDescription(title, description) +
        $.qpsPanel('cortex_query_scheduler_queue_duration_seconds_count{%s}' % $.jobMatcher(querySchedulerJobName))
      )
      .addPanel(
        local title = 'Latency (Time in Queue)';
        $.timeseriesPanel(title) +
        $.panelDescription(title, description) +
        $.latencyPanel('cortex_query_scheduler_queue_duration_seconds', '{%s}' % $.jobMatcher(querySchedulerJobName))
      )
      .addPanel(
        local title = 'Queue length';
        $.timeseriesPanel(title) +
        $.panelDescription(title, description) +
        $.hiddenLegendQueryPanel(
          'sum(min_over_time(cortex_query_scheduler_queue_length{%s}[$__interval]))' % [$.jobMatcher(querySchedulerJobName)],
          'Queue length'
        ) +
        {
          fieldConfig+: {
            defaults+: {
              unit: 'queries',
            },
          },
        },
      ),
      local description = |||
        <p>
          The query scheduler creates subqueues
          broken out by which query components (ingester, store-gateway, or both)
          the querier is expected to fetch data from to service the query.

          Queries which have not had an expected query component determined are labeled 'unknown'.
        </p>
      |||;
      local metricName = 'cortex_query_scheduler_queue_duration_seconds';
      local selector = '{%s}' % $.jobMatcher(querySchedulerJobName);
      local labels = ['additional_queue_dimensions'];
      local labelReplaceArgSets = [
        {
          dstLabel: 'additional_queue_dimensions',
          replacement: 'unknown',
          srcLabel:
            'additional_queue_dimensions',
          regex: '^$',
        },
      ];
      $.row($.capitalize(rowTitlePrefix + 'query-scheduler Latency (Time in Queue) Breakout by Expected Query Component'))
      .addPanel(
        local title = '99th Percentile Latency by Expected Query Component';
        $.timeseriesPanel(title) +
        $.panelDescription(title, description) +
        $.latencyPanelLabelBreakout(
          metricName=metricName,
          selector=selector,
          percentiles=['0.99'],
          includeAverage=false,
          labels=labels,
          labelReplaceArgSets=labelReplaceArgSets,
        )
      )
      .addPanel(
        local title = '50th Percentile Latency by Expected Query Component';
        $.timeseriesPanel(title) +
        $.panelDescription(title, description) +
        $.latencyPanelLabelBreakout(
          metricName=metricName,
          selector=selector,
          percentiles=['0.50'],
          includeAverage=false,
          labels=labels,
          labelReplaceArgSets=labelReplaceArgSets,
        )
      )
      .addPanel(
        local title = 'Average Latency by Expected Query Component';
        $.timeseriesPanel(title) +
        $.panelDescription(title, description) +
        $.latencyPanelLabelBreakout(
          metricName=metricName,
          selector=selector,
          percentiles=[],
          includeAverage=true,
          labels=labels,
          labelReplaceArgSets=labelReplaceArgSets,
        )
      ),
      local description = |||
        <p>
          The query scheduler tracks query requests inflight
          between the scheduler and the connected queriers,
          broken out by which query component (ingester, store-gateway)
          the querier is expected to fetch data from to service the query.

          Queries which require data from both ingesters and store-gateways
          are counted in each category, so the sum of the two categories
          may exceed the true total number of queries inflight.
        </p>
      |||;
      local metricName = 'cortex_query_scheduler_querier_inflight_requests';
      local selector = '{%s}' % $.jobMatcher(querySchedulerJobName);
      local labels = ['additional_queue_dimensions'];
      local labelReplaceArgSets = [
        {
          dstLabel: 'additional_queue_dimensions',
          replacement: 'none',
          srcLabel:
            'additional_queue_dimensions',
          regex: '^$',
        },
      ];
      $.row($.capitalize(rowTitlePrefix + 'query-scheduler <-> Querier Inflight Requests'))
      .addPanel(
        local title = $.capitalize('99th Percentile Inflight Requests by Query Component vs. Total Connected Queriers');
        $.timeseriesPanel(title) +
        $.panelDescription(title, description) +
        $.queryPanel(
          [
            'sum by(query_component) (cortex_query_scheduler_querier_inflight_requests{quantile="0.99", %s})' % [$.jobMatcher(querySchedulerJobName)],
            'sum(cortex_query_scheduler_connected_querier_clients{%s})' % [$.jobMatcher(querySchedulerJobName)],
          ],
          [
            '99th Percentile Inflight Requests: {{query_component}}',
            'Total Connected Queriers',
          ]
        )
      ),
    ] +
    (
      if (!showQueryCacheRow) then [] else [
        $.row('Cache – query results')
        .addPanel(
          $.timeseriesPanel('Requests / sec') +
          $.queryPanel(
            |||
              sum (
                rate(thanos_cache_operations_total{name="frontend-cache", %(frontend)s}[$__rate_interval])
              )
            ||| % {
              frontend: $.jobMatcher(queryFrontendJobName),
            },
            'Requests/s'
          ) +
          { fieldConfig+: { defaults+: { unit: 'ops' } } },
        )
        .addPanel(
          $.timeseriesPanel('Latency') +
          $.latencyPanel(
            'thanos_cache_operation_duration_seconds',
            '{%s, name="frontend-cache"}' % $.jobMatcher(queryFrontendJobName)
          )
        ),
      ]
    ) + [
      $.row($.capitalize(rowTitlePrefix + 'querier'))
      .addPanel(
        $.timeseriesPanel('Requests / sec') +
        $.qpsPanel('cortex_querier_request_duration_seconds_count{%s, route=~"%s"}' % [$.jobMatcher(querierJobName), $.queries.read_http_routes_regex])
      )
      .addPanel(
        $.timeseriesPanel('Latency') +
        $.latencyRecordingRulePanel('cortex_querier_request_duration_seconds', $.jobSelector(querierJobName) + [utils.selector.re('route', $.queries.read_http_routes_regex)])
      )
      .addPanel(
        $.timeseriesPanel('Per %s p99 latency' % $._config.per_instance_label) +
        $.hiddenLegendQueryPanel(
          'histogram_quantile(0.99, sum by(le, %s) (rate(cortex_querier_request_duration_seconds_bucket{%s, route=~"%s"}[$__rate_interval])))' % [$._config.per_instance_label, $.jobMatcher(querierJobName), $.queries.read_http_routes_regex], ''
        )
      ),
    ] +
    $.ingesterStoreGatewayReadsDashboardsRows(
      'ingester',
      $.queries.ingester.requestsPerSecondMetric,
      $.queries.ingester.readRequestsPerSecondSelector,
      $._config.job_names.ingester,
      $._config.ingester_read_path_routes_regex,
      $.queries.querier.ingesterClientRequestsPerSecondMetric,
      std.asciiLower(rowTitlePrefix),
      querierJobName,
      queryPathDescription,
    ) +
    $.ingesterStoreGatewayReadsDashboardsRows(
      'store-gateway',
      $.queries.store_gateway.requestsPerSecondMetric,
      $.queries.store_gateway.readRequestsPerSecondSelector,
      $._config.job_names.store_gateway,
      $._config.store_gateway_read_path_routes_regex,
      $.queries.querier.storeGatewayClientRequestsPerSecondMetric,
      std.asciiLower(rowTitlePrefix),
      querierJobName,
      queryPathDescription,
    ),

  ingesterStoreGatewayReadsDashboardsRows(
    ingesterStoreGatewayComponentName,
    ingesterStoreGatewayRequestsPerSecondMetric,
    ingesterStoreGatewayRequestsPerSecondSelector,
    ingesterStoreGatewayJobNames,
    ingesterStoreGatewayRoutesRegex,
    querierRequestsPerSecondMetric,
    querierPrefix,
    querierJobNames,
    queryPathDescription,
  ):: [
    local description = 'This panel shows %(ingesterStoreGatewayComponentName)s query requests from all sources: the main query path, and the remote ruler query path, if in use. The data shown is as reported by %(ingesterStoreGatewayComponentName)ss.' % { ingesterStoreGatewayComponentName: ingesterStoreGatewayComponentName };

    $.row($.capitalize(ingesterStoreGatewayComponentName + ' - query requests from all sources'))
    .addPanel(
      $.timeseriesPanel('Requests / sec') +
      $.panelDescription('Requests / sec', description) +
      $.qpsPanelNativeHistogram(ingesterStoreGatewayRequestsPerSecondMetric, ingesterStoreGatewayRequestsPerSecondSelector)
    )
    .addPanel(
      $.timeseriesPanel('Latency') +
      $.panelDescription('Latency', description) +
      $.latencyRecordingRulePanelNativeHistogram(ingesterStoreGatewayRequestsPerSecondMetric, $.jobSelector(ingesterStoreGatewayJobNames) + [utils.selector.re('route', ingesterStoreGatewayRoutesRegex)])
    )
    .addPanel(
      $.timeseriesPanel('Per %s %s p99 latency' % [ingesterStoreGatewayComponentName, $._config.per_instance_label]) +
      $.panelDescription('Per %s %s p99 latency' % [ingesterStoreGatewayComponentName, $._config.per_instance_label], description) +
      $.perInstanceLatencyPanelNativeHistogram(
        '0.99',
        ingesterStoreGatewayRequestsPerSecondMetric,
        $.jobSelector(ingesterStoreGatewayJobNames) + [utils.selector.re('route', ingesterStoreGatewayRoutesRegex)],
      ),
    ),

    local description = 'This panel shows %(ingesterStoreGatewayComponentName)s query requests from just the %(queryPathDescription)s. The data shown is as reported by %(querierPrefix)squeriers.' % {
      ingesterStoreGatewayComponentName: ingesterStoreGatewayComponentName,
      queryPathDescription: queryPathDescription,
      querierPrefix: querierPrefix,
    };
    local selectors = $.jobSelector(querierJobNames) + [utils.selector.re('operation', ingesterStoreGatewayRoutesRegex)];

    $.row($.capitalize(ingesterStoreGatewayComponentName + ' - query requests from this query path only'))
    .addPanel(
      $.timeseriesPanel('Requests / sec') +
      $.panelDescription('Requests / sec', description) +
      $.qpsPanelNativeHistogram(querierRequestsPerSecondMetric, utils.toPrometheusSelectorNaked(selectors))
    )
    .addPanel(
      $.timeseriesPanel('Latency') +
      $.panelDescription('Latency', description) +
      $.latencyPanel(querierRequestsPerSecondMetric, utils.toPrometheusSelector(selectors))
    )
    .addPanel(
      $.timeseriesPanel('Per querier %s p99 latency' % $._config.per_instance_label) +
      $.panelDescription('Per querier %s p99 latency' % $._config.per_instance_label, description) +
      $.perInstanceLatencyPanelNativeHistogram('0.99', querierRequestsPerSecondMetric, selectors),
    ),
  ],

  ingestStorageIngesterEndToEndLatencyWhenStartingPanel()::
    $.timeseriesPanel('Kafka end-to-end latency when starting') +
    $.panelDescription(
      'Kafka end-to-end latency when starting',
      |||
        Time between writing request by distributor to Kafka and reading the record by ingester during catch-up phase, when ingesters are starting.
        If ingesters are not starting and catching up in the selected time range, this panel will be empty.
      |||
    ) +
    $.queryPanel(
      [
        'histogram_avg(sum(rate(cortex_ingest_storage_reader_receive_delay_seconds{%s, phase="starting"}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.ingester)],
        'histogram_quantile(0.99, sum(rate(cortex_ingest_storage_reader_receive_delay_seconds{%s, phase="starting"}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.ingester)],
        'histogram_quantile(0.999, sum(rate(cortex_ingest_storage_reader_receive_delay_seconds{%s, phase="starting"}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.ingester)],
        'histogram_quantile(1.0, sum(rate(cortex_ingest_storage_reader_receive_delay_seconds{%s, phase="starting"}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.ingester)],
      ],
      [
        'avg',
        '99th percentile',
        '99.9th percentile',
        '100th percentile',
      ],
    ) + {
      fieldConfig+: {
        defaults+: { unit: 's' },
      },
    },

  ingestStorageIngesterEndToEndLatencyWhenRunningPanel()::
    $.timeseriesPanel('Kafka end-to-end latency when ingesters are running') +
    $.panelDescription(
      'Kafka end-to-end latency when ingesters are running',
      |||
        Time between writing request by distributor to Kafka and reading the record by ingester, when ingesters are running.
      |||
    ) +
    $.queryPanel(
      [
        'histogram_avg(sum(rate(cortex_ingest_storage_reader_receive_delay_seconds{%s, phase="running"}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.ingester)],
        'histogram_quantile(0.99, sum(rate(cortex_ingest_storage_reader_receive_delay_seconds{%s, phase="running"}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.ingester)],
        'histogram_quantile(0.999, sum(rate(cortex_ingest_storage_reader_receive_delay_seconds{%s, phase="running"}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.ingester)],
        'histogram_quantile(1.0, sum(rate(cortex_ingest_storage_reader_receive_delay_seconds{%s, phase="running"}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.ingester)],
      ],
      [
        'avg',
        '99th percentile',
        '99.9th percentile',
        '100th percentile',
      ],
    ) + {
      fieldConfig+: {
        defaults+: { unit: 's' },
      },
    },

  ingestStorageIngesterEndToEndLatencyOutliersWhenRunningPanel()::
    $.timeseriesPanel('Kafka 100th percentile end-to-end latency when ingesters are running (outliers)') +
    $.panelDescription(
      'Kafka 100th percentile end-to-end latency when ingesters are running (outliers only)',
      |||
        The 100th percentile of the time between writing request by distributor to Kafka and reading the record by ingester,
        when ingesters are running. This panel only shows ingester outliers, to easily spot if the high end-to-end latency
        may be caused by few ingesters.
      |||
    ) +
    $.hiddenLegendQueryPanel(
      |||
        histogram_quantile(1.0, sum by(pod) (rate(cortex_ingest_storage_reader_receive_delay_seconds{%(job_matcher)s, phase="running"}[$__rate_interval])))

        # Add a filter to show only the outliers. We consider an ingester an outlier if its
        # 100th percentile latency is greater than the 200%% of the average 100th of the 10
        # worst ingesters (if there are less than 10 ingesters, then all ingesters will be took
        # in account).
        > scalar(
          avg(
            topk(10,
                histogram_quantile(1.0, sum by(pod) (rate(cortex_ingest_storage_reader_receive_delay_seconds{%(job_matcher)s, phase="running"}[$__rate_interval])))
                > 0
            )
          )
          * 2
        )
      ||| % { job_matcher: $.jobMatcher($._config.job_names.ingester) },
      '{{pod}}',
    ) + {
      fieldConfig+: {
        defaults+: { unit: 's' },
      },
    },

  ingestStorageKafkaProducedRecordsRatePanel(jobName)::
    $.timeseriesPanel('Kafka produced records / sec') +
    $.panelDescription(
      'Kafka produced records / sec',
      'Rate of records synchronously produced to Kafka.',
    ) +
    $.queryPanel([
      |||
        sum(rate(cortex_ingest_storage_writer_produce_requests_total{%(job_matcher)s}[$__rate_interval]))
        -
        (sum(rate(cortex_ingest_storage_writer_produce_failures_total{%(job_matcher)s}[$__rate_interval])) or vector(0))
      ||| % { job_matcher: $.jobMatcher($._config.job_names[jobName]) },
      |||
        sum by(reason) (rate(cortex_ingest_storage_writer_produce_failures_total{%(job_matcher)s}[$__rate_interval]))
      ||| % { job_matcher: $.jobMatcher($._config.job_names[jobName]) },
    ], [
      'success',
      'failed - {{ reason }}',
    ]) +
    $.stack +
    $.aliasColors({
      success: $._colors.success,
    }),

  ingestStorageKafkaProducedRecordsLatencyPanel(jobName)::
    $.timeseriesPanel('Kafka produced records latency') +
    $.panelDescription(
      'Kafka produced records latency',
      |||
        Latency of records synchronously produced to Kafka.
      |||
    ) +
    $.queryPanel(
      [
        'histogram_avg(sum(rate(cortex_ingest_storage_writer_latency_seconds{%s}[$__rate_interval])))' % [$.jobMatcher($._config.job_names[jobName])],
        'histogram_quantile(0.99, sum(rate(cortex_ingest_storage_writer_latency_seconds{%s}[$__rate_interval])))' % [$.jobMatcher($._config.job_names[jobName])],
        'histogram_quantile(0.999, sum(rate(cortex_ingest_storage_writer_latency_seconds{%s}[$__rate_interval])))' % [$.jobMatcher($._config.job_names[jobName])],
        'histogram_quantile(1.0, sum(rate(cortex_ingest_storage_writer_latency_seconds{%s}[$__rate_interval])))' % [$.jobMatcher($._config.job_names[jobName])],
      ],
      [
        'avg',
        '99th percentile',
        '99.9th percentile',
        '100th percentile',
      ],
    ) + {
      fieldConfig+: {
        defaults+: { unit: 's' },
      },
    },

  ingestStorageFetchLastProducedOffsetRequestsPanel(jobMatcher)::
    $.timeseriesPanel('Fetch last produced offset requests / sec') +
    $.panelDescription(
      'Fetch last produced offset requests / sec',
      'Shows rate of successful and failed requests to fetch last produced offset(s).',
    ) +
    $.queryPanel(
      [
        |||
          sum(rate(cortex_ingest_storage_reader_last_produced_offset_requests_total{%s}[$__rate_interval]))
          -
          sum(rate(cortex_ingest_storage_reader_last_produced_offset_failures_total{%s}[$__rate_interval]))
        ||| % [jobMatcher, jobMatcher],
        |||
          sum(rate(cortex_ingest_storage_reader_last_produced_offset_failures_total{%s}[$__rate_interval]))
        ||| % [jobMatcher],
      ],
      [
        'successful',
        'failed',
      ],
    ) + {
      fieldConfig+: {
        defaults+: { unit: 'reqps' },
      },
    } +
    $.aliasColors({ successful: $._colors.success, failed: $._colors.failed }) +
    $.stack,

  ingestStorageFetchLastProducedOffsetLatencyPanel(jobMatcher)::
    $.timeseriesPanel('Fetch last produced offset latency') +
    $.panelDescription(
      'Fetch last produced offset latency',
      |||
        How long does it take to fetch "last produced offset" of partition(s).
      |||
    ) +
    $.queryPanel(
      [
        'histogram_avg(sum(rate(cortex_ingest_storage_reader_last_produced_offset_request_duration_seconds{%s}[$__rate_interval])))' % [jobMatcher],
        'histogram_quantile(0.99, sum(rate(cortex_ingest_storage_reader_last_produced_offset_request_duration_seconds{%s}[$__rate_interval])))' % [jobMatcher],
        'histogram_quantile(0.999, sum(rate(cortex_ingest_storage_reader_last_produced_offset_request_duration_seconds{%s}[$__rate_interval])))' % [jobMatcher],
        'histogram_quantile(1.0, sum(rate(cortex_ingest_storage_reader_last_produced_offset_request_duration_seconds{%s}[$__rate_interval])))' % [jobMatcher],
      ],
      [
        'avg',
        '99th percentile',
        '99.9th percentile',
        '100th percentile',
      ],
    ) + {
      fieldConfig+: {
        defaults+: { unit: 's' },
      },
    },

  ingestStorageStrongConsistencyRequestsPanel(component, jobMatcher)::
    // The unit changes whether the metric is exposed from ingesters (partition-reader) or other components. In the ingesters it's the
    // requests issued by queriers to ingesters, while in other components it's the actual query.
    local unit = if component == 'partition-reader' then 'requests' else 'queries';
    local title = '%s with strong read consistency / sec' % (std.asciiUpper(std.substr(unit, 0, 1)) + std.substr(unit, 1, std.length(unit) - 1));

    $.timeseriesPanel(title) +
    $.panelDescription(
      title,
      'Shows rate of %(unit)s with strong read consistency, and rate of failed %(unit)s with strong read consistency.' % {
        unit: unit,
      },
    ) +
    $.queryPanel(
      [
        |||
          sum(rate(cortex_ingest_storage_strong_consistency_requests_total{component="%(component)s", %(jobMatcher)s}[$__rate_interval]))
          -
          sum(rate(cortex_ingest_storage_strong_consistency_failures_total{component="%(component)s", %(jobMatcher)s}[$__rate_interval]))
        ||| % { jobMatcher: jobMatcher, component: component },
        |||
          sum(rate(cortex_ingest_storage_strong_consistency_failures_total{component="%(component)s", %(jobMatcher)s}[$__rate_interval]))
        ||| % { jobMatcher: jobMatcher, component: component },
      ],
      [
        'successful',
        'failed',
      ],
    ) + {
      fieldConfig+: {
        defaults+: { unit: 'reqps' },
      },
    } +
    $.aliasColors({ successful: $._colors.success, failed: $._colors.failed }) +
    $.stack,

  ingestStorageStrongConsistencyWaitLatencyPanel(component, jobMatcher)::
    $.timeseriesPanel('Strong read consistency queries — wait latency') +
    $.panelDescription(
      'Strong read consistency queries — wait latency',
      'How long does the request wait to guarantee strong read consistency.',
    ) +
    $.queryPanel(
      [
        'histogram_avg(sum(rate(cortex_ingest_storage_strong_consistency_wait_duration_seconds{component="%(component)s", %(jobMatcher)s}[$__rate_interval])))' % { component: component, jobMatcher: jobMatcher },
        'histogram_quantile(0.99, sum(rate(cortex_ingest_storage_strong_consistency_wait_duration_seconds{component="%(component)s", %(jobMatcher)s}[$__rate_interval])))' % { component: component, jobMatcher: jobMatcher },
        'histogram_quantile(0.999, sum(rate(cortex_ingest_storage_strong_consistency_wait_duration_seconds{component="%(component)s", %(jobMatcher)s}[$__rate_interval])))' % { component: component, jobMatcher: jobMatcher },
        'histogram_quantile(1.0, sum(rate(cortex_ingest_storage_strong_consistency_wait_duration_seconds{component="%(component)s", %(jobMatcher)s}[$__rate_interval])))' % { component: component, jobMatcher: jobMatcher },
      ],
      [
        'avg',
        '99th percentile',
        '99.9th percentile',
        '100th percentile',
      ],
    ) + {
      fieldConfig+: {
        defaults+: { unit: 's' },
      },
    },

  withExemplars(queryPanel)::
    queryPanel {
      targets: [
        target { exemplar: true }
        for target in queryPanel.targets
      ],
    },
}
