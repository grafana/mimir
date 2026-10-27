local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-reads.json';

{
  local lib =
    (import 'dashboard-utils.libsonnet') +
    (import 'dashboard-queries.libsonnet') +
    { _config:: $._config + $._group_config },

  // Rows of the Reads dashboard, shown in the Reads tab of the dashboard.
  grafanaPanels+:: {
    readsContent:
      (if lib._config.show_dashboard_descriptions.reads then [
         lib.gridRow('Reads dashboard description', [
           lib.rowHelperText(|||
             <p>
               This dashboard shows health metrics for the read path.
               It is broken into sections for each service on the read path, and organized by the order in which the read request flows.
               <br/>
               Incoming queries travel from the gateway → query frontend → query scheduler → querier → ingester and/or store-gateway (depending on the time range of the query).
               <br/>
               For each service, there are 3 panels showing (1) requests per second to that service, (2) average, median, and p99 latency of requests to that service, and (3) p99 latency of requests to each instance of that service.
             </p>
             <p>
               The dashboard also shows metrics for the 4 optional caches that can be deployed:
               the query results cache, the metadata cache, the chunks cache, and the index cache.
               <br/>
               These panels will show “no data” if the caches are not deployed.
             </p>
             <p>
               Lastly, it also includes metrics for how the ingester and store-gateway interact with object storage.
             </p>
           |||),
         ], height=5, showTitle=false),
       ] else [])
      + [
        lib.gridRow('Headlines', [
          local addRuleEvalRate(q) = {
            local ruleEvalRate = ' + sum(rate(cortex_prometheus_rule_evaluations_total{' + lib.jobMatcher(lib._config.job_names.ruler) + '}[$__rate_interval]))',
            classic: q.classic + ruleEvalRate,
            native: q.native + ruleEvalRate,
          };
          lib.gridStatPanel('Instant queries / sec', addRuleEvalRate(lib.queries.query_frontend.ncInstantQueriesPerSecond), unit='reqps') +
          lib.panelDescription(
            'Instant queries per second',
            |||
              Rate of instant queries per second being made to the system.
              Includes both queries made to the <tt>/prometheus</tt> API as
              well as queries from the ruler.
            |||
          ),
          lib.gridStatPanel('Range queries / sec', lib.queries.query_frontend.ncRangeQueriesPerSecond, unit='reqps') +
          lib.panelDescription(
            'Range queries per second',
            |||
              Rate of range queries per second being made to
              %(product)s via the <tt>/prometheus</tt> API.
            ||| % lib._config
          ),
          lib.gridStatPanel('Label names queries / sec', lib.queries.query_frontend.ncLabelNamesQueriesPerSecond, unit='reqps') +
          lib.panelDescription(
            '"Label names" queries per second',
            |||
              Rate of "label names" endpoint queries per second being made to
              %(product)s via the <tt>/prometheus</tt> API.
            ||| % lib._config
          ),
          lib.gridStatPanel('Label values queries / sec', lib.queries.query_frontend.ncLabelValuesQueriesPerSecond, unit='reqps') +
          lib.panelDescription(
            '"Label values" queries per second',
            |||
              Rate of specific "label values" endpoint queries per second being made to
              %(product)s via the <tt>/prometheus</tt> API.
            ||| % lib._config
          ),
          lib.gridStatPanel('Series queries / sec', lib.queries.query_frontend.ncSeriesQueriesPerSecond, unit='reqps') +
          lib.panelDescription(
            'Series queries per second',
            |||
              Rate of series queries per second being made to
              %(product)s via the <tt>/prometheus</tt> API.
            ||| % lib._config
          ),
        ], height=3, showTitle=false),
      ]
      + (if lib._config.gateway_enabled then [
           lib.gridRow('Gateway', [
             lib.timeseriesPanel('Requests / sec') +
             lib.qpsPanelNativeHistogram(lib.queries.gateway.requestsPerSecondMetric, lib.queries.gateway.readRequestsPerSecondSelector),
             lib.timeseriesPanel('Latency') +
             lib.latencyRecordingRulePanelNativeHistogram(lib.queries.gateway.requestsPerSecondMetric, lib.jobSelector(lib._config.job_names.gateway) + [utils.selector.re('route', lib.queries.read_http_routes_regex)]),
             lib.timeseriesPanel('Per %s p99 latency' % lib._config.per_instance_label) +
             lib.perInstanceLatencyPanelNativeHistogram(
               '0.99',
               lib.queries.gateway.requestsPerSecondMetric,
               lib.jobSelector(lib._config.job_names.gateway) + [utils.selector.re('route', lib.queries.read_http_routes_regex)],
             ),
           ]),
         ] else [])
      + [
        lib.fromLegacyRow(row)
        for row in lib.commonReadsDashboardsRows(
          queryFrontendJobName=lib._config.job_names.query_frontend,
          querySchedulerJobName=lib._config.job_names.query_scheduler,
          querierJobName=lib._config.job_names.querier,
          queryRoutesRegex=lib.queries.read_http_routes_regex,
          queryPathDescription='main query path (ie. excluding the ruler query path, if enabled)',
          showQueryCacheRow=true,
        )
      ]
      + (if lib._config.gateway_enabled && lib._config.autoscaling.gateway.enabled then [lib.fromLegacyRow(lib.cpuAndMemoryBasedAutoScalingRow('Gateway'))] else [])
      + (if lib._config.autoscaling.query_frontend.enabled then [lib.fromLegacyRow(lib.cpuAndMemoryBasedAutoScalingRow('Query-frontend'))] else [])
      + (if lib._config.autoscaling.querier.enabled then [
           lib.gridRow('Querier – autoscaling', [
             local title = 'Replicas';
             lib.timeseriesPanel(title) +
             lib.queryPanel(
               [
                 |||
                   max by (scaletargetref_name) (
                     kube_horizontalpodautoscaler_spec_max_replicas{%(namespace_matcher)s, horizontalpodautoscaler=~"%(hpa_name)s"}
                     # Add the scaletargetref_name label which is more readable than "kube-hpa-..."
                     * on (%(cluster_labels)s, horizontalpodautoscaler) group_left (scaletargetref_name)
                       group by (%(cluster_labels)s, horizontalpodautoscaler, scaletargetref_name) (kube_horizontalpodautoscaler_info{%(namespace_matcher)s, horizontalpodautoscaler=~"%(hpa_name)s"})
                   )
                 ||| % {
                   namespace_matcher: lib.namespaceMatcher(),
                   cluster_labels: std.join(', ', lib._config.cluster_labels),
                   hpa_name: lib._config.autoscaling.querier.hpa_name,
                 },
                 |||
                   max by (scaletargetref_name) (
                     kube_horizontalpodautoscaler_status_current_replicas{%(namespace_matcher)s, horizontalpodautoscaler=~"%(hpa_name)s"}
                     # Add the scaletargetref_name label which is more readable than "kube-hpa-..."
                     * on (%(cluster_labels)s, horizontalpodautoscaler) group_left (scaletargetref_name)
                       group by (%(cluster_labels)s, horizontalpodautoscaler, scaletargetref_name) (kube_horizontalpodautoscaler_info{%(namespace_matcher)s, horizontalpodautoscaler=~"%(hpa_name)s"})
                   )
                 ||| % {
                   namespace_matcher: lib.namespaceMatcher(),
                   cluster_labels: std.join(', ', lib._config.cluster_labels),
                   hpa_name: lib._config.autoscaling.querier.hpa_name,
                 },
                 |||
                   max by (scaletargetref_name) (
                     kube_horizontalpodautoscaler_spec_min_replicas{%(namespace_matcher)s, horizontalpodautoscaler=~"%(hpa_name)s"}
                     # Add the scaletargetref_name label which is more readable than "kube-hpa-..."
                     * on (%(cluster_labels)s, horizontalpodautoscaler) group_left (scaletargetref_name)
                       group by (%(cluster_labels)s, horizontalpodautoscaler, scaletargetref_name) (kube_horizontalpodautoscaler_info{%(namespace_matcher)s, horizontalpodautoscaler=~"%(hpa_name)s"})
                   )
                 ||| % {
                   namespace_matcher: lib.namespaceMatcher(),
                   cluster_labels: std.join(', ', lib._config.cluster_labels),
                   hpa_name: lib._config.autoscaling.querier.hpa_name,
                 },
               ],
               [
                 'Max {{ scaletargetref_name }}',
                 'Current {{ scaletargetref_name }}',
                 'Min {{ scaletargetref_name }}',
               ],
             ) +
             lib.panelDescription(
               title,
               |||
                 The maximum, and current number of querier replicas.
               |||
             ) +
             {
               fieldConfig+: {
                 overrides: [
                   lib.overrideField('byRegexp', '/Max .+/', [
                     lib.overrideProperty('custom.fillOpacity', 0),
                     lib.overrideProperty('custom.lineStyle', { fill: 'dash' }),
                   ]),
                   lib.overrideField('byRegexp', '/Current .+/', [
                     lib.overrideProperty('custom.fillOpacity', 0),
                   ]),
                   lib.overrideField('byRegexp', '/Min .+/', [
                     lib.overrideProperty('custom.fillOpacity', 0),
                     lib.overrideProperty('custom.lineStyle', { fill: 'dash' }),
                   ]),
                 ],
               },
             },
             local title = 'Scaling metric (desired replicas)';
             lib.timeseriesPanel(title) +
             lib.queryPanel(
               [
                 |||
                   sum by (scaler) (
                     label_replace(
                       keda_scaler_metrics_value{%(cluster_label)s=~"$cluster", exported_namespace=~"$namespace"},
                       "namespace", "$1", "exported_namespace", "(.*)"
                     )
                     /
                     on(%(aggregation_labels)s, scaledObject, metric) group_left
                     max by (%(aggregation_labels)s, scaledObject, metric) (
                       label_replace(label_replace(
                           kube_horizontalpodautoscaler_spec_target_metric{%(namespace)s, horizontalpodautoscaler=~"%(hpa_name)s"},
                           "metric", "$1", "metric_name", "(.+)"
                       ), "scaledObject", "$1", "horizontalpodautoscaler", "%(hpa_prefix)s(.*)")
                     )
                   )
                 ||| % {
                   aggregation_labels: lib._config.alert_aggregation_labels,
                   cluster_label: lib._config.per_cluster_label,
                   hpa_prefix: lib._config.autoscaling_hpa_prefix,
                   hpa_name: lib._config.autoscaling.querier.hpa_name,
                   namespace: lib.namespaceMatcher(),
                 },
               ], [
                 '{{ scaler }}',
               ]
             ) +
             lib.panelDescription(
               title,
               |||
                 This panel shows the result scaling metric exposed by KEDA divided by the target/threshold used.
                 It should represent the desired number of replicas, ignoring the min/max constraints which are applied later.
               |||
             ),
             local title = 'Autoscaler failures rate';
             lib.timeseriesPanel(title) +
             lib.queryPanel(
               lib.filterKedaScalerErrorsByHPA(lib._config.autoscaling.querier.hpa_name),
               '{{scaler}} failures'
             ) +
             lib.panelDescription(
               title,
               |||
                 The rate of failures in the KEDA custom metrics API server. Whenever an error occurs, the KEDA custom
                 metrics server is unable to query the scaling metric from Prometheus so the autoscaler wouldn't work properly.
               |||
             ),
           ]),
         ] else [])
      + (if lib._config.autoscaling.store_gateway.enabled then [
           lib.gridRow('Store-gateway – autoscaling', [
             lib.autoScalingActualReplicas('store_gateway') + { title: 'Replicas (leader zone)' } +
             lib.panelDescription(
               'Replicas (leader zone)',
               |||
                 The minimum, maximum, and current number of replicas for the leader zone of store-gateways.
                 Other zones scale to follow this zone (with delay for downscale).
               |||
             ),
             lib.timeseriesPanel('Replicas') +
             lib.panelDescription('Replicas', 'Number of store-gateway replicas per zone.') +
             lib.queryPanel(
               [
                 'sum by (%s) (up{%s})' % [lib._config.per_job_label, lib.jobMatcher(lib._config.job_names.store_gateway)],
               ],
               [
                 '{{ %(per_job_label)s }}' % lib._config.per_job_label,
               ],
             ),
             lib.autoScalingDesiredReplicasByValueScalingMetricPanel('store_gateway', '', '') + { title: 'Desired replicas (leader zone)' },
             lib.autoScalingFailuresPanel('store_gateway') + { title: 'Autoscaler failures rate' },
           ]),
         ] else [])
      + [
        lib.fromLegacyRow(lib.kvStoreRow('Store-gateway – key-value store for store-gateways ring', 'store_gateway', 'store-gateway')),
        lib.gridRow('Memcached – block index cache (store-gateway accesses)', [  // Resembles thanosMemcachedCache
          lib.timeseriesPanel('Requests / sec') +
          lib.queryPanel(
            |||
              sum by(operation) (
                rate(
                  thanos_cache_operations_total{
                    component="store-gateway",
                    name="index-cache",
                    %s
                  }[$__rate_interval]
                )
              )
            ||| % lib.jobMatcher(lib._config.job_names.store_gateway),
            '{{operation}}'
          ) +
          lib.stack +
          { fieldConfig+: { defaults+: { unit: 'ops' } } },
          lib.timeseriesPanel('Latency (getmulti)') +
          lib.ncLatencyPanel(
            'thanos_cache_operation_duration_seconds',
            |||
              %s, operation="getmulti", component="store-gateway", name="index-cache"
            ||| % lib.jobMatcher(lib._config.job_names.store_gateway)
          ),
          lib.timeseriesPanel('Hit ratio') +
          lib.queryPanel(
            |||
              sum by(item_type) (
                rate(
                  thanos_store_index_cache_hits_total{
                    component="store-gateway",
                    %s
                  }[$__rate_interval]
                )
              )
              /
              sum by(item_type) (
                rate(
                  thanos_store_index_cache_requests_total{
                    component="store-gateway",
                    %s
                  }[$__rate_interval]
                )
              )
            ||| % [
              lib.jobMatcher(lib._config.job_names.store_gateway),
              lib.jobMatcher(lib._config.job_names.store_gateway),
            ],
            '{{item_type}}'
          ) +
          { fieldConfig+: { defaults+: { unit: 'percentunit' } } } +
          lib.panelDescription(
            'Hit ratio',
            |||
              Even if you do not set up memcached for the blocks index cache, you will still see data in this panel because the store-gateway by default has an
              in-memory blocks index cache.
            |||
          ),
        ]),
        lib.fromLegacyRow(
          lib.thanosMemcachedCache(
            'Memcached – chunks cache (store-gateway accesses)',
            lib._config.job_names.store_gateway,
            'store-gateway',
            'chunks-cache'
          )
        ),
        lib.fromLegacyRow(
          lib.thanosMemcachedCache(
            'Memcached – metadata cache (store-gateway accesses)',
            lib._config.job_names.store_gateway,
            'store-gateway',
            'metadata-cache'
          )
        ),
        lib.fromLegacyRow(
          lib.thanosMemcachedCache(
            'Memcached – metadata cache (querier accesses)',
            lib._config.job_names.querier,
            'querier',
            'metadata-cache'
          )
        ),
      ]
      // Object store metrics for the store-gateway.
      + [lib.fromLegacyRow(row) for row in lib.getObjectStoreRows('Blocks object store (store-gateway accesses)', 'store-gateway')]
      // Object store metrics for the querier.
      + [lib.fromLegacyRow(row) for row in lib.getObjectStoreRows('Blocks object store (querier accesses)', 'querier')]
      + (if lib._config.show_reactive_limiter_panels then [
           lib.gridRow('Instance Limits', [
             lib.timeseriesPanel('Ingester per %s queued requests' % lib._config.per_instance_label) +
             lib.hiddenLegendQueryPanel(
               'sum by (%s) (cortex_ingester_reactive_limiter_queued_requests{%s, request_type="read"})'
               % [lib._config.per_instance_label, lib.jobMatcher(lib._config.job_names.ingester)], '',
             ) +
             { fieldConfig+: { defaults+: { unit: 'req' } } },
             lib.timeseriesPanel('Ingester per %s inflight requests' % lib._config.per_instance_label) +
             lib.hiddenLegendQueryPanel(
               'sum by (%s) (cortex_ingester_reactive_limiter_inflight_requests{%s, request_type="read"})'
               % [lib._config.per_instance_label, lib.jobMatcher(lib._config.job_names.ingester)], '',
             ) +
             { fieldConfig+: { defaults+: { unit: 'req' } } },
             lib.timeseriesPanel('Ingester per %s inflight request limit' % lib._config.per_instance_label) +
             lib.hiddenLegendQueryPanel(
               'sum by (%s) (cortex_ingester_reactive_limiter_inflight_limit{%s, request_type="read"})'
               % [lib._config.per_instance_label, lib.jobMatcher(lib._config.job_names.ingester)], '',
             ) +
             { fieldConfig+: { defaults+: { unit: 'req' } } },
             lib.timeseriesPanel('Rejected ingester requests') +
             lib.queryPanel(
               'sum by (reason) (rate(cortex_ingester_instance_rejected_requests_total{%s, reason="ingester_max_inflight_read_requests"}[$__rate_interval]))'
               % lib.jobMatcher(lib._config.job_names.ingester),
               '{{reason}}',
             ) +
             { fieldConfig+: { defaults+: { unit: 'reqps' } } },
           ]),
         ] else []),
  },

  // The Reads dashboard puts the Reads, Networking and Resources content in a tab each, with the
  // variables of the Reads dashboard: every query matches them with =~, so they work for all tabs.
  grafanaDashboards+: {
    [filename]:
      assert std.md5(filename) == 'e327503188913dc38ad571c647eef643' : 'UID of the dashboard has changed, please update references to dashboard.';
      (import 'dashboard-v2.libsonnet').tabbedDashboard(
        (lib.dashboard('Reads') + { uid: std.md5(filename) })
        .addClusterSelectorTemplates()
        .addShowNativeLatencyVariable(lib.latencyVariableDefault()),
        [{ key: 'reads', title: 'Reads', rows: $.grafanaPanels.readsContent }]
        + (
          if !lib._config.resources_dashboards_enabled then [] else [
            { key: 'networking', title: 'Networking', rows: $.grafanaPanels.readsNetworkingContent },
            { key: 'resources', title: 'Resources', rows: $.grafanaPanels.readsResourcesContent },
          ]
        ),
      ),
  },
}
