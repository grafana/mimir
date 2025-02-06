local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-reads.json';

(import 'dashboard-utils.libsonnet') {
  [filename]:
    assert std.md5(filename) == 'e327503188913dc38ad571c647eef643' : 'UID of the dashboard has changed, please update references to dashboard.';
    ($.dashboard('Reads') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates()
    .addShowNativeLatencyVariable()
    .addRowIf(
      $._config.show_dashboard_descriptions.reads,
      ($.row('Reads dashboard description') { height: '175px', showTitle: false })
      .addPanel(
        $.textPanel('', |||
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
      )
    )
    .addRow(
      ($.row('Headlines') +
       {
         height: '100px',
         showTitle: false,
       })
      .addPanel(
        local addRuleEvalRate(q) = {
          local ruleEvalRate = ' + sum(rate(cortex_prometheus_rule_evaluations_total{' + $.jobMatcher($._config.job_names.ruler) + '}[$__rate_interval]))',
          classic: q.classic + ruleEvalRate,
          native: q.native + ruleEvalRate,
        };
        $.panel('Instant queries / sec') +
        $.statPanel(addRuleEvalRate($.queries.query_frontend.ncInstantQueriesPerSecond), format='reqps') +
        $.panelDescription(
          'Instant queries per second',
          |||
            Rate of instant queries per second being made to the system.
            Includes both queries made to the <tt>/prometheus</tt> API as
            well as queries from the ruler.
          |||
        ),
      )
      .addPanel(
        $.panel('Range queries / sec') +
        $.statPanel($.queries.query_frontend.ncRangeQueriesPerSecond, format='reqps') +
        $.panelDescription(
          'Range queries per second',
          |||
            Rate of range queries per second being made to
            %(product)s via the <tt>/prometheus</tt> API.
          ||| % $._config
        ),
      )
      .addPanel(
        $.panel('Label names queries / sec') +
        $.statPanel($.queries.query_frontend.ncLabelNamesQueriesPerSecond, format='reqps') +
        $.panelDescription(
          '"Label names" queries per second',
          |||
            Rate of "label names" endpoint queries per second being made to
            %(product)s via the <tt>/prometheus</tt> API.
          ||| % $._config
        ),
      )
      .addPanel(
        $.panel('Label values queries / sec') +
        $.statPanel($.queries.query_frontend.ncLabelValuesQueriesPerSecond, format='reqps') +
        $.panelDescription(
          '"Label values" queries per second',
          |||
            Rate of specific "label values" endpoint queries per second being made to
            %(product)s via the <tt>/prometheus</tt> API.
          ||| % $._config
        ),
      )
      .addPanel(
        $.panel('Series queries / sec') +
        $.statPanel($.queries.query_frontend.ncSeriesQueriesPerSecond, format='reqps') +
        $.panelDescription(
          'Series queries per second',
          |||
            Rate of series queries per second being made to
            %(product)s via the <tt>/prometheus</tt> API.
          ||| % $._config
        ),
      )
    )
    .addRowIf(
      $._config.gateway_enabled,
      $.row('Gateway')
      .addPanel(
        $.timeseriesPanel('Requests / sec') +
        $.qpsPanelNativeHistogram($.queries.gateway.requestsPerSecondMetric, $.queries.gateway.readRequestsPerSecondSelector)
      )
      .addPanel(
        $.timeseriesPanel('Latency') +
        $.latencyRecordingRulePanelNativeHistogram($.queries.gateway.requestsPerSecondMetric, $.jobSelector($._config.job_names.gateway) + [utils.selector.re('route', $.queries.read_http_routes_regex)])
      )
      .addPanel(
        $.timeseriesPanel('Per %s p99 latency' % $._config.per_instance_label) +
        $.perInstanceLatencyPanelNativeHistogram(
          '0.99',
          $.queries.gateway.requestsPerSecondMetric,
          $.jobSelector($._config.job_names.gateway) + [utils.selector.re('route', $.queries.read_http_routes_regex)],
        )
      )
    )
    .addRows($.commonReadsDashboardsRows(
      queryFrontendJobName=$._config.job_names.query_frontend,
      querySchedulerJobName=$._config.job_names.query_scheduler,
      querierJobName=$._config.job_names.querier,
      queryRoutesRegex=$.queries.read_http_routes_regex,
      showQueryCacheRow=true,
    ))
    .addRow(
      $.row('Ingester')
      .addPanel(
        $.timeseriesPanel('Requests / sec') +
        $.qpsPanelNativeHistogram($.queries.ingester.requestsPerSecondMetric, $.queries.ingester.readRequestsPerSecondSelector)
      )
      .addPanel(
        $.timeseriesPanel('Latency') +
        $.latencyRecordingRulePanelNativeHistogram($.queries.ingester.requestsPerSecondMetric, $.jobSelector($._config.job_names.ingester) + [utils.selector.re('route', $._config.ingester_read_path_routes_regex)])
      )
      .addPanel(
        $.timeseriesPanel('Per %s p99 latency' % $._config.per_instance_label) +
        $.perInstanceLatencyPanelNativeHistogram(
          '0.99',
          $.queries.ingester.requestsPerSecondMetric,
          $.jobSelector($._config.job_names.ingester) + [utils.selector.re('route', $._config.ingester_read_path_routes_regex)],
        ),
      )
    )
    .addRow(
      $.row('Store-gateway')
      .addPanel(
        $.timeseriesPanel('Requests / sec') +
        $.qpsPanelNativeHistogram($.queries.store_gateway.requestsPerSecondMetric, $.queries.store_gateway.readRequestsPerSecondSelector)
      )
      .addPanel(
        $.timeseriesPanel('Latency') +
        $.latencyRecordingRulePanelNativeHistogram($.queries.store_gateway.requestsPerSecondMetric, $.jobSelector($._config.job_names.store_gateway) + [utils.selector.re('route', $._config.store_gateway_read_path_routes_regex)])
      )
      .addPanel(
        $.timeseriesPanel('Per %s p99 latency' % $._config.per_instance_label) +
        $.perInstanceLatencyPanelNativeHistogram(
          '0.99',
          $.queries.store_gateway.requestsPerSecondMetric,
          $.jobSelector($._config.job_names.store_gateway) + [utils.selector.re('route', $._config.store_gateway_read_path_routes_regex)],
        ),
      )
    )
    .addRowIf(
      $._config.gateway_enabled && $._config.autoscaling.gateway.enabled,
      $.cpuAndMemoryBasedAutoScalingRow('Gateway'),
    )
    .addRowIf(
      $._config.autoscaling.query_frontend.enabled,
      $.cpuAndMemoryBasedAutoScalingRow('Query-frontend'),
    )
    .addRowIf(
      $._config.autoscaling.querier.enabled,
      $.row('Querier – autoscaling')
      .addPanel(
        local title = 'Replicas';
        $.timeseriesPanel(title) +
        $.queryPanel(
          [
            |||
              max by (scaletargetref_name) (
                kube_horizontalpodautoscaler_spec_max_replicas{%(namespace_matcher)s, horizontalpodautoscaler=~"%(hpa_name)s"}
                # Add the scaletargetref_name label which is more readable than "kube-hpa-..."
                * on (%(cluster_labels)s, horizontalpodautoscaler) group_left (scaletargetref_name)
                  group by (%(cluster_labels)s, horizontalpodautoscaler, scaletargetref_name) (kube_horizontalpodautoscaler_info{%(namespace_matcher)s, horizontalpodautoscaler=~"%(hpa_name)s"})
              )
            ||| % {
              namespace_matcher: $.namespaceMatcher(),
              cluster_labels: std.join(', ', $._config.cluster_labels),
              hpa_name: $._config.autoscaling.querier.hpa_name,
            },
            |||
              max by (scaletargetref_name) (
                kube_horizontalpodautoscaler_status_current_replicas{%(namespace_matcher)s, horizontalpodautoscaler=~"%(hpa_name)s"}
                # Add the scaletargetref_name label which is more readable than "kube-hpa-..."
                * on (%(cluster_labels)s, horizontalpodautoscaler) group_left (scaletargetref_name)
                  group by (%(cluster_labels)s, horizontalpodautoscaler, scaletargetref_name) (kube_horizontalpodautoscaler_info{%(namespace_matcher)s, horizontalpodautoscaler=~"%(hpa_name)s"})
              )
            ||| % {
              namespace_matcher: $.namespaceMatcher(),
              cluster_labels: std.join(', ', $._config.cluster_labels),
              hpa_name: $._config.autoscaling.querier.hpa_name,
            },
            |||
              max by (scaletargetref_name) (
                kube_horizontalpodautoscaler_spec_min_replicas{%(namespace_matcher)s, horizontalpodautoscaler=~"%(hpa_name)s"}
                # Add the scaletargetref_name label which is more readable than "kube-hpa-..."
                * on (%(cluster_labels)s, horizontalpodautoscaler) group_left (scaletargetref_name)
                  group by (%(cluster_labels)s, horizontalpodautoscaler, scaletargetref_name) (kube_horizontalpodautoscaler_info{%(namespace_matcher)s, horizontalpodautoscaler=~"%(hpa_name)s"})
              )
            ||| % {
              namespace_matcher: $.namespaceMatcher(),
              cluster_labels: std.join(', ', $._config.cluster_labels),
              hpa_name: $._config.autoscaling.querier.hpa_name,
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
            The maximum, and current number of querier replicas.
          |||
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
        local title = 'Scaling metric (desired replicas)';
        $.timeseriesPanel(title) +
        $.queryPanel(
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
              aggregation_labels: $._config.alert_aggregation_labels,
              cluster_label: $._config.per_cluster_label,
              hpa_prefix: $._config.autoscaling_hpa_prefix,
              hpa_name: $._config.autoscaling.querier.hpa_name,
              namespace: $.namespaceMatcher(),
            },
          ], [
            '{{ scaler }}',
          ]
        ) +
        $.panelDescription(
          title,
          |||
            This panel shows the result scaling metric exposed by KEDA divided by the target/threshold used.
            It should represent the desired number of replicas, ignoring the min/max constraints which are applied later.
          |||
        )
      )
      .addPanel(
        local title = 'Autoscaler failures rate';
        $.timeseriesPanel(title) +
        $.queryPanel(
          $.filterKedaScalerErrorsByHPA($._config.autoscaling.querier.hpa_name),
          '{{scaler}} failures'
        ) +
        $.panelDescription(
          title,
          |||
            The rate of failures in the KEDA custom metrics API server. Whenever an error occurs, the KEDA custom
            metrics server is unable to query the scaling metric from Prometheus so the autoscaler wouldn't work properly.
          |||
        ),
      )
    )
    .addRowIf(
      $._config.autoscaling.store_gateway.enabled,
      $.row('Store-gateway – autoscaling')
      .addPanel(
        $.autoScalingActualReplicas('store_gateway') + { title: 'Replicas (leader zone)' } +
        $.panelDescription(
          'Replicas (leader zone)',
          |||
            The minimum, maximum, and current number of replicas for the leader zone of store-gateways.
            Other zones scale to follow this zone (with delay for downscale).
          |||
        )
      )
      .addPanel(
        $.timeseriesPanel('Replicas') +
        $.panelDescription('Replicas', 'Number of store-gateway replicas per zone.') +
        $.queryPanel(
          [
            'sum by (%s) (up{%s})' % [$._config.per_job_label, $.jobMatcher($._config.job_names.store_gateway)],
          ],
          [
            '{{ %(per_job_label)s }}' % $._config.per_job_label,
          ],
        ),
      )
      .addPanel(
        $.autoScalingDesiredReplicasByValueScalingMetricPanel('store_gateway', '', '') + { title: 'Desired replicas (leader zone)' }
      )
      .addPanel(
        $.autoScalingFailuresPanel('store_gateway') + { title: 'Autoscaler failures rate' }
      ),
    )
    .addRow(
      $.kvStoreRow('Store-gateway – key-value store for store-gateways ring', 'store_gateway', 'store-gateway')
    )
    .addRow(
      $.row('Memcached – block index cache (store-gateway accesses)')  // Resembles thanosMemcachedCache
      .addPanel(
        $.timeseriesPanel('Requests / sec') +
        $.queryPanel(
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
          ||| % $.jobMatcher($._config.job_names.store_gateway),
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
              %s,
              operation="getmulti",
              component="store-gateway",
              name="index-cache"
            }
          ||| % $.jobMatcher($._config.job_names.store_gateway)
        )
      )
      .addPanel(
        $.timeseriesPanel('Hit ratio') +
        $.queryPanel(
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
            $.jobMatcher($._config.job_names.store_gateway),
            $.jobMatcher($._config.job_names.store_gateway),
          ],
          '{{item_type}}'
        ) +
        { fieldConfig+: { defaults+: { unit: 'percentunit' } } } +
        $.panelDescription(
          'Hit ratio',
          |||
            Even if you do not set up memcached for the blocks index cache, you will still see data in this panel because the store-gateway by default has an
            in-memory blocks index cache.
          |||
        ),
      )
    )
    .addRow(
      $.thanosMemcachedCache(
        'Memcached – chunks cache (store-gateway accesses)',
        $._config.job_names.store_gateway,
        'store-gateway',
        'chunks-cache'
      )
    )
    .addRow(
      $.thanosMemcachedCache(
        'Memcached – metadata cache (store-gateway accesses)',
        $._config.job_names.store_gateway,
        'store-gateway',
        'metadata-cache'
      )
    )
    .addRow(
      $.thanosMemcachedCache(
        'Memcached – metadata cache (querier accesses)',
        $._config.job_names.querier,
        'querier',
        'metadata-cache'
      )
    )
    // Object store metrics for the store-gateway.
    .addRows(
      $.getObjectStoreRows('Blocks object store (store-gateway accesses)', 'store-gateway')
    )
    // Object store metrics for the querier.
    .addRows(
      $.getObjectStoreRows('Blocks object store (querier accesses)', 'querier')
    )
    .addRow(
      $.row('Instance Limits')
      .addPanel(
        $.timeseriesPanel('Ingester per %s blocked requests' % $._config.per_instance_label) +
        $.hiddenLegendQueryPanel(
          'sum by (%s) (cortex_ingester_adaptive_limiter_blocked_requests{%s, request_type="read"})'
          % [$._config.per_instance_label, $.jobMatcher($._config.job_names.ingester)], '',
        ) +
        { fieldConfig+: { defaults+: { unit: 'req' } } }
      )
      .addPanel(
        $.timeseriesPanel('Ingester per %s inflight requests' % $._config.per_instance_label) +
        $.hiddenLegendQueryPanel(
          'sum by (%s) (cortex_ingester_adaptive_limiter_inflight_requests{%s, request_type="read"})'
          % [$._config.per_instance_label, $.jobMatcher($._config.job_names.ingester)], '',
        ) +
        { fieldConfig+: { defaults+: { unit: 'req' } } }
      )
      .addPanel(
        $.timeseriesPanel('Ingester %s pod inflight request limit' % $._config.per_instance_label) +
        $.hiddenLegendQueryPanel(
          'sum by (%s) (cortex_ingester_adaptive_limiter_inflight_limit{%s, request_type="read"})'
          % [$._config.per_instance_label, $.jobMatcher($._config.job_names.ingester)], '',
        ) +
        { fieldConfig+: { defaults+: { unit: 'req' } } }
      )
      .addPanel(
        $.timeseriesPanel('Rejected ingester requests') +
        $.queryPanel(
          'sum by (reason) (rate(cortex_ingester_instance_rejected_requests_total{%s, reason="ingester_max_inflight_read_requests"}[$__rate_interval]))'
          % $.jobMatcher($._config.job_names.ingester),
          '{{reason}}',
        ) +
        { fieldConfig+: { defaults+: { unit: 'reqps' } } }
      ),
    ),
}
