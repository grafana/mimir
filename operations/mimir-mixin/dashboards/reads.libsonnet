local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-reads.json';

(import 'dashboard-utils.libsonnet') {
  [filename]:
    assert std.md5(filename) == 'e327503188913dc38ad571c647eef643' : 'UID of the dashboard has changed, please update references to dashboard.';
    ($.dashboard('Reads') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates()
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
        $.panel('Instant queries / sec') +
        $.statPanel(|||
          sum(
            rate(
              cortex_request_duration_seconds_count{
                %(queryFrontend)s,
                route=~"(prometheus|api_prom)_api_v1_query"
              }[$__rate_interval]
            )
            or
            rate(
              cortex_prometheus_rule_evaluations_total{
                %(ruler)s
              }[$__rate_interval]
            )
          )
        ||| % {
          queryFrontend: $.jobMatcher($._config.job_names.query_frontend),
          ruler: $.jobMatcher($._config.job_names.ruler),
        }, format='reqps') +
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
        $.statPanel($.queries.query_frontend.rangeQueriesPerSecond, format='reqps') +
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
        $.statPanel($.queries.query_frontend.labelNamesQueriesPerSecond, format='reqps') +
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
        $.statPanel($.queries.query_frontend.labelValuesQueriesPerSecond, format='reqps') +
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
        $.statPanel($.queries.query_frontend.seriesQueriesPerSecond, format='reqps') +
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
        $.qpsPanel($.queries.gateway.readRequestsPerSecond)
      )
      .addPanel(
        $.timeseriesPanel('Latency') +
        $.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector($._config.job_names.gateway) + [utils.selector.re('route', $.queries.read_http_routes_regex)])
      )
      .addPanel(
        $.timeseriesPanel('Per %s p99 latency' % $._config.per_instance_label) +
        $.hiddenLegendQueryPanel(
          'histogram_quantile(0.99, sum by(le, %s) (rate(cortex_request_duration_seconds_bucket{%s, route=~"%s"}[$__rate_interval])))' % [$._config.per_instance_label, $.jobMatcher($._config.job_names.gateway), $.queries.read_http_routes_regex], ''
        )
      )
    )
    .addRow(
      $.row('Query-frontend')
      .addPanel(
        $.timeseriesPanel('Requests / sec') +
        $.qpsPanel($.queries.query_frontend.readRequestsPerSecond)
      )
      .addPanel(
        $.timeseriesPanel('Latency') +
        $.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector($._config.job_names.query_frontend) + [utils.selector.re('route', $.queries.read_http_routes_regex)])
      )
      .addPanel(
        $.timeseriesPanel('Per %s p99 latency' % $._config.per_instance_label) +
        $.hiddenLegendQueryPanel(
          'histogram_quantile(0.99, sum by(le, %s) (rate(cortex_request_duration_seconds_bucket{%s, route=~"%s"}[$__rate_interval])))' % [$._config.per_instance_label, $.jobMatcher($._config.job_names.query_frontend), $.queries.read_http_routes_regex], ''
        )
      )
    )
    .addRow(
      local description = |||
        <p>
          The query scheduler is an optional service that moves
          the internal queue from the query-frontend into a
          separate component.
          If this service is not deployed,
          these panels will show "No data."
        </p>
      |||;
      $.row('Query-scheduler')
      .addPanel(
        local title = 'Requests / sec';
        $.timeseriesPanel(title) +
        $.panelDescription(title, description) +
        $.qpsPanel('cortex_query_scheduler_queue_duration_seconds_count{%s}' % $.jobMatcher($._config.job_names.query_scheduler))
      )
      .addPanel(
        local title = 'Latency (Time in Queue)';
        $.timeseriesPanel(title) +
        $.panelDescription(title, description) +
        $.latencyPanel('cortex_query_scheduler_queue_duration_seconds', '{%s}' % $.jobMatcher($._config.job_names.query_scheduler))
      )
      .addPanel(
        local title = 'Queue length';
        $.timeseriesPanel(title) +
        $.panelDescription(title, description) +
        $.hiddenLegendQueryPanel(
          'sum(min_over_time(cortex_query_scheduler_queue_length{%s}[$__interval]))' % [$.jobMatcher($._config.job_names.query_scheduler)],
          'Queue length'
        ) +
        {
          fieldConfig+: {
            defaults+: {
              unit: 'queries',
            },
          },
        },
      )
    )
    .addRow(
      local description = |||
        <p>
          The query scheduler can optionally create subqueues
          in order to enforce round-robin query queuing fairness
          across additional queue dimensions beyond the default.

          By default, query queuing fairness is only applied by tenant ID.
          Queries without additional queue dimensions are labeled 'none'.
        </p>
      |||;
      local metricName = 'cortex_query_scheduler_queue_duration_seconds';
      local selector = '{%s}' % $.jobMatcher($._config.job_names.query_scheduler);
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
      $.row('Query-scheduler Latency (Time in Queue) Breakout by Additional Queue Dimensions')
      .addPanel(
        local title = '99th Percentile Latency by Queue Dimension';
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
        local title = '50th Percentile Latency by Queue Dimension';
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
        local title = 'Average Latency by Queue Dimension';
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
      )
    )
    .addRow(
      $.row('Cache – query results')
      .addPanel(
        $.timeseriesPanel('Requests / sec') +
        $.queryPanel(
          |||
            sum (
              rate(thanos_memcached_operations_total{name="frontend-cache", %(frontend)s}[$__rate_interval])
              or ignoring(backend)
              rate(thanos_cache_operations_total{name="frontend-cache", %(frontend)s}[$__rate_interval])
            )
          ||| % {
            frontend: $.jobMatcher($._config.job_names.query_frontend),
          },
          'Requests/s'
        ) +
        { fieldConfig+: { defaults+: { unit: 'ops' } } },
      )
      .addPanel(
        $.timeseriesPanel('Latency') +
        $.backwardsCompatibleLatencyPanel(
          'thanos_memcached_operation_duration_seconds',
          'thanos_cache_operation_duration_seconds',
          '{%s, name="frontend-cache"}' % $.jobMatcher($._config.job_names.query_frontend)
        )
      )
    )
    .addRow(
      $.row('Querier')
      .addPanel(
        $.timeseriesPanel('Requests / sec') +
        $.qpsPanel('cortex_querier_request_duration_seconds_count{%s, route=~"%s"}' % [$.jobMatcher($._config.job_names.querier), $.queries.read_http_routes_regex])
      )
      .addPanel(
        $.timeseriesPanel('Latency') +
        $.latencyRecordingRulePanel('cortex_querier_request_duration_seconds', $.jobSelector($._config.job_names.querier) + [utils.selector.re('route', $.queries.read_http_routes_regex)])
      )
      .addPanel(
        $.timeseriesPanel('Per %s p99 latency' % $._config.per_instance_label) +
        $.hiddenLegendQueryPanel(
          'histogram_quantile(0.99, sum by(le, %s) (rate(cortex_querier_request_duration_seconds_bucket{%s, route=~"%s"}[$__rate_interval])))' % [$._config.per_instance_label, $.jobMatcher($._config.job_names.querier), $.queries.read_http_routes_regex], ''
        )
      )
    )
    .addRow(
      $.row('Ingester')
      .addPanel(
        $.timeseriesPanel('Requests / sec') +
        $.qpsPanel('cortex_request_duration_seconds_count{%s,route=~"/cortex.Ingester/Query(Stream)?|/cortex.Ingester/MetricsForLabelMatchers|/cortex.Ingester/LabelValues|/cortex.Ingester/MetricsMetadata"}' % $.jobMatcher($._config.job_names.ingester))
      )
      .addPanel(
        $.timeseriesPanel('Latency') +
        $.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector($._config.job_names.ingester) + [utils.selector.re('route', '/cortex.Ingester/Query(Stream)?|/cortex.Ingester/MetricsForLabelMatchers|/cortex.Ingester/LabelValues|/cortex.Ingester/MetricsMetadata')])
      )
      .addPanel(
        $.timeseriesPanel('Per %s p99 latency' % $._config.per_instance_label) +
        $.hiddenLegendQueryPanel(
          'histogram_quantile(0.99, sum by(le, %s) (rate(cortex_request_duration_seconds_bucket{%s, route=~"/cortex.Ingester/Query(Stream)?|/cortex.Ingester/MetricsForLabelMatchers|/cortex.Ingester/LabelValues|/cortex.Ingester/MetricsMetadata"}[$__rate_interval])))' % [$._config.per_instance_label, $.jobMatcher($._config.job_names.ingester)], ''
        )
      )
    )
    .addRow(
      $.row('Store-gateway')
      .addPanel(
        $.timeseriesPanel('Requests / sec') +
        $.qpsPanel('cortex_request_duration_seconds_count{%s,route=~"/gatewaypb.StoreGateway/.*"}' % $.jobMatcher($._config.job_names.store_gateway))
      )
      .addPanel(
        $.timeseriesPanel('Latency') +
        $.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector($._config.job_names.store_gateway) + [utils.selector.re('route', '/gatewaypb.StoreGateway/.*')])
      )
      .addPanel(
        $.timeseriesPanel('Per %s p99 latency' % $._config.per_instance_label) +
        $.hiddenLegendQueryPanel(
          'histogram_quantile(0.99, sum by(le, %s) (rate(cortex_request_duration_seconds_bucket{%s, route=~"/gatewaypb.StoreGateway/.*"}[$__rate_interval])))' % [$._config.per_instance_label, $.jobMatcher($._config.job_names.store_gateway)], ''
        )
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
      $.row('Querier - autoscaling')
      .addPanel(
        local title = 'Replicas';
        $.timeseriesPanel(title) +
        $.queryPanel(
          [
            |||
              max by (scaletargetref_name) (
                kube_horizontalpodautoscaler_spec_max_replicas{%(namespace_matcher)s, horizontalpodautoscaler=~"%(hpa_name)s"}
                # Add the scaletargetref_name label which is more readable than "kube-hpa-..."
                + on (%(cluster_labels)s, horizontalpodautoscaler) group_left (scaletargetref_name)
                  0*kube_horizontalpodautoscaler_info{%(namespace_matcher)s, horizontalpodautoscaler=~"%(hpa_name)s"}
              )
            ||| % {
              namespace_matcher: $.namespaceMatcher(),
              cluster_labels: std.join(', ', $._config.cluster_labels),
              hpa_name: $._config.autoscaling.querier.hpa_name,
            },
            |||
              max by (scaletargetref_name) (
                kube_horizontalpodautoscaler_status_current_replicas{%(namespace_matcher)s, horizontalpodautoscaler=~"%(hpa_name)s"}
                # HPA doesn't go to 0 replicas, so we multiply by 0 if the HPA is not active.
                * on (%(cluster_labels)s, horizontalpodautoscaler)
                  kube_horizontalpodautoscaler_status_condition{%(namespace_matcher)s, horizontalpodautoscaler=~"%(hpa_name)s", condition="ScalingActive", status="true"}
                # Add the scaletargetref_name label which is more readable than "kube-hpa-..."
                + on (%(cluster_labels)s, horizontalpodautoscaler) group_left (scaletargetref_name)
                  0*kube_horizontalpodautoscaler_info{%(namespace_matcher)s, horizontalpodautoscaler=~"%(hpa_name)s"}
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
                + on (%(cluster_labels)s, horizontalpodautoscaler) group_left (scaletargetref_name)
                  0*kube_horizontalpodautoscaler_info{%(namespace_matcher)s, horizontalpodautoscaler=~"%(hpa_name)s"}
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
            Please note that the current number of replicas can still show 1 replica even when scaled to 0.
            Since HPA never reports 0 replicas, the query will report 0 only if the HPA is not active.
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
                label_replace(label_replace(
                    kube_horizontalpodautoscaler_spec_target_metric{%(namespace)s, horizontalpodautoscaler=~"%(hpa_name)s"},
                    "metric", "$1", "metric_name", "(.+)"
                ), "scaledObject", "$1", "horizontalpodautoscaler", "%(hpa_prefix)s(.*)")
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
            metrics server is unable to query the scaling metric from Prometheus so the autoscaler woudln't work properly.
          |||
        ),
      )
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
              # Backwards compatibility
              rate(
                thanos_memcached_operations_total{
                  component="store-gateway",
                  name="index-cache",
                  %s
                }[$__rate_interval]
              )
              or ignoring(backend)
              rate(
                thanos_cache_operations_total{
                  component="store-gateway",
                  name="index-cache",
                  %s
                }[$__rate_interval]
              )
            )
          ||| % [
            $.jobMatcher($._config.job_names.store_gateway),
            $.jobMatcher($._config.job_names.store_gateway),
          ],
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
    ),
}
