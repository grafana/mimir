local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-reads.json';

(import 'dashboard-utils.libsonnet') {
  [filename]:
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
        $.statPanel(|||
          sum(
            rate(
              cortex_request_duration_seconds_count{
                %(queryFrontend)s,
                route=~"(prometheus|api_prom)_api_v1_query_range"
              }[$__rate_interval]
            )
          )
        ||| % {
          queryFrontend: $.jobMatcher($._config.job_names.query_frontend),
        }, format='reqps') +
        $.panelDescription(
          'Range queries per second',
          |||
            Rate of range queries per second being made to
            %(product)s via the <tt>/prometheus</tt> API.
          ||| % $._config
        ),
      )
    )
    .addRowIf(
      $._config.gateway_enabled,
      $.row('Gateway')
      .addPanel(
        $.panel('Requests / sec') +
        $.qpsPanel('cortex_request_duration_seconds_count{%s, route=~"(prometheus|api_prom)_api_v1_.+"}' % $.jobMatcher($._config.job_names.gateway))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector($._config.job_names.gateway) + [utils.selector.re('route', '(prometheus|api_prom)_api_v1_.+')])
      )
      .addPanel(
        $.panel('Per %s p99 latency' % $._config.per_instance_label) +
        $.hiddenLegendQueryPanel(
          'histogram_quantile(0.99, sum by(le, %s) (rate(cortex_request_duration_seconds_bucket{%s, route=~"(prometheus|api_prom)_api_v1_.+"}[$__rate_interval])))' % [$._config.per_instance_label, $.jobMatcher($._config.job_names.gateway)], ''
        ) +
        { yaxes: $.yaxes('s') }
      )
    )
    .addRow(
      $.row('Query-frontend')
      .addPanel(
        $.panel('Requests / sec') +
        $.qpsPanel('cortex_request_duration_seconds_count{%s, route=~"(prometheus|api_prom)_api_v1_.+"}' % $.jobMatcher($._config.job_names.query_frontend))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector($._config.job_names.query_frontend) + [utils.selector.re('route', '(prometheus|api_prom)_api_v1_.+')])
      )
      .addPanel(
        $.panel('Per %s p99 latency' % $._config.per_instance_label) +
        $.hiddenLegendQueryPanel(
          'histogram_quantile(0.99, sum by(le, %s) (rate(cortex_request_duration_seconds_bucket{%s, route=~"(prometheus|api_prom)_api_v1_.+"}[$__rate_interval])))' % [$._config.per_instance_label, $.jobMatcher($._config.job_names.query_frontend)], ''
        ) +
        { yaxes: $.yaxes('s') }
      )
    )
    .addRow(
      $.row('Query-scheduler')
      .addPanel(
        $.textPanel(
          '',
          |||
            <p>
              The query scheduler is an optional service that moves
              the internal queue from the query-frontend into a
              separate component.
              If this service is not deployed,
              these panels will show "No data."
            </p>
          |||
        )
      )
      .addPanel(
        $.panel('Requests / sec') +
        $.qpsPanel('cortex_query_scheduler_queue_duration_seconds_count{%s}' % $.jobMatcher($._config.job_names.query_scheduler))
      )
      .addPanel(
        $.panel('Latency (Time in Queue)') +
        $.latencyPanel('cortex_query_scheduler_queue_duration_seconds', '{%s}' % $.jobMatcher($._config.job_names.query_scheduler))
      )
    )
    .addRow(
      $.row('Cache – query results')
      .addPanel(
        $.panel('Requests / sec') +
        $.queryPanel(
          |||
            # Query metrics before and after migration to new memcached backend.
            sum (
              rate(cortex_cache_request_duration_seconds_count{name=~"frontend.+", %(frontend)s}[$__rate_interval])
              or
              rate(thanos_memcached_operation_duration_seconds_count{name="frontend-cache", %(frontend)s}[$__rate_interval])
            )
          ||| % {
            frontend: $.jobMatcher($._config.job_names.query_frontend),
          },
          'Requests/s'
        ) +
        { yaxes: $.yaxes('ops') },
      )
      .addPanel(
        $.panel('Latency (old)') +
        utils.latencyRecordingRulePanel('cortex_cache_request_duration_seconds', $.jobSelector($._config.job_names.query_frontend) + [utils.selector.re('method', 'frontend.+')])
      )
      .addPanel(
        $.panel('Latency (new)') +
        $.latencyPanel(
          'thanos_memcached_operation_duration_seconds',
          '{%s, name="frontend-cache"}' % $.jobMatcher($._config.job_names.query_frontend)
        )
      )
    )
    .addRow(
      $.row('Querier')
      .addPanel(
        $.panel('Requests / sec') +
        $.qpsPanel('cortex_querier_request_duration_seconds_count{%s, route=~"(prometheus|api_prom)_api_v1_.+"}' % $.jobMatcher($._config.job_names.querier))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_querier_request_duration_seconds', $.jobSelector($._config.job_names.querier) + [utils.selector.re('route', '(prometheus|api_prom)_api_v1_.+')])
      )
      .addPanel(
        $.panel('Per %s p99 latency' % $._config.per_instance_label) +
        $.hiddenLegendQueryPanel(
          'histogram_quantile(0.99, sum by(le, %s) (rate(cortex_querier_request_duration_seconds_bucket{%s, route=~"(prometheus|api_prom)_api_v1_.+"}[$__rate_interval])))' % [$._config.per_instance_label, $.jobMatcher($._config.job_names.querier)], ''
        ) +
        { yaxes: $.yaxes('s') }
      )
    )
    .addRowIf(
      $._config.autoscaling.querier_enabled,
      $.row('Querier - autoscaling')
      .addPanel(
        local title = 'Replicas';
        $.panel(title) +
        $.queryPanel(
          [
            'kube_horizontalpodautoscaler_spec_min_replicas{%s, horizontalpodautoscaler="%s"}' % [$.namespaceMatcher(), $._config.autoscaling.querier_hpa_name],
            'kube_horizontalpodautoscaler_spec_max_replicas{%s, horizontalpodautoscaler="%s"}' % [$.namespaceMatcher(), $._config.autoscaling.querier_hpa_name],
            'kube_horizontalpodautoscaler_status_current_replicas{%s, horizontalpodautoscaler="%s"}' % [$.namespaceMatcher(), $._config.autoscaling.querier_hpa_name],
          ],
          [
            'Min',
            'Max',
            'Current',
          ],
        ) +
        $.panelDescription(
          title,
          |||
            The minimum, maximum, and current number of querier replicas.
          |||
        ),
      )
      .addPanel(
        local title = 'Scaling metric';
        $.panel(title) +
        $.queryPanel(
          [
            $.filterKedaMetricByHPA('keda_metrics_adapter_scaler_metrics_value', $._config.autoscaling.querier_hpa_name),
            'kube_horizontalpodautoscaler_spec_target_metric{%s, horizontalpodautoscaler="%s"}' % [$.namespaceMatcher(), $._config.autoscaling.querier_hpa_name],
          ], [
            'Scaling metric',
            'Target per replica',
          ]
        ) +
        $.panelDescription(
          title,
          |||
            This panel shows the result of the query used as scaling metric and target/threshold used.
            The desired number of replicas is computed by HPA as: <scaling metric> / <target per replica>.
          |||
        ) +
        $.panelAxisPlacement('Target per replica', 'right'),
      )
      .addPanel(
        local title = 'Autoscaler failures rate';
        $.panel(title) +
        $.queryPanel(
          $.filterKedaMetricByHPA('sum by(metric) (rate(keda_metrics_adapter_scaler_errors[$__rate_interval]))', $._config.autoscaling.querier_hpa_name),
          'Failures per second'
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
      $.row('Ingester')
      .addPanel(
        $.panel('Requests / sec') +
        $.qpsPanel('cortex_request_duration_seconds_count{%s,route=~"/cortex.Ingester/Query(Stream)?|/cortex.Ingester/MetricsForLabelMatchers|/cortex.Ingester/LabelValues|/cortex.Ingester/MetricsMetadata"}' % $.jobMatcher($._config.job_names.ingester))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector($._config.job_names.ingester) + [utils.selector.re('route', '/cortex.Ingester/Query(Stream)?|/cortex.Ingester/MetricsForLabelMatchers|/cortex.Ingester/LabelValues|/cortex.Ingester/MetricsMetadata')])
      )
      .addPanel(
        $.panel('Per %s p99 latency' % $._config.per_instance_label) +
        $.hiddenLegendQueryPanel(
          'histogram_quantile(0.99, sum by(le, %s) (rate(cortex_request_duration_seconds_bucket{%s, route=~"/cortex.Ingester/Query(Stream)?|/cortex.Ingester/MetricsForLabelMatchers|/cortex.Ingester/LabelValues|/cortex.Ingester/MetricsMetadata"}[$__rate_interval])))' % [$._config.per_instance_label, $.jobMatcher($._config.job_names.ingester)], ''
        ) +
        { yaxes: $.yaxes('s') }
      )
    )
    .addRow(
      $.row('Store-gateway')
      .addPanel(
        $.panel('Requests / sec') +
        $.qpsPanel('cortex_request_duration_seconds_count{%s,route=~"/gatewaypb.StoreGateway/.*"}' % $.jobMatcher($._config.job_names.store_gateway))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector($._config.job_names.store_gateway) + [utils.selector.re('route', '/gatewaypb.StoreGateway/.*')])
      )
      .addPanel(
        $.panel('Per %s p99 latency' % $._config.per_instance_label) +
        $.hiddenLegendQueryPanel(
          'histogram_quantile(0.99, sum by(le, %s) (rate(cortex_request_duration_seconds_bucket{%s, route=~"/gatewaypb.StoreGateway/.*"}[$__rate_interval])))' % [$._config.per_instance_label, $.jobMatcher($._config.job_names.store_gateway)], ''
        ) +
        { yaxes: $.yaxes('s') }
      )
    )
    .addRow(
      $.kvStoreRow('Store-gateway – key-value store for store-gateways ring', 'store_gateway', 'store-gateway')
    )
    .addRow(
      $.row('Memcached – block index cache (store-gateway accesses)')  // Resembles thanosMemcachedCache
      .addPanel(
        $.panel('Requests / sec') +
        $.queryPanel(
          |||
            sum by(operation) (
              rate(
                thanos_memcached_operations_total{
                  component="store-gateway",
                  name="index-cache",
                  %s
                }[$__rate_interval]
              )
            )
          ||| % $.jobMatcher($._config.job_names.store_gateway), '{{operation}}'
        ) +
        $.stack +
        { yaxes: $.yaxes('ops') },
      )
      .addPanel(
        $.panel('Latency (getmulti)') +
        $.latencyPanel(
          'thanos_memcached_operation_duration_seconds',
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
        $.panel('Hit ratio') +
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
        { yaxes: $.yaxes('percentunit') } +
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
