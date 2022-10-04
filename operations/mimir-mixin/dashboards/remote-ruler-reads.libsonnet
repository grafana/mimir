local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-remote-ruler-reads.json';

(import 'dashboard-utils.libsonnet') +
(import 'dashboard-queries.libsonnet') {
  // Both support gRPC and HTTP requests. HTTP request is used when rule evaluation query requests go through the query-tee.
  local rulerRoutesRegex = '/httpgrpc.HTTP/Handle|.*api_v1_query',

  [filename]:
    ($.dashboard('Remote ruler reads') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates()
    .addRowIf(
      $._config.show_dashboard_descriptions.reads,
      ($.row('Remote ruler reads dashboard description') { height: '175px', showTitle: false })
      .addPanel(
        $.textPanel('', |||
          <p>
            This dashboard shows health metrics for the ruler read path when remote operational mode is enabled.
            It is broken into sections for each service on the ruler read path, and organized by the order in which the read request flows.
            <br/>
            For each service, there are three panels showing (1) requests per second to that service, (2) average, median, and p99 latency of requests to that service, and (3) p99 latency of requests to each instance of that service.
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
        $.panel('Evaluations / sec') +
        $.statPanel(|||
          sum(
            rate(
              cortex_request_duration_seconds_count{
                %(queryFrontend)s,
                route=~"%(rulerRoutesRegex)s"
              }[$__rate_interval]
            )
          )
        ||| % {
          queryFrontend: $.jobMatcher($._config.job_names.ruler_query_frontend),
          rulerRoutesRegex: rulerRoutesRegex,
        }, format='reqps') +
        $.panelDescription(
          'Evaluations per second',
          |||
            Rate of rule expressions evaluated per second.
          |||
        ),
      )
    )
    .addRow(
      $.row('Query-frontend (dedicated to ruler)')
      .addPanel(
        $.panel('Requests / sec') +
        $.qpsPanel('cortex_request_duration_seconds_count{%s, route=~"%s"}' % [$.jobMatcher($._config.job_names.ruler_query_frontend), rulerRoutesRegex])
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector($._config.job_names.ruler_query_frontend) + [utils.selector.re('route', rulerRoutesRegex)])
      )
      .addPanel(
        $.timeseriesPanel('Per %s p99 latency' % $._config.per_instance_label) +
        $.hiddenLegendQueryPanel(
          'histogram_quantile(0.99, sum by(le, %s) (rate(cortex_request_duration_seconds_bucket{%s, route=~"%s"}[$__rate_interval])))' % [$._config.per_instance_label, $.jobMatcher($._config.job_names.ruler_query_frontend), rulerRoutesRegex], ''
        )
      )
    )
    .addRow(
      $.row('Query-scheduler (dedicated to ruler)')
      .addPanel(
        $.panel('Requests / sec') +
        $.qpsPanel('cortex_query_scheduler_queue_duration_seconds_count{%s}' % $.jobMatcher($._config.job_names.ruler_query_scheduler))
      )
      .addPanel(
        $.panel('Latency (time in queue)') +
        $.latencyPanel('cortex_query_scheduler_queue_duration_seconds', '{%s}' % $.jobMatcher($._config.job_names.ruler_query_scheduler))
      )
    )
    .addRow(
      $.row('Querier (dedicated to ruler)')
      .addPanel(
        $.panel('Requests / sec') +
        $.qpsPanel('cortex_querier_request_duration_seconds_count{%s, route=~"%s"}' % [$.jobMatcher($._config.job_names.ruler_querier), $.queries.read_http_routes_regex])
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_querier_request_duration_seconds', $.jobSelector($._config.job_names.ruler_querier) + [utils.selector.re('route', $.queries.read_http_routes_regex)])
      )
      .addPanel(
        $.timeseriesPanel('Per %s p99 latency' % $._config.per_instance_label) +
        $.hiddenLegendQueryPanel(
          'histogram_quantile(0.99, sum by(le, %s) (rate(cortex_querier_request_duration_seconds_bucket{%s, route=~"%s"}[$__rate_interval])))' % [$._config.per_instance_label, $.jobMatcher($._config.job_names.ruler_querier), $.queries.read_http_routes_regex], ''
        )
      )
    )
    .addRowIf(
      $._config.autoscaling.querier_enabled,
      $.row('Querier (dedicated to ruler) - autoscaling')
      .addPanel(
        local title = 'Replicas';
        $.panel(title) +
        $.queryPanel(
          [
            'kube_horizontalpodautoscaler_spec_min_replicas{%s, horizontalpodautoscaler="%s"}' % [$.namespaceMatcher(), $._config.autoscaling.ruler_querier_hpa_name],
            'kube_horizontalpodautoscaler_spec_max_replicas{%s, horizontalpodautoscaler="%s"}' % [$.namespaceMatcher(), $._config.autoscaling.ruler_querier_hpa_name],
            'kube_horizontalpodautoscaler_status_current_replicas{%s, horizontalpodautoscaler="%s"}' % [$.namespaceMatcher(), $._config.autoscaling.ruler_querier_hpa_name],
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
            $.filterKedaMetricByHPA('keda_metrics_adapter_scaler_metrics_value', $._config.autoscaling.ruler_querier_hpa_name),
            'kube_horizontalpodautoscaler_spec_target_metric{%s, horizontalpodautoscaler="%s"}' % [$.namespaceMatcher(), $._config.autoscaling.ruler_querier_hpa_name],
          ], [
            'Scaling metric',
            'Target per replica',
          ]
        ) +
        $.panelDescription(
          title,
          |||
            This panel shows the result of the query that is used as the scaling metric, and the target and threshold used.
            The desired number of replicas is computed by HPA as: <scaling metric> / <target per replica>.
          |||
        ) +
        $.panelAxisPlacement('Target per replica', 'right'),
      )
      .addPanel(
        local title = 'Autoscaler failures rate';
        $.panel(title) +
        $.queryPanel(
          $.filterKedaMetricByHPA('sum by(metric) (rate(keda_metrics_adapter_scaler_errors[$__rate_interval]))', $._config.autoscaling.ruler_querier_hpa_name),
          'Failures per second'
        ) +
        $.panelDescription(
          title,
          |||
            The rate of failures in the KEDA custom metrics API server. Whenever an error occurs, the KEDA custom
            metrics server is unable to query the scaling metric from Prometheus so the autoscaler does not work properly.
          |||
        ),
      )
    ),
}
