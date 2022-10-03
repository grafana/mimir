local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-overview.json';

(import 'dashboard-utils.libsonnet') +
(import 'dashboard-queries.libsonnet') {
  [filename]:
    ($.dashboard('Overview') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates()

    .addRow(
      $.row('Writes')
      .addPanel(
        $.textPanel('', |||
          These panels show an overview on the write path.
          Visit the following specific dashboards to drill down into the write path:

          <ul>
            <li><a href="%(writesDashboardURL)s">Writes</a></li>
            <li><a href="%(writesResourcesDashboardURL)s">Writes resources</a></li>
            <li><a href="%(writesNetworkingDashboardURL)s">Writes networking</a></li>
          </ul>
        ||| % {
          writesDashboardURL: $.dashboardURL('mimir-writes.json'),
          writesResourcesDashboardURL: $.dashboardURL('mimir-writes-resources.json'),
          writesNetworkingDashboardURL: $.dashboardURL('mimir-writes-networking.json'),
        }),
      )
      .addPanel(
        $.panel('Write requests / sec') +
        $.qpsPanel(
          if $._config.gateway_enabled then
            $.queries.gateway.writeRequestsPerSecond
          else
            $.queries.distributor.writeRequestsPerSecond
        )
      )
      .addPanel(
        $.panel('Write latency') + (
          if $._config.gateway_enabled then
            utils.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector($._config.job_names.gateway) + [utils.selector.re('route', $.queries.write_http_routes_regex)])
          else
            utils.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector($._config.job_names.distributor) + [utils.selector.re('route', '/distributor.Distributor/Push|/httpgrpc.*|%s' % $.queries.write_http_routes_regex)])
        )
      )
      .addPanel(
        $.panel('Ingestion / sec') +
        $.queryPanel(
          [$.queries.distributor.samplesPerSecond, $.queries.distributor.exemplarsPerSecond],
          ['samples / sec', 'exemplars / sec'],
        ) +
        $.stack +
        { yaxes: $.yaxes('cps') },
      )
    )

    .addRow(
      $.row('Reads')
      .addPanel(
        $.textPanel('', |||
          These panels show an overview on the read path.
          Visit the following specific dashboards to drill down into the read path:

          <ul>
            <li><a href="%(readsDashboardURL)s">Reads</a></li>
            <li><a href="%(readsResourcesDashboardURL)s">Reads resources</a></li>
            <li><a href="%(readsNetworkingDashboardURL)s">Reads networking</a></li>
            <li><a href="%(queriesDashboardURL)s">Queries</a></li>
          </ul>
        ||| % {
          readsDashboardURL: $.dashboardURL('mimir-reads.json'),
          readsResourcesDashboardURL: $.dashboardURL('mimir-reads-resources.json'),
          readsNetworkingDashboardURL: $.dashboardURL('mimir-reads-networking.json'),
          queriesDashboardURL: $.dashboardURL('mimir-queries.json'),
        }),
      )
      .addPanel(
        $.panel('Read requests / sec') +
        $.qpsPanel(
          if $._config.gateway_enabled then
            $.queries.gateway.readRequestsPerSecond
          else
            $.queries.query_frontend.readRequestsPerSecond
        )
      )
      .addPanel(
        $.panel('Read latency') + (
          if $._config.gateway_enabled then
            utils.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector($._config.job_names.gateway) + [utils.selector.re('route', $.queries.read_http_routes_regex)])
          else
            utils.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector($._config.job_names.query_frontend) + [utils.selector.re('route', $.queries.read_http_routes_regex)])
        )
      )
      .addPanel(
        $.panel('Queries / sec') +
        $.queryPanel(
          [
            $.queries.query_frontend.instantQueriesPerSecond,
            $.queries.query_frontend.rangeQueriesPerSecond,
            $.queries.query_frontend.labelQueriesPerSecond,
            $.queries.query_frontend.seriesQueriesPerSecond,
            $.queries.query_frontend.otherQueriesPerSecond,
          ],
          ['instant queries', 'range queries', 'label queries', 'series queries', 'other'],
        ) +
        $.stack +
        { yaxes: $.yaxes('reqps') },
      )
    )

    .addRow(
      $.row('Recording and alerting rules')
      .addPanel(
        $.textPanel('', |||
          These panels show an overview on the recording and alerting rules evaluation.
          Visit the following specific dashboards to drill down into the rules evaluation and alerts notifications:

          <ul>
            <li><a href="%(rulerDashboardURL)s">Ruler</a></li>
            <li><a href="%(alertmanagerDashboardURL)s">Alertmanager</a></li>
            <li><a href="%(alertmanagerResourcesDashboardURL)s">Alertmanager resources</a></li>
          </ul>
        ||| % {
          rulerDashboardURL: $.dashboardURL('mimir-ruler.json'),
          alertmanagerDashboardURL: $.dashboardURL('mimir-alertmanager.json'),
          alertmanagerResourcesDashboardURL: $.dashboardURL('mimir-alertmanager-resources.json'),
        }),
      )
      .addPanel(
        $.panel('Rule evaluations / sec') +
        $.queryPanel(
          [
            $.queries.ruler.evaluations.successPerSecond,
            $.queries.ruler.evaluations.failurePerSecond,
            $.queries.ruler.evaluations.missedIterationsPerSecond,
          ],
          ['success', 'failed', 'missed'],
        )
      )
      .addPanel(
        $.panel('Rule evaluations latency') +
        $.queryPanel(
          $.queries.ruler.evaluations.latency,
          'average'
        ) +
        { yaxes: $.yaxes('s') },
      )
      .addPanel(
        local success = |||
          sum(rate(cortex_prometheus_notifications_sent_total{%s}[$__rate_interval]))
            -
          sum(rate(cortex_prometheus_notifications_errors_total{%s}[$__rate_interval]))
        ||| % [$.jobMatcher($._config.job_names.ruler), $.jobMatcher($._config.job_names.ruler)];

        local failure = |||
          sum(rate(cortex_prometheus_notifications_errors_total{%s}[$__rate_interval]))
        ||| % $.jobMatcher($._config.job_names.ruler);

        $.successFailurePanel('Alerting notifications sent to Alertmanager / sec', success, failure)
      )
    )

    .addRow(
      $.row('Long-term storage (object storage)')
      .addPanel(
        $.textPanel('', |||
          These panels show an overview on the long-term storage (object storage).
          Visit the following specific dashboards to drill down into the storage:

          <ul>
            <li><a href="%(objectStoreDashboardURL)s">Object store</a></li>
          </ul>
        ||| % {
          objectStoreDashboardURL: $.dashboardURL('mimir-object-store.json'),
        }),
      )
      .addPanel(
        local failure = 'sum(rate(thanos_objstore_bucket_operation_failures_total{%s}[$__rate_interval]))' % $.namespaceMatcher();
        local success = 'sum(rate(thanos_objstore_bucket_operations_total{%s}[$__rate_interval])) - %s' % [$.namespaceMatcher(), failure];

        $.successFailurePanel('Requests / sec', success, failure) +
        { yaxes: $.yaxes('reqps') },
      )
      .addPanel(
        $.panel('Operations / sec') +
        $.queryPanel('sum by(operation) (rate(thanos_objstore_bucket_operations_total{%s}[$__rate_interval]))' % $.namespaceMatcher(), '{{operation}}') +
        $.stack +
        { yaxes: $.yaxes('reqps') },
      )
      .addPanel(
        $.panel('Total number of blocks in the storage') +
        // Look at the max over the last 15m to correctly work during rollouts
        // (the metrics disappear until the next cleanup runs).
        $.queryPanel('sum(max by(user) (max_over_time(cortex_bucket_blocks_count{%s}[15m])))' % $.jobMatcher($._config.job_names.compactor), 'blocks'),
      )
    ),
}
