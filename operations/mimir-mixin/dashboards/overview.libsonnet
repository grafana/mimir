local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-overview.json';

(import 'dashboard-utils.libsonnet') +
(import 'dashboard-queries.libsonnet') {
  [filename]:
    local helpers = {
      // Adds a suffix to the title of panels whose metrics are gathered from the gateway.
      gatewayEnabledPanelTitleSuffix: if !$._config.gateway_enabled then '' else
        '(gateway)',

      // Adds an extra description to rows containing panels whose metrics are gathered from the gateway.
      gatewayEnabledRowDescription: if !$._config.gateway_enabled then '' else
        'Requests rate and latency is measured on the gateway.',

      // Dashboard URLs.
      alertmanagerDashboardURL: $.dashboardURL('mimir-alertmanager.json'),
      alertmanagerResourcesDashboardURL: $.dashboardURL('mimir-alertmanager-resources.json'),
      compactorDashboardURL: $.dashboardURL('mimir-compactor.json'),
      objectStoreDashboardURL: $.dashboardURL('mimir-object-store.json'),
      overviewNetworkingDashboardURL: $.dashboardURL('mimir-overview-networking.json'),
      overviewResourcesDashboardURL: $.dashboardURL('mimir-overview-resources.json'),
      queriesDashboardURL: $.dashboardURL('mimir-queries.json'),
      readsDashboardURL: $.dashboardURL('mimir-reads.json'),
      readsNetworkingDashboardURL: $.dashboardURL('mimir-reads-networking.json'),
      readsResourcesDashboardURL: $.dashboardURL('mimir-reads-resources.json'),
      rulerDashboardURL: $.dashboardURL('mimir-ruler.json'),
      writesDashboardURL: $.dashboardURL('mimir-writes.json'),
      writesNetworkingDashboardURL: $.dashboardURL('mimir-writes-networking.json'),
      writesResourcesDashboardURL: $.dashboardURL('mimir-writes-resources.json'),
    };

    assert std.md5(filename) == 'ffcd83628d7d4b5a03d1cafd159e6c9c' : 'UID of the dashboard has changed, please update references to dashboard.';
    ($.dashboard('Overview') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates()
    .addShowNativeLatencyVariable()

    .addRow(
      $.row('%(product)s cluster health' % $._config)
      .addPanel(
        $.textPanel('', |||
          The 'Status' panel shows an overview on the cluster health over the time.
          To investigate failures, see a specific dashboard:

          - <a target="_blank" href="%(writesDashboardURL)s">Writes</a>
          - <a target="_blank" href="%(readsDashboardURL)s">Reads</a>
          - <a target="_blank" href="%(rulerDashboardURL)s">Rule evaluations</a>
          - <a target="_blank" href="%(alertmanagerDashboardURL)s">Alerting notifications</a>
          - <a target="_blank" href="%(objectStoreDashboardURL)s">Object storage</a>
        ||| % helpers),
      )
      .addPanel(
        $.stateTimelinePanel(
          'Status',
          [
            // Write failures.
            utils.showNativeHistogramQuery(
              if $._config.gateway_enabled then $.queries.gateway.writeFailuresRate else $.queries.distributor.writeFailuresRate
            ),
            // Write failures but from classic histograms.
            utils.showClassicHistogramQuery(
              if $._config.gateway_enabled then $.queries.gateway.writeFailuresRate else $.queries.distributor.writeFailuresRate
            ),
            // Read failures.
            utils.showNativeHistogramQuery(
              if $._config.gateway_enabled then $.queries.gateway.readFailuresRate else $.queries.query_frontend.readFailuresRate,
            ),
            // Read failures but from classic histograms.
            utils.showClassicHistogramQuery(
              if $._config.gateway_enabled then $.queries.gateway.readFailuresRate else $.queries.query_frontend.readFailuresRate,
            ),
            // Rule evaluation failures.
            $.queries.ruler.evaluations.failuresRate,
            // Alerting notifications.
            |||
              (
                # Failed notifications from ruler to Alertmanager (handling the case the ruler metrics are missing).
                ((%(rulerFailurePerSecond)s) or vector(0))
                +
                # Failed notifications from Alertmanager to receivers (handling the case the alertmanager metrics are missing).
                ((%(alertmanagerFailurePerSecond)s) or vector(0))
              )
              /
              (
                # Total notifications from ruler to Alertmanager (handling the case the ruler metrics are missing).
                ((%(rulerTotalPerSecond)s) or vector(0))
                +
                # Total notifications from Alertmanager to receivers (handling the case the alertmanager metrics are missing).
                ((%(alertmanagerTotalPerSecond)s) or vector(0))
              )
            ||| % {
              rulerFailurePerSecond: $.queries.ruler.notifications.failurePerSecond,
              rulerTotalPerSecond: $.queries.ruler.notifications.totalPerSecond,
              alertmanagerFailurePerSecond: $.queries.alertmanager.notifications.failurePerSecond,
              alertmanagerTotalPerSecond: $.queries.alertmanager.notifications.totalPerSecond,
            },
            // Object storage failures.
            $.queries.storage.failuresRate,
          ],
          ['Writes', 'Writes', 'Reads', 'Reads', 'Rule evaluations', 'Alerting notifications', 'Object storage']
        )
      )
      .addPanel(
        $.alertListPanel('Firing alerts', $._config.product, $.namespaceMatcher())
      ) + {
        panels: [
          // Custom width for panels, so that the text panel (description) has the same width of the description in the following rows,
          // and the status takes more space than the others.
          panel { span: if panel.type == 'state-timeline' then 6 else 3 }
          for panel in super.panels
        ],
      }
    )

    .addRow(
      $.row('Writes')
      .addPanel(
        $.textPanel('', |||
          These panels show an overview on the write path. %(gatewayEnabledRowDescription)s
          To examine the write path in detail, see a specific dashboard:

          - <a target="_blank" href="%(writesDashboardURL)s">Writes</a>
          - <a target="_blank" href="%(writesResourcesDashboardURL)s">Writes resources</a>
          - <a target="_blank" href="%(writesNetworkingDashboardURL)s">Writes networking</a>
          - <a target="_blank" href="%(overviewResourcesDashboardURL)s">Overview resources</a>
          - <a target="_blank" href="%(overviewNetworkingDashboardURL)s">Overview networking</a>
        ||| % helpers),
      )
      .addPanel(
        $.timeseriesPanel(std.stripChars('Write requests / sec %(gatewayEnabledPanelTitleSuffix)s' % helpers, ' ')) +
        $.qpsPanelNativeHistogram(
          if $._config.gateway_enabled then
            $.queries.gateway.requestsPerSecondMetric
          else
            $.queries.distributor.requestsPerSecondMetric,
          if $._config.gateway_enabled then
            $.queries.gateway.writeRequestsPerSecondSelector
          else
            $.queries.distributor.writeRequestsPerSecondSelector
        )
      )
      .addPanel(
        $.timeseriesPanel(std.stripChars('Write latency %(gatewayEnabledPanelTitleSuffix)s' % helpers, ' ')) + (
          if $._config.gateway_enabled then
            $.latencyRecordingRulePanelNativeHistogram($.queries.gateway.requestsPerSecondMetric, $.jobSelector($._config.job_names.gateway) + [utils.selector.re('route', $.queries.write_http_routes_regex)])
          else
            $.latencyRecordingRulePanelNativeHistogram($.queries.distributor.requestsPerSecondMetric, $.jobSelector($._config.job_names.distributor) + [utils.selector.re('route', '%s' % $.queries.distributor.writeRequestsPerSecondRouteRegex)])
        )
      )
      .addPanel(
        $.timeseriesPanel('Ingestion / sec') +
        $.queryPanel(
          [$.queries.distributor.samplesPerSecond, $.queries.distributor.exemplarsPerSecond],
          ['samples / sec', 'exemplars / sec'],
        ) +
        $.stack +
        { fieldConfig+: { defaults+: { unit: 'cps' } } },
      )
    )

    .addRow(
      $.row('Reads')
      .addPanel(
        $.textPanel('', |||
          These panels show an overview on the read path. %(gatewayEnabledRowDescription)s
          To examine the read path in detail, see a specific dashboard:

          - <a target="_blank" href="%(readsDashboardURL)s">Reads</a>
          - <a target="_blank" href="%(readsResourcesDashboardURL)s">Reads resources</a>
          - <a target="_blank" href="%(readsNetworkingDashboardURL)s">Reads networking</a>
          - <a target="_blank" href="%(overviewResourcesDashboardURL)s">Overview resources</a>
          - <a target="_blank" href="%(overviewNetworkingDashboardURL)s">Overview networking</a>
          - <a target="_blank" href="%(queriesDashboardURL)s">Queries</a>
          - <a target="_blank" href="%(compactorDashboardURL)s">Compactor</a>
        ||| % helpers),
      )
      .addPanel(
        $.timeseriesPanel(std.stripChars('Read requests / sec %(gatewayEnabledPanelTitleSuffix)s' % helpers, ' ')) +
        $.qpsPanelNativeHistogram(
          if $._config.gateway_enabled then
            $.queries.gateway.requestsPerSecondMetric
          else
            $.queries.query_frontend.requestsPerSecondMetric,
          if $._config.gateway_enabled then
            $.queries.gateway.readRequestsPerSecondSelector
          else
            $.queries.query_frontend.readRequestsPerSecondSelector
        )
      )
      .addPanel(
        $.timeseriesPanel(std.stripChars('Read latency %(gatewayEnabledPanelTitleSuffix)s' % helpers, ' ')) + (
          if $._config.gateway_enabled then
            $.latencyRecordingRulePanelNativeHistogram($.queries.gateway.requestsPerSecondMetric, $.jobSelector($._config.job_names.gateway) + [utils.selector.re('route', $.queries.read_http_routes_regex)])
          else
            $.latencyRecordingRulePanelNativeHistogram($.queries.query_frontend.requestsPerSecondMetric, $.jobSelector($._config.job_names.query_frontend) + [utils.selector.re('route', $.queries.read_http_routes_regex)])
        )
      )
      .addPanel(
        $.timeseriesPanel('Queries / sec') +
        {
          targets: [
            {
              expr: utils.showClassicHistogramQuery(utils.ncHistogramSumBy(utils.ncHistogramCountRate($.queries.query_frontend.overviewRoutesPerSecondMetric, $.queries.query_frontend.overviewRoutesPerSecondSelector), ['route'])),
              format: 'time_series',
              legendLink: null,
            },
            {
              expr: utils.showNativeHistogramQuery(utils.ncHistogramSumBy(utils.ncHistogramCountRate($.queries.query_frontend.overviewRoutesPerSecondMetric, $.queries.query_frontend.overviewRoutesPerSecondSelector), ['route'])),
              format: 'time_series',
              legendLink: null,
            },
            {
              expr: utils.showClassicHistogramQuery(utils.ncHistogramSumBy(utils.ncHistogramCountRate($.queries.query_frontend.overviewRoutesPerSecondMetric, $.queries.query_frontend.nonOverviewRoutesPerSecondSelector))),
              format: 'time_series',
              legendFormat: 'other',
              legendLink: null,
            },
            {
              expr: utils.showNativeHistogramQuery(utils.ncHistogramSumBy(utils.ncHistogramCountRate($.queries.query_frontend.overviewRoutesPerSecondMetric, $.queries.query_frontend.nonOverviewRoutesPerSecondSelector))),
              format: 'time_series',
              legendFormat: 'other',
              legendLink: null,
            },
          ],
        } +
        {
          fieldConfig+: {
            defaults+: { unit: 'reqps' },
            overrides+: $.overridesNonErrorColorsPalette($.queries.query_frontend.overviewRoutesOverrides),
          },
        } +
        $.stack
      )
    )

    .addRow(
      $.row('Recording and alerting rules')
      .addPanel(
        $.textPanel('', |||
          These panels show an overview on the recording and alerting rules evaluation.
          To examine the rules evaluation and alerts notifications in detail, see a specific dashboard:

          - <a target="_blank" href="%(rulerDashboardURL)s">Ruler</a>
          - <a target="_blank" href="%(alertmanagerDashboardURL)s">Alertmanager</a>
          - <a target="_blank" href="%(alertmanagerResourcesDashboardURL)s">Alertmanager resources</a>
          - <a target="_blank" href="%(overviewResourcesDashboardURL)s">Overview resources</a>
          - <a target="_blank" href="%(overviewNetworkingDashboardURL)s">Overview networking</a>
        ||| % helpers),
      )
      .addPanel(
        $.timeseriesPanel('Rule evaluations / sec') +
        $.successFailureCustomPanel(
          [
            $.queries.ruler.evaluations.successPerSecond,
            $.queries.ruler.evaluations.failurePerSecond,
            $.queries.ruler.evaluations.missedIterationsPerSecond,
          ],
          ['success', 'failed', 'missed'],
        )
      )
      .addPanel(
        $.timeseriesPanel('Rule evaluations latency') +
        $.queryPanel(
          $.queries.ruler.evaluations.latency,
          'average'
        ) +
        { fieldConfig+: { defaults+: { unit: 's' } } },
      )
      .addPanel(
        $.timeseriesPanel('Alerting notifications sent to Alertmanager / sec') +
        $.successFailurePanel($.queries.ruler.notifications.successPerSecond, $.queries.ruler.notifications.failurePerSecond) +
        $.stack
      )
    )

    .addRow(
      $.row('Long-term storage (object storage)')
      .addPanel(
        $.textPanel('', |||
          These panels show an overview on the long-term storage (object storage).
          To examine the storage in detail, see a specific dashboard:

          - <a target="_blank" href="%(objectStoreDashboardURL)s">Object store</a>
          - <a target="_blank" href="%(compactorDashboardURL)s">Compactor</a>
        ||| % helpers),
      )
      .addPanel(
        $.timeseriesPanel('Requests / sec') +
        $.successFailurePanel($.queries.storage.successPerSecond, $.queries.storage.failurePerSecond) +
        $.stack +
        { fieldConfig+: { defaults+: { unit: 'reqps' } } }
      )
      .addPanel(
        $.timeseriesPanel('Operations / sec') +
        $.queryPanel('sum by(operation) (rate(thanos_objstore_bucket_operations_total{%s}[$__rate_interval]))' % $.namespaceMatcher(), '{{operation}}') +
        $.panelSeriesNonErrorColorsPalette(['attributes', 'delete', 'exists', 'get', 'get_range', 'iter', 'upload']) +
        $.stack +
        { fieldConfig+: { defaults+: { unit: 'reqps' } } },
      )
      .addPanel(
        $.timeseriesPanel('Total number of blocks in the storage') +
        // Look at the max over the last 15m to correctly work during rollouts
        // (the metrics disappear until the next cleanup runs).
        $.queryPanel('sum(max by(user) (max_over_time(cortex_bucket_blocks_count{%s}[15m])))' % $.jobMatcher($._config.job_names.compactor), 'blocks'),
      )
    ),
}
