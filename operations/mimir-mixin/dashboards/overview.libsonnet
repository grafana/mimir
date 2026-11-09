local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-overview.json';

{
  grafanaDashboards+: {
    [filename]:
      // Dashboard helpers and queries, configured with the mixin config.
      local lib =
        (import 'dashboard-utils.libsonnet') +
        (import 'dashboard-queries.libsonnet') +
        { _config:: $._config + $._group_config };

      local helpers = {
        // Adds a suffix to the title of panels whose metrics are gathered from the gateway.
        gatewayEnabledPanelTitleSuffix: if !lib._config.gateway_enabled then '' else
          '(gateway)',

        // Adds an extra description to rows containing panels whose metrics are gathered from the gateway.
        gatewayEnabledRowDescription: if !lib._config.gateway_enabled then '' else
          'Requests rate and latency is measured on the gateway.',

        // Dashboard URLs.
        alertmanagerDashboardURL: lib.dashboardURL('mimir-alertmanager.json'),
        alertmanagerResourcesDashboardURL: lib.dashboardURL('mimir-alertmanager-resources.json'),
        compactorDashboardURL: lib.dashboardURL('mimir-compactor.json'),
        objectStoreDashboardURL: lib.dashboardURL('mimir-object-store.json'),
        overviewNetworkingDashboardURL: lib.dashboardURL('mimir-overview-networking.json'),
        overviewResourcesDashboardURL: lib.dashboardURL('mimir-overview-resources.json'),
        queriesDashboardURL: lib.dashboardURL('mimir-queries.json'),
        readsDashboardURL: lib.dashboardURL('mimir-reads.json'),
        readsNetworkingDashboardURL: lib.dashboardURL('mimir-reads-networking.json'),
        readsResourcesDashboardURL: lib.dashboardURL('mimir-reads-resources.json'),
        rulerDashboardURL: lib.dashboardURL('mimir-ruler.json'),
        writesDashboardURL: lib.dashboardURL('mimir-writes.json'),
        writesNetworkingDashboardURL: lib.dashboardURL('mimir-writes-networking.json'),
        writesResourcesDashboardURL: lib.dashboardURL('mimir-writes-resources.json'),
      };

      assert std.md5(filename) == 'ffcd83628d7d4b5a03d1cafd159e6c9c' : 'UID of the dashboard has changed, please update references to dashboard.';
      (lib.dashboard('Overview') + { uid: std.md5(filename) })
      .addClusterSelectorTemplates()
      .addShowNativeLatencyVariable(lib.latencyVariableDefault())

      .addRow(
        lib.row('%(product)s cluster health' % lib._config)
        .addPanel(
          lib.textPanel('', |||
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
          lib.stateTimelinePanel(
            'Status',
            [
              // Write failures.
              utils.showNativeHistogramQuery(
                if lib._config.gateway_enabled then lib.queries.gateway.writeFailuresRate else lib.queries.distributor.writeFailuresRate
              ),
              // Write failures but from classic histograms.
              utils.showClassicHistogramQuery(
                if lib._config.gateway_enabled then lib.queries.gateway.writeFailuresRate else lib.queries.distributor.writeFailuresRate
              ),
              // Read failures.
              utils.showNativeHistogramQuery(
                if lib._config.gateway_enabled then lib.queries.gateway.readFailuresRate else lib.queries.query_frontend.readFailuresRate,
              ),
              // Read failures but from classic histograms.
              utils.showClassicHistogramQuery(
                if lib._config.gateway_enabled then lib.queries.gateway.readFailuresRate else lib.queries.query_frontend.readFailuresRate,
              ),
              // Rule evaluation failures.
              lib.queries.ruler.evaluations.failuresRate,
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
                rulerFailurePerSecond: lib.queries.ruler.notifications.failurePerSecond,
                rulerTotalPerSecond: lib.queries.ruler.notifications.totalPerSecond,
                alertmanagerFailurePerSecond: lib.queries.alertmanager.notifications.failurePerSecond,
                alertmanagerTotalPerSecond: lib.queries.alertmanager.notifications.totalPerSecond,
              },
              // Object storage failures.
              lib.queries.storage.failuresRate,
            ],
            ['Writes', 'Writes', 'Reads', 'Reads', 'Rule evaluations', 'Alerting notifications', 'Object storage']
          )
        )
        .addPanel(
          lib.alertListPanel('Firing alerts', lib._config.product, lib.namespaceMatcher())
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
        lib.row('Writes')
        .addPanel(
          lib.textPanel('', |||
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
          lib.timeseriesPanel(std.stripChars('Write requests / sec %(gatewayEnabledPanelTitleSuffix)s' % helpers, ' ')) +
          lib.qpsPanelNativeHistogram(
            if lib._config.gateway_enabled then
              lib.queries.gateway.requestsPerSecondMetric
            else
              lib.queries.distributor.requestsPerSecondMetric,
            if lib._config.gateway_enabled then
              lib.queries.gateway.writeRequestsPerSecondSelector
            else
              lib.queries.distributor.writeRequestsPerSecondSelector
          )
        )
        .addPanel(
          lib.timeseriesPanel(std.stripChars('Write latency %(gatewayEnabledPanelTitleSuffix)s' % helpers, ' ')) + (
            if lib._config.gateway_enabled then
              lib.latencyRecordingRulePanelNativeHistogram(lib.queries.gateway.requestsPerSecondMetric, lib.jobSelector(lib._config.job_names.gateway) + [utils.selector.re('route', lib.queries.write_http_routes_regex)])
            else
              lib.latencyRecordingRulePanelNativeHistogram(lib.queries.distributor.requestsPerSecondMetric, lib.jobSelector(lib._config.job_names.distributor) + [utils.selector.re('route', '%s' % lib.queries.distributor.writeRequestsPerSecondRouteRegex)])
          )
        )
        .addPanel(
          lib.timeseriesPanel('Ingestion / sec') +
          lib.queryPanel(
            [lib.queries.distributor.samplesPerSecond, lib.queries.distributor.exemplarsPerSecond],
            ['samples / sec', 'exemplars / sec'],
          ) +
          lib.stack +
          { fieldConfig+: { defaults+: { unit: 'cps' } } },
        )
      )

      .addRow(
        lib.row('Reads')
        .addPanel(
          lib.textPanel('', |||
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
          lib.timeseriesPanel(std.stripChars('Read requests / sec %(gatewayEnabledPanelTitleSuffix)s' % helpers, ' ')) +
          lib.qpsPanelNativeHistogram(
            if lib._config.gateway_enabled then
              lib.queries.gateway.requestsPerSecondMetric
            else
              lib.queries.query_frontend.requestsPerSecondMetric,
            if lib._config.gateway_enabled then
              lib.queries.gateway.readRequestsPerSecondSelector
            else
              lib.queries.query_frontend.readRequestsPerSecondSelector
          )
        )
        .addPanel(
          lib.timeseriesPanel(std.stripChars('Read latency %(gatewayEnabledPanelTitleSuffix)s' % helpers, ' ')) + (
            if lib._config.gateway_enabled then
              lib.latencyRecordingRulePanelNativeHistogram(lib.queries.gateway.requestsPerSecondMetric, lib.jobSelector(lib._config.job_names.gateway) + [utils.selector.re('route', lib.queries.read_http_routes_regex)])
            else
              lib.latencyRecordingRulePanelNativeHistogram(lib.queries.query_frontend.requestsPerSecondMetric, lib.jobSelector(lib._config.job_names.query_frontend) + [utils.selector.re('route', lib.queries.read_http_routes_regex)])
          )
        )
        .addPanel(
          lib.timeseriesPanel('Queries / sec') +
          {
            targets: [
              {
                expr: utils.showClassicHistogramQuery(utils.ncHistogramSumBy(utils.ncHistogramCountRate(lib.queries.query_frontend.overviewRoutesPerSecondMetric, lib.queries.query_frontend.overviewRoutesPerSecondSelector), ['route'])),
                format: 'time_series',
                legendLink: null,
              },
              {
                expr: utils.showNativeHistogramQuery(utils.ncHistogramSumBy(utils.ncHistogramCountRate(lib.queries.query_frontend.overviewRoutesPerSecondMetric, lib.queries.query_frontend.overviewRoutesPerSecondSelector), ['route'])),
                format: 'time_series',
                legendLink: null,
              },
              {
                expr: utils.showClassicHistogramQuery(utils.ncHistogramSumBy(utils.ncHistogramCountRate(lib.queries.query_frontend.overviewRoutesPerSecondMetric, lib.queries.query_frontend.nonOverviewRoutesPerSecondSelector))),
                format: 'time_series',
                legendFormat: 'other',
                legendLink: null,
              },
              {
                expr: utils.showNativeHistogramQuery(utils.ncHistogramSumBy(utils.ncHistogramCountRate(lib.queries.query_frontend.overviewRoutesPerSecondMetric, lib.queries.query_frontend.nonOverviewRoutesPerSecondSelector))),
                format: 'time_series',
                legendFormat: 'other',
                legendLink: null,
              },
            ],
          } +
          {
            fieldConfig+: {
              defaults+: { unit: 'reqps' },
              overrides+: lib.overridesNonErrorColorsPalette(lib.queries.query_frontend.overviewRoutesOverrides),
            },
          } +
          lib.stack
        )
      )

      .addRow(
        lib.row('Recording and alerting rules')
        .addPanel(
          lib.textPanel('', |||
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
          lib.timeseriesPanel('Rule evaluations / sec') +
          lib.successFailureCustomPanel(
            [
              lib.queries.ruler.evaluations.successPerSecond,
              lib.queries.ruler.evaluations.failurePerSecond,
              lib.queries.ruler.evaluations.missedIterationsPerSecond,
            ],
            ['success', 'failed', 'missed'],
          )
        )
        .addPanel(
          lib.timeseriesPanel('Rule evaluations latency') +
          lib.queryPanel(
            lib.queries.ruler.evaluations.latency,
            'average'
          ) +
          { fieldConfig+: { defaults+: { unit: 's' } } },
        )
        .addPanel(
          lib.timeseriesPanel('Alerting notifications sent to Alertmanager / sec') +
          lib.successFailurePanel(lib.queries.ruler.notifications.successPerSecond, lib.queries.ruler.notifications.failurePerSecond) +
          lib.stack
        )
      )

      .addRow(
        lib.row('Long-term storage (object storage)')
        .addPanel(
          lib.textPanel('', |||
            These panels show an overview on the long-term storage (object storage).
            To examine the storage in detail, see a specific dashboard:

            - <a target="_blank" href="%(objectStoreDashboardURL)s">Object store</a>
            - <a target="_blank" href="%(compactorDashboardURL)s">Compactor</a>
          ||| % helpers),
        )
        .addPanel(
          lib.timeseriesPanel('Requests / sec') +
          lib.successFailurePanel(lib.queries.storage.successPerSecond, lib.queries.storage.failurePerSecond) +
          lib.stack +
          { fieldConfig+: { defaults+: { unit: 'reqps' } } }
        )
        .addPanel(
          lib.timeseriesPanel('Operations / sec') +
          lib.queryPanel('sum by(operation) (rate(thanos_objstore_bucket_operations_total{%s}[$__rate_interval]))' % lib.namespaceMatcher(), '{{operation}}') +
          lib.panelSeriesNonErrorColorsPalette(['attributes', 'delete', 'exists', 'get', 'get_range', 'iter', 'upload']) +
          lib.stack +
          { fieldConfig+: { defaults+: { unit: 'reqps' } } },
        )
        .addPanel(
          lib.timeseriesPanel('Total number of blocks in the storage') +
          // Look at the max over the last 15m to correctly work during rollouts
          // (the metrics disappear until the next cleanup runs).
          lib.queryPanel('sum(max by(user) (max_over_time(cortex_bucket_blocks_count{%s}[15m])))' % lib.jobMatcher(lib._config.job_names.compactor), 'blocks'),
        )
      ),
  },
}
