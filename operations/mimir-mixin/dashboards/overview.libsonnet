local utils = import 'mixin-utils/utils.libsonnet';
local grafana = import 'grafonnet-latest/main.libsonnet',
      grid = grafana.util.grid,
      row = grafana.panel.row;
local filename = 'mimir-overview.json';

{
  grafanaDashboards+: {
    [filename]:
      // Dashboard helpers and queries, configured with the mixin config.
      local lib =
        (import 'dashboard-utils.libsonnet') +
        (import 'dashboard-queries.libsonnet') +
        { _config:: $._config + $._group_config };

      // Request rate and latency are gathered from the gateway when enabled: say so in the row
      // description and the panel titles.
      local gatewayRowDescription = if lib._config.gateway_enabled then 'Requests rate and latency is measured on the gateway.' else '';
      local gatewayPanelTitle(title) = if lib._config.gateway_enabled then title + ' (gateway)' else title;

      local url = {
        alertmanager: lib.dashboardURL('mimir-alertmanager.json'),
        alertmanagerResources: lib.dashboardURL('mimir-alertmanager-resources.json'),
        compactor: lib.dashboardURL('mimir-compactor.json'),
        objectStorage: lib.dashboardURL('mimir-object-store.json'),
        overviewNetworking: lib.dashboardURL('mimir-overview-networking.json'),
        overviewResources: lib.dashboardURL('mimir-overview-resources.json'),
        queries: lib.dashboardURL('mimir-queries.json'),
        reads: lib.dashboardURL('mimir-reads.json'),
        readsNetworking: lib.dashboardURL('mimir-reads-networking.json'),
        readsResources: lib.dashboardURL('mimir-reads-resources.json'),
        ruler: lib.dashboardURL('mimir-ruler.json'),
        writes: lib.dashboardURL('mimir-writes.json'),
        writesNetworking: lib.dashboardURL('mimir-writes-networking.json'),
        writesResources: lib.dashboardURL('mimir-writes-resources.json'),
      };

      // Request metrics are gathered from the gateway when enabled, otherwise from the distributor (writes)
      // and query-frontend (reads).
      local writes = {
        metric: if lib._config.gateway_enabled then lib.queries.gateway.requestsPerSecondMetric else lib.queries.distributor.requestsPerSecondMetric,
        selector: if lib._config.gateway_enabled then lib.queries.gateway.writeRequestsPerSecondSelector else lib.queries.distributor.writeRequestsPerSecondSelector,
        latencySelector:
          if lib._config.gateway_enabled
          then lib.jobSelector(lib._config.job_names.gateway) + [utils.selector.re('route', lib.queries.write_http_routes_regex)]
          else lib.jobSelector(lib._config.job_names.distributor) + [utils.selector.re('route', '%s' % lib.queries.distributor.writeRequestsPerSecondRouteRegex)],
        failuresRate: if lib._config.gateway_enabled then lib.queries.gateway.writeFailuresRate else lib.queries.distributor.writeFailuresRate,
      };

      local reads = {
        metric: if lib._config.gateway_enabled then lib.queries.gateway.requestsPerSecondMetric else lib.queries.query_frontend.requestsPerSecondMetric,
        selector: if lib._config.gateway_enabled then lib.queries.gateway.readRequestsPerSecondSelector else lib.queries.query_frontend.readRequestsPerSecondSelector,
        latencySelector:
          (if lib._config.gateway_enabled then lib.jobSelector(lib._config.job_names.gateway) else lib.jobSelector(lib._config.job_names.query_frontend))
          + [utils.selector.re('route', lib.queries.read_http_routes_regex)],
        failuresRate: if lib._config.gateway_enabled then lib.queries.gateway.readFailuresRate else lib.queries.query_frontend.readFailuresRate,
      };

      // Panels for the cluster health section.

      local clusterHealthTextPanel =
        lib.rowHelperText(|||
          The 'Status' panel shows an overview on the cluster health over the time.
          To investigate failures, see a specific dashboard:

          - <a target="_blank" href="%(writes)s">Writes</a>
          - <a target="_blank" href="%(reads)s">Reads</a>
          - <a target="_blank" href="%(ruler)s">Rule evaluations</a>
          - <a target="_blank" href="%(alertmanager)s">Alerting notifications</a>
          - <a target="_blank" href="%(objectStorage)s">Object storage</a>
        ||| % url);

      local statusTimelinePanel =
        lib.stateTimelinePanel(
          'Status',
          [
            // Write and read failures, from both native and classic histograms.
            utils.showNativeHistogramQuery(writes.failuresRate),
            utils.showClassicHistogramQuery(writes.failuresRate),
            utils.showNativeHistogramQuery(reads.failuresRate),
            utils.showClassicHistogramQuery(reads.failuresRate),
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
        );

      local alertListPanel =
        lib.alertListPanel('Firing alerts', lib._config.product, lib.namespaceMatcher());

      // Panels for the writes section.

      local writesTextPanel =
        lib.rowHelperText(|||
          These panels show an overview on the write path. %(gatewayRowDescription)s
          To examine the write path in detail, see a specific dashboard:

          - <a target="_blank" href="%(writes)s">Writes</a>
          - <a target="_blank" href="%(writesResources)s">Writes resources</a>
          - <a target="_blank" href="%(writesNetworking)s">Writes networking</a>
          - <a target="_blank" href="%(overviewResources)s">Overview resources</a>
          - <a target="_blank" href="%(overviewNetworking)s">Overview networking</a>
        ||| % (url { gatewayRowDescription: gatewayRowDescription }));

      local writeRequestsPanel =
        lib.timeseriesPanel(gatewayPanelTitle('Write requests / sec')) +
        lib.qpsPanelNativeHistogram(writes.metric, writes.selector);

      local writeLatencyPanel =
        lib.timeseriesPanel(gatewayPanelTitle('Write latency')) +
        lib.latencyRecordingRulePanelNativeHistogram(writes.metric, writes.latencySelector);

      local ingestionRatePanel =
        lib.timeseriesPanel('Ingestion / sec') +
        lib.queryPanel(
          [lib.queries.distributor.samplesPerSecond, lib.queries.distributor.exemplarsPerSecond],
          ['samples / sec', 'exemplars / sec'],
        ) +
        lib.stack +
        { fieldConfig+: { defaults+: { unit: 'cps' } } };

      // Panels for the reads section.

      local readsTextPanel =
        lib.rowHelperText(|||
          These panels show an overview on the read path. %(gatewayRowDescription)s
          To examine the read path in detail, see a specific dashboard:

          - <a target="_blank" href="%(reads)s">Reads</a>
          - <a target="_blank" href="%(readsResources)s">Reads resources</a>
          - <a target="_blank" href="%(readsNetworking)s">Reads networking</a>
          - <a target="_blank" href="%(overviewResources)s">Overview resources</a>
          - <a target="_blank" href="%(overviewNetworking)s">Overview networking</a>
          - <a target="_blank" href="%(queries)s">Queries</a>
          - <a target="_blank" href="%(compactor)s">Compactor</a>
        ||| % (url { gatewayRowDescription: gatewayRowDescription }));

      local readRequestsPanel =
        lib.timeseriesPanel(gatewayPanelTitle('Read requests / sec')) +
        lib.qpsPanelNativeHistogram(reads.metric, reads.selector);

      local readLatencyPanel =
        lib.timeseriesPanel(gatewayPanelTitle('Read latency')) +
        lib.latencyRecordingRulePanelNativeHistogram(reads.metric, reads.latencySelector);

      local queriesRatePanel =
        // Queries per route, with the routes not explicitly listed in the overview grouped as "other".
        local routesRate(selector, sumBy=[]) =
          utils.ncHistogramSumBy(utils.ncHistogramCountRate(lib.queries.query_frontend.overviewRoutesPerSecondMetric, selector), sumBy);
        local byRoute = routesRate(lib.queries.query_frontend.overviewRoutesPerSecondSelector, ['route']);
        local other = routesRate(lib.queries.query_frontend.nonOverviewRoutesPerSecondSelector);

        lib.timeseriesPanel('Queries / sec') +
        {
          targets: [
            { expr: utils.showClassicHistogramQuery(byRoute), format: 'time_series', legendLink: null },
            { expr: utils.showNativeHistogramQuery(byRoute), format: 'time_series', legendLink: null },
            { expr: utils.showClassicHistogramQuery(other), format: 'time_series', legendFormat: 'other', legendLink: null },
            { expr: utils.showNativeHistogramQuery(other), format: 'time_series', legendFormat: 'other', legendLink: null },
          ],
        } +
        {
          fieldConfig+: {
            defaults+: { unit: 'reqps' },
            overrides+: lib.overridesNonErrorColorsPalette(lib.queries.query_frontend.overviewRoutesOverrides),
          },
        } +
        lib.stack;

      // Panels for the recording and alerting rules section.

      local rulesTextPanel =
        lib.rowHelperText(|||
          These panels show an overview on the recording and alerting rules evaluation.
          To examine the rules evaluation and alerts notifications in detail, see a specific dashboard:

          - <a target="_blank" href="%(ruler)s">Ruler</a>
          - <a target="_blank" href="%(alertmanager)s">Alertmanager</a>
          - <a target="_blank" href="%(alertmanagerResources)s">Alertmanager resources</a>
          - <a target="_blank" href="%(overviewResources)s">Overview resources</a>
          - <a target="_blank" href="%(overviewNetworking)s">Overview networking</a>
        ||| % url);

      local rulesEvaluationPanel =
        lib.timeseriesPanel('Rule evaluations / sec') +
        lib.successFailureCustomPanel(
          [
            lib.queries.ruler.evaluations.successPerSecond,
            lib.queries.ruler.evaluations.failurePerSecond,
            lib.queries.ruler.evaluations.missedIterationsPerSecond,
          ],
          ['success', 'failed', 'missed'],
        );

      local rulesEvaluationLatencyPanel =
        lib.timeseriesPanel('Rule evaluations latency') +
        lib.queryPanel(lib.queries.ruler.evaluations.latency, 'average') +
        { fieldConfig+: { defaults+: { unit: 's' } } };

      local alertingNotificationsPanel =
        lib.timeseriesPanel('Alerting notifications sent to Alertmanager / sec') +
        lib.successFailurePanel(lib.queries.ruler.notifications.successPerSecond, lib.queries.ruler.notifications.failurePerSecond) +
        lib.stack;

      // Panels for the long-term storage (object storage) section.

      local objectStorageTextPanel =
        lib.rowHelperText(|||
          These panels show an overview on the long-term storage (object storage).
          To examine the storage in detail, see a specific dashboard:

          - <a target="_blank" href="%(objectStorage)s">Object store</a>
          - <a target="_blank" href="%(compactor)s">Compactor</a>
        ||| % url);

      local objectStorageRequestsPanel =
        lib.timeseriesPanel('Requests / sec') +
        lib.successFailurePanel(lib.queries.storage.successPerSecond, lib.queries.storage.failurePerSecond) +
        lib.stack +
        { fieldConfig+: { defaults+: { unit: 'reqps' } } };

      local objectStorageOperationsPanel =
        lib.timeseriesPanel('Operations / sec') +
        lib.queryPanel('sum by(operation) (rate(thanos_objstore_bucket_operations_total{%s}[$__rate_interval]))' % lib.namespaceMatcher(), '{{operation}}') +
        lib.panelSeriesNonErrorColorsPalette(['attributes', 'delete', 'exists', 'get', 'get_range', 'iter', 'upload']) +
        lib.stack +
        { fieldConfig+: { defaults+: { unit: 'reqps' } } };

      local objectStorageBlocksPanel =
        lib.timeseriesPanel('Total number of blocks in the storage') +
        // Look at the max over the last 15m to correctly work during rollouts
        // (the metrics disappear until the next cleanup runs).
        lib.queryPanel('sum(max by(user) (max_over_time(cortex_bucket_blocks_count{%s}[15m])))' % lib.jobMatcher(lib._config.job_names.compactor), 'blocks');

      // grid.makeGrid() sets the same width on every panel, so patch the gridPos of the panels
      // that need a custom one, keyed by panel title: the status takes more space than the others,
      // while the text panels keep the same width across all rows.
      local adjustGridPos(panels, patches) = [
        if std.objectHas(patches, p.title) then p { gridPos+: patches[p.title] } else p
        for p in panels
      ];

      local gridPosPatches = {
        [statusTimelinePanel.title]: { w: 12 },
        [alertListPanel.title]: { x: 18 },
      };

      local panels = adjustGridPos(
        grid.makeGrid([
          row.new('%(product)s cluster health' % lib._config)
          + row.withPanels([
            clusterHealthTextPanel,
            statusTimelinePanel,
            alertListPanel,
          ]),
          row.new('Writes')
          + row.withPanels([
            writesTextPanel,
            writeRequestsPanel,
            writeLatencyPanel,
            ingestionRatePanel,
          ]),
          row.new('Reads')
          + row.withPanels([
            readsTextPanel,
            readRequestsPanel,
            readLatencyPanel,
            queriesRatePanel,
          ]),
          row.new('Recording and alerting rules')
          + row.withPanels([
            rulesTextPanel,
            rulesEvaluationPanel,
            rulesEvaluationLatencyPanel,
            alertingNotificationsPanel,
          ]),
          row.new('Long-term storage (object storage)')
          + row.withPanels([
            objectStorageTextPanel,
            objectStorageRequestsPanel,
            objectStorageOperationsPanel,
            objectStorageBlocksPanel,
          ]),
        ], panelWidth=6),
        gridPosPatches,
      );

      assert std.md5(filename) == 'ffcd83628d7d4b5a03d1cafd159e6c9c' : 'UID of the dashboard has changed, please update references to dashboard.';
      (lib.dashboard('Overview') + { uid: std.md5(filename) })
      .addClusterSelectorTemplates()
      .addShowNativeLatencyVariable(lib.latencyVariableDefault())
      .addGridPanels(panels),
  },
}
