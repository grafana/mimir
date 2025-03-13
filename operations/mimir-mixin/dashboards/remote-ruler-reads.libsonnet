local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-remote-ruler-reads.json';

(import 'dashboard-utils.libsonnet') +
(import 'dashboard-queries.libsonnet') {
  [filename]:
    assert std.md5(filename) == 'f103238f7f5ab2f1345ce650cbfbfe2f' : 'UID of the dashboard has changed, please update references to dashboard.';
    ($.dashboard('Remote ruler reads') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates()
    .addShowNativeLatencyVariable()
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
        $.statPanel(utils.ncHistogramSumBy(utils.ncHistogramCountRate($.queries.ruler_query_frontend.requestsPerSecondMetric, $.queries.ruler_query_frontend.readRequestsPerSecondSelector)), format='reqps') +
        $.panelDescription(
          'Evaluations per second',
          |||
            Rate of rule expressions evaluated per second.
          |||
        ),
      )
    )
    .addRows($.commonReadsDashboardsRows(
      queryFrontendJobName=$._config.job_names.ruler_query_frontend,
      querySchedulerJobName=$._config.job_names.ruler_query_scheduler,
      querierJobName=$._config.job_names.ruler_querier,
      queryRoutesRegex=$.queries.ruler_query_frontend_routes_regex,
      queryPathDescription='ruler query path',
      rowTitlePrefix='Ruler-',
    ))
    .addRowIf(
      $._config.autoscaling.ruler_querier.enabled,
      $.row('Ruler-querier - autoscaling')
      .addPanel(
        $.autoScalingActualReplicas('ruler_querier')
      )
      .addPanel(
        $.autoScalingFailuresPanel('ruler_querier')
      )
    )
    .addRowIf(
      $._config.autoscaling.ruler_querier.enabled,
      $.row('')
      .addPanel(
        $.autoScalingDesiredReplicasByAverageValueScalingMetricPanel('ruler_querier', 'CPU', 'cpu')
      )
      .addPanel(
        $.autoScalingDesiredReplicasByAverageValueScalingMetricPanel('ruler_querier', 'memory', 'memory')
      )
      .addPanel(
        $.autoScalingDesiredReplicasByAverageValueScalingMetricPanel('ruler_querier', 'in-flight queries', 'queries')
      )
    ),
}
