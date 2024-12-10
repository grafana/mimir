local utils = import 'mixin-utils/utils.libsonnet';

{
  // Helper function to produce failure rate in percentage queries for native and classic histograms.
  // Takes a metric name and a selector as strings and returns a dictionary with classic and native queries.
  ncHistogramFailureRate(metric, selector):: {
    local template = |||
      (
          # gRPC errors are not tracked as 5xx but "error".
          sum(%(countFailQuery)s)
          or
          # Handle the case no failure has been tracked yet.
          vector(0)
      )
      /
      sum(%(countQuery)s)
    |||,
    classic: template % {
      countFailQuery: utils.ncHistogramCountRate(metric, selector + ',status_code=~"5.*|error"').classic,
      countQuery: utils.ncHistogramCountRate(metric, selector).classic,
    },
    native: template % {
      countFailQuery: utils.ncHistogramCountRate(metric, selector + ',status_code=~"5.*|error"').native,
      countQuery: utils.ncHistogramCountRate(metric, selector).native,
    },
  },

  ncSumHistogramCountRate(metric, selectors, extra_selector, rate_interval='$__rate_interval')::
    local selectorsStr = $.toPrometheusSelector(selectors);
    local extendedSelectorsStr = $.toPrometheusSelector(selectors + extra_selector);
    {
      classic: 'sum(rate(%(metric)s_count%(extendedSelectors)s[%(rateInterval)s])) /\nsum(rate(%(metric)s_count%(selectors)s[%(rateInterval)s]))' % {
        metric: metric,
        rateInterval: rate_interval,
        extendedSelectors: extendedSelectorsStr,
        selectors: selectorsStr,
      },
      native: 'sum(histogram_count(rate(%(metric)s%(extendedSelectors)s[%(rateInterval)s]))) /\nsum(histogram_count(rate(%(metric)s%(selectors)s[%(rateInterval)s])))' % {
        metric: metric,
        rateInterval: rate_interval,
        extendedSelectors: extendedSelectorsStr,
        selectors: selectorsStr,
      },
    },

  ncAvgHistogramQuantile(quantile, metric, selectors, offset, rate_interval='$__rate_interval')::
    local labels = std.join('_', [matcher.label for matcher in selectors]);
    local metricStr = '%(labels)s:%(metric)s' % { labels: labels, metric: metric };
    local selectorsStr = $.toPrometheusSelector(selectors);
    {
      classic: |||
        1 - (
          avg_over_time(histogram_quantile(%(quantile)s, sum by (le) (%(metric)s_bucket:sum_rate%(selectors)s offset %(offset)s))[%(rateInterval)s])
          /
          avg_over_time(histogram_quantile(%(quantile)s, sum by (le) (%(metric)s_bucket:sum_rate%(selectors)s))[%(rateInterval)s])
        )
      ||| % {
        quantile: quantile,
        metric: metricStr,
        selectors: selectorsStr,
        offset: offset,
        rateInterval: rate_interval,
      },
      native: |||
        1 - (
          avg_over_time(histogram_quantile(%(quantile)s, sum(%(metric)s:sum_rate%(selectors)s offset %(offset)s))[%(rateInterval)s])
          /
          avg_over_time(histogram_quantile(%(quantile)s, sum(%(metric)s:sum_rate%(selectors)s))[%(rateInterval)s])
        )
      ||| % {
        quantile: quantile,
        metric: metricStr,
        selectors: selectorsStr,
        offset: offset,
        rateInterval: rate_interval,
      },
    },

  // This object contains common queries used in the Mimir dashboards.
  // These queries are NOT intended to be configurable or overriddeable via jsonnet,
  // but they're defined in a common place just to share them between different dashboards.
  queries:: {
    // Define the supported replacement variables in a single place. Most of them are frequently used.
    local variables = {
      requestsPerSecondMetric: $.requests_per_second_metric,
      gatewayMatcher: $.jobMatcher($._config.job_names.gateway),
      distributorMatcher: $.jobMatcher($._config.job_names.distributor),
      ingesterMatcher: $.jobMatcher($._config.job_names.ingester),
      queryFrontendMatcher: $.jobMatcher($._config.job_names.query_frontend),
      rulerMatcher: $.jobMatcher($._config.job_names.ruler),
      alertmanagerMatcher: $.jobMatcher($._config.job_names.alertmanager),
      namespaceMatcher: $.namespaceMatcher(),
      storeGatewayMatcher: $.jobMatcher($._config.job_names.store_gateway),
      rulerQueryFrontendMatcher: $.jobMatcher($._config.job_names.ruler_query_frontend),
      writeHTTPRoutesRegex: $.queries.write_http_routes_regex,
      writeDistributorRoutesRegex: std.join('|', [$.queries.write_grpc_distributor_routes_regex, $.queries.write_http_routes_regex]),
      writeGRPCIngesterRoute: $.queries.write_grpc_ingester_route,
      readHTTPRoutesRegex: $.queries.read_http_routes_regex,
      readGRPCIngesterRoute: $.queries.read_grpc_ingester_route,
      readGRPCStoreGatewayRoute: $.queries.read_grpc_store_gateway_route,
      rulerQueryFrontendRoutesRegex: $.queries.ruler_query_frontend_routes_regex,
      perClusterLabel: $._config.per_cluster_label,
      recordingRulePrefix: $.recordingRulePrefix($.jobSelector('any')),  // The job name does not matter here.
      groupPrefixJobs: $._config.group_prefix_jobs,
      instance: $._config.per_instance_label,
    },

    requests_per_second_metric: 'cortex_request_duration_seconds',
    write_http_routes_regex: 'api_(v1|prom)_push|otlp_v1_metrics',
    write_grpc_distributor_routes_regex: '/distributor.Distributor/Push|/httpgrpc.*',
    write_grpc_ingester_route: '/cortex.Ingester/Push',
    read_http_routes_regex: '(prometheus|api_prom)_api_v1_.+',
    read_grpc_ingester_route: $._config.ingester_read_path_routes_regex,
    read_grpc_store_gateway_route: $._config.store_gateway_read_path_routes_regex,
    query_http_routes_regex: '(prometheus|api_prom)_api_v1_query(_range)?',
    alertmanager_http_routes_regex: 'api_v1_alerts|alertmanager',
    alertmanager_grpc_routes_regex: '/alertmanagerpb.Alertmanager/HandleRequest',
    // Both support gRPC and HTTP requests. HTTP request is used when rule evaluation query requests go through the query-tee.
    ruler_query_frontend_routes_regex: '/httpgrpc.HTTP/Handle|.*api_v1_query',

    gateway: {
      local p = self,
      requestsPerSecondMetric: $.queries.requests_per_second_metric,
      writeRequestsPerSecondSelector: '%(gatewayMatcher)s, route=~"%(writeHTTPRoutesRegex)s"' % variables,
      readRequestsPerSecondSelector: '%(gatewayMatcher)s, route=~"%(readHTTPRoutesRegex)s"' % variables,

      // Write failures rate as percentage of total requests.
      writeFailuresRate: $.ncHistogramFailureRate(p.requestsPerSecondMetric, p.writeRequestsPerSecondSelector),

      // Read failures rate as percentage of total requests.
      readFailuresRate: $.ncHistogramFailureRate(p.requestsPerSecondMetric, p.readRequestsPerSecondSelector),
    },

    distributor: {
      local p = self,
      requestsPerSecondMetric: $.queries.requests_per_second_metric,
      writeRequestsPerSecondRouteRegex: '%(writeDistributorRoutesRegex)s' % variables,
      writeRequestsPerSecondSelector: '%(distributorMatcher)s, route=~"%(writeDistributorRoutesRegex)s"' % variables,
      samplesPerSecond: 'sum(%(groupPrefixJobs)s:cortex_distributor_received_samples:rate5m{%(distributorMatcher)s})' % variables,
      exemplarsPerSecond: 'sum(%(groupPrefixJobs)s:cortex_distributor_received_exemplars:rate5m{%(distributorMatcher)s})' % variables,

      // Write failures rate as percentage of total requests.
      writeFailuresRate: $.ncHistogramFailureRate(p.requestsPerSecondMetric, p.writeRequestsPerSecondSelector),
    },

    query_frontend: {
      local p = self,
      requestsPerSecondMetric: $.queries.requests_per_second_metric,
      readRequestsPerSecondSelector: '%(queryFrontendMatcher)s, route=~"%(readHTTPRoutesRegex)s"' % variables,
      // These query routes are used in the overview and other dashboard, everythign else is considered "other" queries.
      // Has to be a list to keep the same colors as before, see overridesNonErrorColorsPalette.
      local overviewRoutes = [
        { name: 'instantQuery', displayName: 'instant queries', route: '/api/v1/query', routeLabel: '_api_v1_query' },
        { name: 'rangeQuery', displayName: 'range queries', route: '/api/v1/query_range', routeLabel: '_api_v1_query_range' },
        { name: 'labelNames', displayName: '"label names" queries', route: '/api/v1/labels', routeLabel: '_api_v1_labels' },
        { name: 'labelValues', displayName: '"label values" queries', route: '/api/v1/label_name_values', routeLabel: '_api_v1_label_name_values' },
        { name: 'series', displayName: 'series queries', route: '/api/v1/series', routeLabel: '_api_v1_series' },
        { name: 'remoteRead', displayName: 'remote read queries', route: '/api/v1/read', routeLabel: '_api_v1_read' },
        { name: 'metadata', displayName: 'metadata queries', route: '/api/v1/metadata', routeLabel: '_api_v1_metadata' },
        { name: 'exemplars', displayName: 'exemplar queries', route: '/api/v1/query_exemplars', routeLabel: '_api_v1_query_exemplars' },
        { name: 'activeSeries', displayName: '"active series" queries', route: '/api/v1/cardinality_active_series', routeLabel: '_api_v1_cardinality_active_series' },
        { name: 'labelNamesCardinality', displayName: '"label name cardinality" queries', route: '/api/v1/cardinality_label_names', routeLabel: '_api_v1_cardinality_label_names' },
        { name: 'labelValuesCardinality', displayName: '"label value cardinality" queries', route: '/api/v1/cardinality_label_values', routeLabel: '_api_v1_cardinality_label_values' },
      ],
      local overviewRoutesRegex = '(prometheus|api_prom)(%s)' % std.join('|', [r.routeLabel for r in overviewRoutes]),
      overviewRoutesOverrides: [
        {
          matcher: {
            id: 'byRegexp',
            // To distinguish between query and query_range, we need to match the route with a negative lookahead.
            options: '/.*%s($|[^_])/' % r.routeLabel,
          },
          properties: [
            {
              id: 'displayName',
              value: r.displayName,
            },
          ],
        }
        for r in overviewRoutes
      ],
      overviewRoutesPerSecondMetric: 'cortex_request_duration_seconds',
      overviewRoutesPerSecondSelector: '%(queryFrontendMatcher)s,route=~"%(overviewRoutesRegex)s"' % (variables { overviewRoutesRegex: overviewRoutesRegex }),
      nonOverviewRoutesPerSecondSelector: '%(queryFrontendMatcher)s,route=~"(prometheus|api_prom)_api_v1_.*",route!~"%(overviewRoutesRegex)s"' % (variables { overviewRoutesRegex: overviewRoutesRegex }),

      local ncQueryPerSecond(name) = utils.ncHistogramSumBy(utils.ncHistogramCountRate(p.requestsPerSecondMetric, '%(queryFrontendMatcher)s,route=~"(prometheus|api_prom)%(route)s"' %
                                                                                                                  (variables { route: std.filter(function(r) r.name == name, overviewRoutes)[0].routeLabel }))),
      ncInstantQueriesPerSecond: ncQueryPerSecond('instantQuery'),
      ncRangeQueriesPerSecond: ncQueryPerSecond('rangeQuery'),
      ncLabelNamesQueriesPerSecond: ncQueryPerSecond('labelNames'),
      ncLabelValuesQueriesPerSecond: ncQueryPerSecond('labelValues'),
      ncSeriesQueriesPerSecond: ncQueryPerSecond('series'),

      // Read failures rate as percentage of total requests.
      readFailuresRate: $.ncHistogramFailureRate(p.requestsPerSecondMetric, p.readRequestsPerSecondSelector),
    },

    ruler: {
      requestsPerSecondMetric: $.queries.requests_per_second_metric,
      evaluations: {
        successPerSecond:
          |||
            sum(rate(cortex_prometheus_rule_evaluations_total{%(rulerMatcher)s}[$__rate_interval]))
            -
            sum(rate(cortex_prometheus_rule_evaluation_failures_total{%(rulerMatcher)s}[$__rate_interval]))
          ||| % variables,
        failurePerSecond: 'sum(rate(cortex_prometheus_rule_evaluation_failures_total{%(rulerMatcher)s}[$__rate_interval]))' % variables,
        missedIterationsPerSecond: 'sum(rate(cortex_prometheus_rule_group_iterations_missed_total{%(rulerMatcher)s}[$__rate_interval]))' % variables,
        latency:
          |||
            sum (rate(cortex_prometheus_rule_evaluation_duration_seconds_sum{%(rulerMatcher)s}[$__rate_interval]))
              /
            sum (rate(cortex_prometheus_rule_evaluation_duration_seconds_count{%(rulerMatcher)s}[$__rate_interval]))
          ||| % variables,

        // Rule evaluation failures rate as percentage of total requests.
        failuresRate: |||
          (
            (
                sum(rate(cortex_prometheus_rule_evaluation_failures_total{%(rulerMatcher)s}[$__rate_interval]))
                +
                # Consider missed evaluations as failures.
                sum(rate(cortex_prometheus_rule_group_iterations_missed_total{%(rulerMatcher)s}[$__rate_interval]))
            )
            or
            # Handle the case no failure has been tracked yet.
            vector(0)
          )
          /
          sum(rate(cortex_prometheus_rule_evaluations_total{%(rulerMatcher)s}[$__rate_interval]))
        ||| % variables,
      },
      notifications: {
        // Notifications / sec attempted to send to the Alertmanager.
        totalPerSecond: |||
          sum(rate(cortex_prometheus_notifications_sent_total{%(rulerMatcher)s}[$__rate_interval]))
        ||| % variables,

        // Notifications / sec successfully sent to the Alertmanager.
        successPerSecond: |||
          sum(rate(cortex_prometheus_notifications_sent_total{%(rulerMatcher)s}[$__rate_interval]))
            -
          sum(rate(cortex_prometheus_notifications_errors_total{%(rulerMatcher)s}[$__rate_interval]))
        ||| % variables,

        // Notifications / sec failed to be sent to the Alertmanager.
        failurePerSecond: |||
          sum(rate(cortex_prometheus_notifications_errors_total{%(rulerMatcher)s}[$__rate_interval]))
        ||| % variables,
      },
    },

    alertmanager: {
      requestsPerSecondMetric: $.queries.requests_per_second_metric,
      notifications: {
        // Notifications / sec attempted to deliver by the Alertmanager to the receivers.
        totalPerSecond: |||
          sum(%(recordingRulePrefix)s_integration:cortex_alertmanager_notifications_total:rate5m{%(alertmanagerMatcher)s})
        ||| % variables,

        // Notifications / sec successfully delivered by the Alertmanager to the receivers.
        successPerSecond: |||
          sum(%(recordingRulePrefix)s_integration:cortex_alertmanager_notifications_total:rate5m{%(alertmanagerMatcher)s})
          -
          sum(%(recordingRulePrefix)s_integration:cortex_alertmanager_notifications_failed_total:rate5m{%(alertmanagerMatcher)s})
        ||| % variables,

        // Notifications / sec failed to be delivered by the Alertmanager to the receivers.
        failurePerSecond: |||
          sum(%(recordingRulePrefix)s_integration:cortex_alertmanager_notifications_failed_total:rate5m{%(alertmanagerMatcher)s})
        ||| % variables,
      },
    },

    storage: {
      successPerSecond: |||
        sum(rate(thanos_objstore_bucket_operations_total{%(namespaceMatcher)s}[$__rate_interval]))
        -
        sum(rate(thanos_objstore_bucket_operation_failures_total{%(namespaceMatcher)s}[$__rate_interval]))
      ||| % variables,
      failurePerSecond: |||
        sum(rate(thanos_objstore_bucket_operation_failures_total{%(namespaceMatcher)s}[$__rate_interval]))
      ||| % variables,

      // Object storage operation failures rate as percentage of total operations.
      failuresRate: |||
        sum(rate(thanos_objstore_bucket_operation_failures_total{%(namespaceMatcher)s}[$__rate_interval]))
        /
        sum(rate(thanos_objstore_bucket_operations_total{%(namespaceMatcher)s}[$__rate_interval]))
      ||| % variables,
    },

    ingester: {
      requestsPerSecondMetric: $.queries.requests_per_second_metric,
      readRequestsPerSecondSelector: '%(ingesterMatcher)s,route=~"%(readGRPCIngesterRoute)s"' % variables,
      writeRequestsPerSecondSelector: '%(ingesterMatcher)s, route="%(writeGRPCIngesterRoute)s"' % variables,

      ingestOrClassicDeduplicatedQuery(perIngesterQuery, groupByLabels=''):: |||
        ( # Classic storage
          sum by (%(groupByCluster)s, %(groupByLabels)s) (
            %(perIngesterQuery)s unless on (job)
            cortex_partition_ring_partitions{%(ingester)s}
          )
          / on (%(groupByCluster)s) group_left()
          max by (%(groupByCluster)s) (cortex_distributor_replication_factor{%(distributor)s})
        )
        or
        ( # Ingest storage
          sum by (%(groupByCluster)s, %(groupByLabels)s) (
            max by (ingester_id, %(groupByCluster)s, %(groupByLabels)s) (
              label_replace(
                %(perIngesterQuery)s,
                "ingester_id", "$1", "%(instance)s", ".*-([0-9]+)$"
              )
            )
          )
        )
      ||| % {
        perIngesterQuery: perIngesterQuery,
        instance: variables.instance,
        groupByLabels: groupByLabels,
        groupByCluster: $._config.group_by_cluster,
        distributor: variables.distributorMatcher,
        ingester: variables.ingesterMatcher,
      },
    },

    store_gateway: {
      requestsPerSecondMetric: $.queries.requests_per_second_metric,
      readRequestsPerSecondSelector: '%(storeGatewayMatcher)s,route=~"%(readGRPCStoreGatewayRoute)s"' % variables,
    },

    ruler_query_frontend: {
      requestsPerSecondMetric: $.queries.requests_per_second_metric,
      readRequestsPerSecondSelector: '%(rulerQueryFrontendMatcher)s,route=~"%(rulerQueryFrontendRoutesRegex)s"' % variables,
    },
  },
}
