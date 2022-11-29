{
  // This object contains common queries used in the Mimir dashboards.
  // These queries are NOT intended to be configurable or overriddeable via jsonnet,
  // but they're defined in a common place just to share them between different dashboards.
  queries:: {
    // Define the supported replacement variables in a single place. Most of them are frequently used.
    local variables = {
      gatewayMatcher: $.jobMatcher($._config.job_names.gateway),
      distributorMatcher: $.jobMatcher($._config.job_names.distributor),
      queryFrontendMatcher: $.jobMatcher($._config.job_names.query_frontend),
      rulerMatcher: $.jobMatcher($._config.job_names.ruler),
      alertmanagerMatcher: $.jobMatcher($._config.job_names.alertmanager),
      namespaceMatcher: $.namespaceMatcher(),
      writeHTTPRoutesRegex: $.queries.write_http_routes_regex,
      writeGRPCRoutesRegex: $.queries.write_grpc_routes_regex,
      readHTTPRoutesRegex: $.queries.read_http_routes_regex,
      perClusterLabel: $._config.per_cluster_label,
      recordingRulePrefix: $.recordingRulePrefix($.jobSelector('any')),  // The job name does not matter here.
      groupPrefixJobs: $._config.group_prefix_jobs,
    },

    write_http_routes_regex: 'api_(v1|prom)_push|otlp_v1_metrics',
    write_grpc_routes_regex: '/distributor.Distributor/Push|/httpgrpc.*',
    read_http_routes_regex: '(prometheus|api_prom)_api_v1_.+',

    gateway: {
      writeRequestsPerSecond: 'cortex_request_duration_seconds_count{%(gatewayMatcher)s, route=~"%(writeHTTPRoutesRegex)s"}' % variables,
      readRequestsPerSecond: 'cortex_request_duration_seconds_count{%(gatewayMatcher)s, route=~"%(readHTTPRoutesRegex)s"}' % variables,

      // Write failures rate as percentage of total requests.
      writeFailuresRate: |||
        (
            sum(rate(cortex_request_duration_seconds_count{%(gatewayMatcher)s, route=~"%(writeHTTPRoutesRegex)s",status_code=~"5.*"}[$__rate_interval]))
            or
            # Handle the case no failure has been tracked yet.
            vector(0)
        )
        /
        sum(rate(cortex_request_duration_seconds_count{%(gatewayMatcher)s, route=~"%(writeHTTPRoutesRegex)s"}[$__rate_interval]))
      ||| % variables,

      // Read failures rate as percentage of total requests.
      readFailuresRate: |||
        (
            sum(rate(cortex_request_duration_seconds_count{%(gatewayMatcher)s, route=~"%(readHTTPRoutesRegex)s",status_code=~"5.*"}[$__rate_interval]))
            or
            # Handle the case no failure has been tracked yet.
            vector(0)
        )
        /
        sum(rate(cortex_request_duration_seconds_count{%(gatewayMatcher)s, route=~"%(readHTTPRoutesRegex)s"}[$__rate_interval]))
      ||| % variables,
    },

    distributor: {
      writeRequestsPerSecond: 'cortex_request_duration_seconds_count{%(distributorMatcher)s, route=~"%(writeGRPCRoutesRegex)s|%(writeHTTPRoutesRegex)s"}' % variables,
      samplesPerSecond: 'sum(%(groupPrefixJobs)s:cortex_distributor_received_samples:rate5m{%(distributorMatcher)s})' % variables,
      exemplarsPerSecond: 'sum(%(groupPrefixJobs)s:cortex_distributor_received_exemplars:rate5m{%(distributorMatcher)s})' % variables,

      // Write failures rate as percentage of total requests.
      writeFailuresRate: |||
        (
            # gRPC errors are not tracked as 5xx but "error".
            sum(rate(cortex_request_duration_seconds_count{%(distributorMatcher)s, route=~"%(writeGRPCRoutesRegex)s|%(writeHTTPRoutesRegex)s",status_code=~"5.*|error"}[$__rate_interval]))
            or
            # Handle the case no failure has been tracked yet.
            vector(0)
        )
        /
        sum(rate(cortex_request_duration_seconds_count{%(distributorMatcher)s, route=~"%(writeGRPCRoutesRegex)s|%(writeHTTPRoutesRegex)s"}[$__rate_interval]))
      ||| % variables,
    },

    query_frontend: {
      readRequestsPerSecond: 'cortex_request_duration_seconds_count{%(queryFrontendMatcher)s, route=~"%(readHTTPRoutesRegex)s"}' % variables,
      instantQueriesPerSecond: 'sum(rate(cortex_request_duration_seconds_count{%(queryFrontendMatcher)s,route=~"(prometheus|api_prom)_api_v1_query"}[$__rate_interval]))' % variables,
      rangeQueriesPerSecond: 'sum(rate(cortex_request_duration_seconds_count{%(queryFrontendMatcher)s,route=~"(prometheus|api_prom)_api_v1_query_range"}[$__rate_interval]))' % variables,
      labelQueriesPerSecond: 'sum(rate(cortex_request_duration_seconds_count{%(queryFrontendMatcher)s,route=~"(prometheus|api_prom)_api_v1_label.*"}[$__rate_interval]))' % variables,
      seriesQueriesPerSecond: 'sum(rate(cortex_request_duration_seconds_count{%(queryFrontendMatcher)s,route=~"(prometheus|api_prom)_api_v1_series"}[$__rate_interval]))' % variables,
      remoteReadQueriesPerSecond: 'sum(rate(cortex_request_duration_seconds_count{%(queryFrontendMatcher)s,route=~"(prometheus|api_prom)_api_v1_read"}[$__rate_interval]))' % variables,
      metadataQueriesPerSecond: 'sum(rate(cortex_request_duration_seconds_count{%(queryFrontendMatcher)s,route=~"(prometheus|api_prom)_api_v1_metadata"}[$__rate_interval]))' % variables,
      exemplarsQueriesPerSecond: 'sum(rate(cortex_request_duration_seconds_count{%(queryFrontendMatcher)s,route=~"(prometheus|api_prom)_api_v1_query_exemplars"}[$__rate_interval]))' % variables,
      otherQueriesPerSecond: 'sum(rate(cortex_request_duration_seconds_count{%(queryFrontendMatcher)s,route=~"(prometheus|api_prom)_api_v1_.*",route!~".*(query|query_range|label.*|series|read|metadata|query_exemplars)"}[$__rate_interval]))' % variables,

      // Read failures rate as percentage of total requests.
      readFailuresRate: |||
        (
            sum(rate(cortex_request_duration_seconds_count{%(queryFrontendMatcher)s, route=~"%(readHTTPRoutesRegex)s",status_code=~"5.*"}[$__rate_interval]))
            or
            # Handle the case no failure has been tracked yet.
            vector(0)
        )
        /
        sum(rate(cortex_request_duration_seconds_count{%(queryFrontendMatcher)s, route=~"%(readHTTPRoutesRegex)s"}[$__rate_interval]))
      ||| % variables,
    },

    ruler: {
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
  },
}
