{
  // This object contains common queries used in the Mimir dashboards.
  // These queries are NOT intended to be configurable or overriddeable via jsonnet,
  // but they're defined in a common place just to share them between different dashboards.
  queries:: {
    write_http_routes_regex: 'api_(v1|prom)_push|otlp_v1_metrics',
    read_http_routes_regex: '(prometheus|api_prom)_api_v1_.+',

    gateway: {
      writeRequestsPerSecond: 'cortex_request_duration_seconds_count{%s, route=~"%s"}' % [$.jobMatcher($._config.job_names.gateway), $.queries.write_http_routes_regex],
      readRequestsPerSecond: 'cortex_request_duration_seconds_count{%s, route=~"%s"}' % [$.jobMatcher($._config.job_names.gateway), $.queries.read_http_routes_regex],
    },

    distributor: {
      writeRequestsPerSecond: 'cortex_request_duration_seconds_count{%s, route=~"/distributor.Distributor/Push|/httpgrpc.*|%s"}' % [$.jobMatcher($._config.job_names.distributor), $.queries.write_http_routes_regex],
      samplesPerSecond: 'sum(%(group_prefix_jobs)s:cortex_distributor_received_samples:rate5m{%(job)s})' % (
        $._config {
          job: $.jobMatcher($._config.job_names.distributor),
        }
      ),
      exemplarsPerSecond: 'sum(%(group_prefix_jobs)s:cortex_distributor_received_exemplars:rate5m{%(job)s})' % (
        $._config {
          job: $.jobMatcher($._config.job_names.distributor),
        }
      ),
    },

    query_frontend: {
      readRequestsPerSecond: 'cortex_request_duration_seconds_count{%s, route=~"%s"}' % [$.jobMatcher($._config.job_names.query_frontend), $.queries.read_http_routes_regex],
      instantQueriesPerSecond: 'sum(rate(cortex_request_duration_seconds_count{%s,route=~"(prometheus|api_prom)_api_v1_query"}[$__rate_interval]))' % $.jobMatcher($._config.job_names.query_frontend),
      rangeQueriesPerSecond: 'sum(rate(cortex_request_duration_seconds_count{%s,route=~"(prometheus|api_prom)_api_v1_query_range"}[$__rate_interval]))' % $.jobMatcher($._config.job_names.query_frontend),
      labelQueriesPerSecond: 'sum(rate(cortex_request_duration_seconds_count{%s,route=~"(prometheus|api_prom)_api_v1_label.*"}[$__rate_interval]))' % $.jobMatcher($._config.job_names.query_frontend),
      seriesQueriesPerSecond: 'sum(rate(cortex_request_duration_seconds_count{%s,route=~"(prometheus|api_prom)_api_v1_series"}[$__rate_interval]))' % $.jobMatcher($._config.job_names.query_frontend),
      otherQueriesPerSecond: 'sum(rate(cortex_request_duration_seconds_count{%s,route=~"(prometheus|api_prom)_api_v1_.*",route!~".*(query|query_range|label.*|series)"}[$__rate_interval]))' % $.jobMatcher($._config.job_names.query_frontend),
    },

    ruler: {
      evaluations: {
        successPerSecond:
          |||
            sum(rate(cortex_prometheus_rule_evaluations_total{%s}[$__rate_interval]))
            -
            sum(rate(cortex_prometheus_rule_evaluation_failures_total{%s}[$__rate_interval]))
          ||| % [$.jobMatcher($._config.job_names.ruler), $.jobMatcher($._config.job_names.ruler)],
        failurePerSecond: 'sum(rate(cortex_prometheus_rule_evaluation_failures_total{%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.ruler),
        missedIterationsPerSecond: 'sum(rate(cortex_prometheus_rule_group_iterations_missed_total{%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.ruler),
        latency:
          |||
            sum (rate(cortex_prometheus_rule_evaluation_duration_seconds_sum{%s}[$__rate_interval]))
              /
            sum (rate(cortex_prometheus_rule_evaluation_duration_seconds_count{%s}[$__rate_interval]))
          ||| % [$.jobMatcher($._config.job_names.ruler), $.jobMatcher($._config.job_names.ruler)],
      },
    },
  },
}
