{
  // This object contains common queries used in the Mimir dashboards.
  // These queries are NOT intended to be configurable or overriddeable via jsonnet,
  // but they're defined in a common place just to share them between different dashboards.
  queries:: {
    write_http_routes_regex: 'api_(v1|prom)_push|otlp_v1_metrics',
    read_http_routes_regex: '(prometheus|api_prom)_api_v1_.+',
  },
}
