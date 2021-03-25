{
  local container = $.core.v1.container,
  local containerPort = $.core.v1.containerPort,

  test_exporter_args:: {
    'user-id': $._config.test_exporter_user_id,
    'prometheus-address': 'http://query-frontend.%(namespace)s.svc.cluster.local/prometheus' % $._config,
    'test-query-start': $._config.test_exporter_start_time,
    'extra-selectors': 'job="%(namespace)s/test-exporter"' % $._config,
    'test-query-min-size': '1m',
    'test-epsilion': '0.05',  // There is enough jitter in our system for scrapes to be off by 5%.
  },

  test_exporter_container::
    if !($._config.test_exporter_enabled)
    then {}
    else
      container.new('test-exporter', $._images.testExporter) +
      container.withPorts($.util.defaultPorts) +
      container.withArgsMixin($.util.mapToFlags($.test_exporter_args)) +
      $.util.resourcesRequests('100m', '100Mi') +
      $.util.resourcesLimits('100m', '100Mi') +
      $.jaeger_mixin,

  local deployment = $.apps.v1.deployment,

  test_exporter_deployment:
    if !($._config.test_exporter_enabled)
    then {}
    else
      deployment.new('test-exporter', 1, [
        $.test_exporter_container,
      ]),

  test_exporter_service:
    if !($._config.test_exporter_enabled)
    then {}
    else
      $.util.serviceFor($.test_exporter_deployment),
}
