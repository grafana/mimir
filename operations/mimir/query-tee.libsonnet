{
  local container = $.core.v1.container,
  local containerPort = $.core.v1.containerPort,
  local deployment = $.apps.v1.deployment,
  local service = $.core.v1.service,
  local servicePort = $.core.v1.servicePort,

  query_tee_args:: {
    'backend.endpoints': std.join(',', $._config.query_tee_backend_endpoints),
    'backend.preferred': $._config.query_tee_backend_preferred,
  },

  query_tee_container:: if !($._config.query_tee_enabled) then {} else
    container.new('query-tee', $._images.query_tee) +
    container.withPorts([
      containerPort.newNamed(name='http', containerPort=80),
      containerPort.newNamed(name='http-metrics', containerPort=9900),
    ]) +
    container.withArgsMixin($.util.mapToFlags($.query_tee_args)) +
    $.util.resourcesRequests('1', '512Mi') +
    $.jaeger_mixin,

  query_tee_deployment: if !($._config.query_tee_enabled) then {} else
    deployment.new('query-tee', 2, [$.query_tee_container]),

  query_tee_service: if !($._config.query_tee_enabled) then {} else
    service.new('query-tee', { name: 'query-tee' }, [
      servicePort.newNamed('http', 80, 80) +
      servicePort.withNodePort($._config.query_tee_node_port),
    ]) +
    service.mixin.spec.withType('NodePort'),
}
