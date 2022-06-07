{
  namespace:
    $.core.v1.namespace.new($._config.namespace),

  util+:: {
    local containerPort = $.core.v1.containerPort,
    local container = $.core.v1.container,

    defaultPorts::
      [
        containerPort.newNamed(name='http-metrics', containerPort=$._config.server_http_port),
        containerPort.newNamed(name='grpc', containerPort=9095),
      ],

    readinessProbe::
      container.mixin.readinessProbe.httpGet.withPath('/ready') +
      container.mixin.readinessProbe.httpGet.withPort($._config.server_http_port) +
      container.mixin.readinessProbe.withInitialDelaySeconds(15) +
      container.mixin.readinessProbe.withTimeoutSeconds(1),
  },

  newDiscoveryService(name, deployment)::
    local service = $.core.v1.service;

    $.util.serviceFor(deployment, $._config.service_ignored_labels) +
    service.mixin.spec.withPublishNotReadyAddresses(true) +
    service.mixin.spec.withClusterIp('None') +
    service.mixin.metadata.withName(name),
}
