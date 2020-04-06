{
  namespace:
    $.core.v1.namespace.new($._config.namespace),

  util+:: {
    local containerPort = $.core.v1.containerPort,
    local container = $.core.v1.container,

    defaultPorts::
      [
        containerPort.newNamed(name='http-metrics', containerPort=80),
        containerPort.newNamed(name='grpc', containerPort=9095),
      ],

    readinessProbe::
      container.mixin.readinessProbe.httpGet.withPath('/ready') +
      container.mixin.readinessProbe.httpGet.withPort(80) +
      container.mixin.readinessProbe.withInitialDelaySeconds(15) +
      container.mixin.readinessProbe.withTimeoutSeconds(1),
  },
}
