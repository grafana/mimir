{
  local container = $.core.v1.container,
  local containerPort = $.core.v1.containerPort,

  _config+: {
    pgUser: 'cortex',
    pgPassword: '1234',
  },

  postgresql_container::
    container.new('postgres', $._images.postgresql) +
    container.withPorts([
      containerPort.newNamed('postgresql', 5432),
    ]) +
    container.withEnvMap({
      POSTGRES_USER: $._config.pgUser,
      POSTGRES_DB: 'configs',
    }) +
    $.util.resourcesRequests('2', '1Gi') +
    $.util.resourcesLimits('4', '2Gi'),

  local deployment = $.apps.v1beta1.deployment,
  postgresql_deployment:
    deployment.new('postgresql', 1, [$.postgresql_container]),

  local service = $.core.v1.service,
  postgresql_service:
    $.util.serviceFor($.postgresql_deployment),
}
