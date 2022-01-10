// this enables overrides exporter, which will expose the configured
// overrides and presets (if configured). Those metrics can be potentially
// high cardinality.
{
  local name = 'overrides-exporter',

  _config+: {
    // overrides exporter can also make the configured presets available, this
    // list references entries within $._config.overrides

    overrides_exporter_presets:: [
      'extra_small_user',
      'small_user',
      'medium_user',
      'big_user',
      'super_user',
      'mega_user',
    ],
  },

  local containerPort = $.core.v1.containerPort,
  overrides_exporter_port:: containerPort.newNamed(name='http-metrics', containerPort=80),

  overrides_exporter_args:: {
    target: 'overrides-exporter',

    'runtime-config.file': '/etc/cortex/overrides.yaml',
  } + $._config.limitsConfig,

  local container = $.core.v1.container,
  overrides_exporter_container::
    container.new(name, $._images.cortex) +
    container.withPorts([
      $.overrides_exporter_port,
    ]) +
    container.withArgsMixin($.util.mapToFlags($.overrides_exporter_args)) +
    $.util.resourcesRequests('0.5', '0.5Gi') +
    $.util.readinessProbe +
    container.mixin.readinessProbe.httpGet.withPort($.overrides_exporter_port.name),

  local deployment = $.apps.v1.deployment,
  overrides_exporter_deployment:
    deployment.new(name, 1, [$.overrides_exporter_container], { name: name }) +
    $.util.configVolumeMount($._config.overrides_configmap, '/etc/cortex') +
    deployment.mixin.metadata.withLabels({ name: name }),

  overrides_exporter_service:
    $.util.serviceFor($.overrides_exporter_deployment),
}
