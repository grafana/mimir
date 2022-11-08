// this enables overrides exporter, which will expose the configured
// overrides and presets (if configured). Those metrics can be potentially
// high cardinality.
{
  local name = 'overrides-exporter',

  _config+: {
    overrides_exporter_enabled: false,

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
  overrides_exporter_port:: containerPort.newNamed(name='http-metrics', containerPort=$._config.server_http_port),

  overrides_exporter_args:: {
    target: 'overrides-exporter',

    'server.http-listen-port': $._config.server_http_port,
  } + $._config.limitsConfig + $.mimirRuntimeConfigFile,

  local container = $.core.v1.container,
  overrides_exporter_container::
    container.new(name, $._images.overrides_exporter) +
    container.withPorts([
      $.overrides_exporter_port,
    ]) +
    container.withArgsMixin($.util.mapToFlags($.overrides_exporter_args)) +
    $.util.resourcesRequests('0.5', '0.5Gi') +
    $.util.readinessProbe +
    container.mixin.readinessProbe.httpGet.withPort($.overrides_exporter_port.name),

  local deployment = $.apps.v1.deployment,
  overrides_exporter_deployment: if !$._config.overrides_exporter_enabled then null else
    deployment.new(name, 1, [$.overrides_exporter_container], { name: name }) +
    $.mimirVolumeMounts +
    deployment.mixin.metadata.withLabels({ name: name }),

  overrides_exporter_service: if !$._config.overrides_exporter_enabled then null else
    $.util.serviceFor($.overrides_exporter_deployment, $._config.service_ignored_labels),
}
