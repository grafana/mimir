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

  local presets_enabled = std.length($._config.overrides_exporter_presets) > 0,

  local configMap = $.core.v1.configMap,
  overrides_exporter_presets_configmap:
    if presets_enabled then
      configMap.new('overrides-presets') +
      configMap.withData({
        'overrides-presets.yaml': $.util.manifestYaml(
          {
            presets: {
              [key]: $._config.overrides[key]
              for key in $._config.overrides_exporter_presets
            },
          }
        ),
      }),

  local containerPort = $.core.v1.containerPort,
  overrides_exporter_port:: containerPort.newNamed(name='http-metrics', containerPort=9683),

  overrides_exporter_args:: {
    'overrides-file': '/etc/cortex/overrides.yaml',
  } + if presets_enabled then {
    'presets-file': '/etc/cortex_presets/overrides-presets.yaml',
  } else {},

  local container = $.core.v1.container,
  overrides_exporter_container::
    container.new(name, $._images.cortex_tools) +
    container.withPorts([
      $.overrides_exporter_port,
    ]) +
    container.withArgsMixin([name] + $.util.mapToFlags($.overrides_exporter_args, prefix='--')) +
    $.util.resourcesRequests('0.5', '0.5Gi') +
    $.util.readinessProbe +
    container.mixin.readinessProbe.httpGet.withPort($.overrides_exporter_port.name),

  local deployment = $.apps.v1.deployment,
  overrides_exporter_deployment:
    deployment.new(name, 1, [$.overrides_exporter_container], { name: name }) +
    $.util.configVolumeMount($._config.overrides_configmap, '/etc/cortex') +
    $.util.configVolumeMount('overrides-presets', '/etc/cortex_presets') +
    deployment.mixin.metadata.withLabels({ name: name }),

  overrides_exporter_service:
    $.util.serviceFor($.overrides_exporter_deployment),
}
