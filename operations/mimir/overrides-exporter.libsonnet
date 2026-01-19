// this enables overrides exporter, which will expose the configured
// overrides and presets (if configured). Those metrics can be potentially
// high cardinality.
{
  local name = 'overrides-exporter',

  _config+: {
    overrides_exporter_enabled: false,
    overrides_exporter_ring_enabled: false,

    // overrides exporter can also make the configured presets available, this
    // list references entries within $._config.overrides
    overrides_exporter_presets:: [
      'extra_small_user',
      'small_user',
      'medium_user',
      'big_user',
      'super_user',
      'mega_user',
      'user_24M',
      'user_32M',
    ],

    // Configure the limits/ configuration options expored by the overrides-exporter.
    overrides_exporter_exported_limits: [
      'ingestion_rate',
      'ingestion_burst_size',
      'max_global_series_per_user',
      'max_global_series_per_metric',
      'max_global_exemplars_per_user',
      'max_fetched_chunks_per_query',
      'max_fetched_series_per_query',
      'max_fetched_chunk_bytes_per_query',
      'ruler_max_rules_per_rule_group',
      'ruler_max_rule_groups_per_tenant',
    ],
  },

  local containerPort = $.core.v1.containerPort,
  overrides_exporter_port:: containerPort.newNamed(name='http-metrics', containerPort=$._config.server_http_port),

  overrides_exporter_args::
    $._config.commonConfig +
    $._config.limitsConfig +
    $._config.queryConfig +
    $._config.overridesExporterRingConfig +
    $.mimirRuntimeConfigFile +
    {
      target: 'overrides-exporter',

      'server.http-listen-port': $._config.server_http_port,
      'overrides-exporter.enabled-metrics': std.join(',', std.sort($._config.overrides_exporter_exported_limits)),
    },

  overrides_exporter_container_env_map:: {},

  overrides_exporter_node_affinity_matchers:: [],

  local container = $.core.v1.container,
  overrides_exporter_container::
    container.new(name, $._images.overrides_exporter) +
    container.withPorts([
      $.overrides_exporter_port,
    ]) +
    container.withArgsMixin($.util.mapToFlags($.overrides_exporter_args)) +
    (if std.length($.overrides_exporter_container_env_map) > 0 then container.withEnvMap(std.prune($.overrides_exporter_container_env_map)) else {}) +
    $.util.resourcesRequests('0.5', '0.5Gi') +
    $.util.readinessProbe +
    container.mixin.readinessProbe.httpGet.withPort($.overrides_exporter_port.name),

  local deployment = $.apps.v1.deployment,
  overrides_exporter_deployment: if !$._config.overrides_exporter_enabled then null else
    deployment.new(name, 1, [$.overrides_exporter_container], { name: name }) +
    $.newMimirNodeAffinityMatchers($.overrides_exporter_node_affinity_matchers) +
    $.mimirVolumeMounts +
    deployment.mixin.metadata.withLabels({ name: name }) +
    deployment.mixin.spec.strategy.rollingUpdate.withMaxSurge('15%') +
    deployment.mixin.spec.strategy.rollingUpdate.withMaxUnavailable(0),

  overrides_exporter_service: if !$._config.overrides_exporter_enabled then null else
    $.util.serviceFor($.overrides_exporter_deployment, $._config.service_ignored_labels),

  overrides_exporter_pdb: if !$._config.overrides_exporter_enabled then null else
    $.newMimirPdb(name),
}
