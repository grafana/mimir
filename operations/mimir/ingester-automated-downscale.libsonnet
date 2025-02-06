{
  // Allow ingesters to be safely downscaled using the "prepare-downscale" and "downscale-leader" features
  // of the rollout-operator. Only available when using multi-zone ingesters. This feature can be enabled
  // via the ingester_automated_downscale_enabled flag. When enabled, the number of replicas in zone B will
  // be scaled to match the number of replicas in zone A on a configurable delay. Correspondingly, zone C will
  // follow zone B on the same configurable delay. The default delay can be changed via
  // $._config.ingester_automated_downscale_min_time_between_zones. The number of replicas in zone A
  // is still controlled by $._config.multi_zone_ingester_replicas until a reasonable metric to scale it
  // automatically is determined and tested.

  _config+: {
    ingester_automated_downscale_enabled: false,

    // Allow to selectively enable it on a per-zone basis.
    ingester_automated_downscale_zone_a_enabled: $._config.ingester_automated_downscale_enabled,
    ingester_automated_downscale_zone_b_enabled: $._config.ingester_automated_downscale_enabled,
    ingester_automated_downscale_zone_c_enabled: $._config.ingester_automated_downscale_enabled,

    ingester_automated_downscale_min_time_between_zones: '12h',
  },

  // Utility used to override a field only if exists in super.
  local overrideSuperIfExists(name, override) = if !( name in super) || super[name] == null || super[name] == {} then null else
    super[name] + override,

  // Let the rollout-operator know that it must call the prepare-shutdown endpoint before
  // scaling down ingesters in this statefulset.
  local statefulSet = $.apps.v1.statefulSet,
  local prepareDownscaleLabelsAnnotations =
    statefulSet.mixin.metadata.withLabelsMixin({
      'grafana.com/prepare-downscale': 'true',
      'grafana.com/min-time-between-zones-downscale': $._config.ingester_automated_downscale_min_time_between_zones,
    }) +
    statefulSet.mixin.metadata.withAnnotationsMixin({
      'grafana.com/prepare-downscale-http-path': 'ingester/prepare-shutdown',
      'grafana.com/prepare-downscale-http-port': '%(server_http_port)s' % $._config,
    }),

  // Ingester prepare-downscale configuration
  ingester_zone_a_statefulset: overrideSuperIfExists(
    'ingester_zone_a_statefulset',
    if !$._config.ingester_automated_downscale_zone_a_enabled || !$._config.multi_zone_ingester_enabled then {} else
      prepareDownscaleLabelsAnnotations
  ),

  ingester_zone_b_statefulset: overrideSuperIfExists(
    'ingester_zone_b_statefulset',
    if !$._config.ingester_automated_downscale_zone_b_enabled || !$._config.multi_zone_ingester_enabled then {} else
      prepareDownscaleLabelsAnnotations +
      $.removeReplicasFromSpec +
      statefulSet.mixin.metadata.withAnnotationsMixin({
        'grafana.com/rollout-downscale-leader': 'ingester-zone-a',
      }),
  ),

  ingester_zone_c_statefulset: overrideSuperIfExists(
    'ingester_zone_c_statefulset',
    if !$._config.ingester_automated_downscale_zone_c_enabled || !$._config.multi_zone_ingester_enabled then {} else
      prepareDownscaleLabelsAnnotations +
      $.removeReplicasFromSpec +
      statefulSet.mixin.metadata.withAnnotationsMixin({
        'grafana.com/rollout-downscale-leader': 'ingester-zone-b',
      }),
  ),
}
