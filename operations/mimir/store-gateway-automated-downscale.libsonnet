{
  // Allow store-gateways to be safely downscaled using the "prepare-downscale" and "downscale-leader" features
  // of the rollout-operator. Only available when using multi-zone store-gateways. This feature can be enabled
  // via the store_gateway_automated_downscale_enabled flag. When enabled, the number of replicas in zone B will
  // be scaled to match the number of replicas in zone A on a configurable delay. Correspondingly, zone C will
  // follow zone B on the same configurable delay. The default delay can be changed via
  // $._config.store_gateway_automated_downscale_min_time_between_zones. The number of replicas in zone A
  // is still controlled by $._config.multi_zone_store_gateway_replicas until a reasonable metric to scale it
  // automatically is determined and tested.

  _config+: {
    store_gateway_automated_downscale_enabled: false,
    // Give more time if lazy-loading is disabled.
    store_gateway_automated_downscale_min_time_between_zones: if $._config.store_gateway_lazy_loading_enabled then '15m' else '60m',
  },

  // Utility used to override a field only if exists in super.
  local overrideSuperIfExists(name, override) = if !( name in super) || super[name] == null || super[name] == {} then null else
    super[name] + override,

  // Let the rollout-operator know that it must call the prepare-shutdown endpoint before
  // scaling down store-gateways in this statefulset.
  local statefulSet = $.apps.v1.statefulSet,
  local prepareDownscaleLabelsAnnotations =
    statefulSet.mixin.metadata.withLabelsMixin({
      'grafana.com/prepare-downscale': 'true',
      'grafana.com/min-time-between-zones-downscale': $._config.store_gateway_automated_downscale_min_time_between_zones,
    }) +
    statefulSet.mixin.metadata.withAnnotationsMixin({
      'grafana.com/prepare-downscale-http-path': 'store-gateway/prepare-shutdown',
      'grafana.com/prepare-downscale-http-port': '80',
    }),

  // Store-gateway prepare-downscale configuration
  store_gateway_zone_a_statefulset: overrideSuperIfExists(
    'store_gateway_zone_a_statefulset',
    if !$._config.store_gateway_automated_downscale_enabled || !$._config.multi_zone_store_gateway_enabled then {} else
      prepareDownscaleLabelsAnnotations
  ),

  store_gateway_zone_b_statefulset: overrideSuperIfExists(
    'store_gateway_zone_b_statefulset',
    if !$._config.store_gateway_automated_downscale_enabled || !$._config.multi_zone_store_gateway_enabled then {} else
      prepareDownscaleLabelsAnnotations +
      $.removeReplicasFromSpec +
      statefulSet.mixin.metadata.withAnnotationsMixin({
        'grafana.com/rollout-downscale-leader': 'store-gateway-zone-a',
      }),
  ),

  store_gateway_zone_c_statefulset: overrideSuperIfExists(
    'store_gateway_zone_c_statefulset',
    if !$._config.store_gateway_automated_downscale_enabled || !$._config.multi_zone_store_gateway_enabled then {} else
      prepareDownscaleLabelsAnnotations +
      $.removeReplicasFromSpec +
      statefulSet.mixin.metadata.withAnnotationsMixin({
        'grafana.com/rollout-downscale-leader': 'store-gateway-zone-b',
      }),
  ),

  store_gateway_args+:: if $._config.deployment_mode != 'microservices' || !$._config.store_gateway_automated_downscale_enabled || !$._config.multi_zone_store_gateway_enabled then {} else {
    // When prepare-downscale webhook is in use, we don't need the auto-forget feature to ensure
    // store-gateways are removed from the ring, because the shutdown endpoint (called by the
    // rollout-operator) will do it. For this reason, we disable the auto-forget which has the benefit
    // of not causing some store-gateways getting overwhelmed (due to increased owned blocks) when several
    // store-gateways in a zone are unhealthy for an extended period of time (e.g. when running 1 out of 3
    // zones on spot VMs).
    'store-gateway.sharding-ring.auto-forget-enabled': false,
  },
}
