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
    // Allow to selectively enable it on a per-zone basis.
    store_gateway_automated_downscale_zone_a_enabled: $._config.store_gateway_automated_downscale_enabled,
    store_gateway_automated_downscale_zone_b_enabled: $._config.store_gateway_automated_downscale_enabled,
    store_gateway_automated_downscale_zone_c_enabled: $._config.store_gateway_automated_downscale_enabled,
    store_gateway_automated_downscale_zone_a_backup_enabled: $._config.store_gateway_automated_downscale_enabled,
    store_gateway_automated_downscale_zone_b_backup_enabled: $._config.store_gateway_automated_downscale_enabled,

    autoscaling_store_gateway_enabled: false,
    // With a tolerance of +/- 10%, 77% disk usage will trigger scaling when the worst case pod hits 85% disk usage.
    autoscaling_store_gateway_disk_usage_threshold: 77,
    autoscaling_store_gateway_min_replicas_per_zone: error 'you must set autoscaling_store_gateway_min_replicas_per_zone in the _config',
    autoscaling_store_gateway_max_replicas_per_zone: error 'you must set autoscaling_store_gateway_max_replicas_per_zone in the _config',

    // Per-compartment, per-zone autoscaling bounds.
    autoscaling_store_gateway_min_replicas_per_compartment_zone: 3,
    autoscaling_store_gateway_max_replicas_per_compartment_zone: 10,

    // Give more time if lazy-loading is disabled.
    store_gateway_automated_downscale_min_time_between_zones: if $._config.store_gateway_lazy_loading_enabled then '15m' else '60m',
  },

  local keda = $.keda.v1alpha1,
  local scaledObject = keda.scaledObject,
  local scaleDown = scaledObject.spec.advanced.horizontalPodAutoscalerConfig.behavior.scaleDown,
  local scaleDownPolicies = scaleDown.policies,
  local scaleUp = scaledObject.spec.advanced.horizontalPodAutoscalerConfig.behavior.scaleUp,
  local scaleUpPolicies = scaleUp.policies,

  // Create a KEDA scaled object for the leader zone (zone A). The replica count for other
  // zones will follow zone A using the rollout-operator.
  newLeaderStoreGatewayScaledObject(name_and_zone, min_replicas, max_replicas, usage_threshold, pvc_name_pattern=null, extra_matchers='')::
    local pat = if pvc_name_pattern != null then pvc_name_pattern else 'store-gateway-.*';
    local qry = '(1 - (min(kubelet_volume_stats_available_bytes{namespace="%(namespace)s", persistentvolumeclaim=~"%(pvc_name_pattern)s"%(extra_matchers)s}/kubelet_volume_stats_capacity_bytes{namespace="%(namespace)s", persistentvolumeclaim=~"%(pvc_name_pattern)s"%(extra_matchers)s}))) * 100';

    self.newScaledObject(name_and_zone, $._config.namespace, {
      min_replica_count: min_replicas,
      max_replica_count: max_replicas,
      // see https://keda.sh/docs/2.9/concepts/scaling-deployments/#triggers
      triggers: [
        {
          metric_name: 'mimir_%s_replicas_hpa_%s' % [std.strReplace(name_and_zone, '-', '_'), std.strReplace($._config.namespace, '-', '_')],
          query: qry % {
            namespace: $._config.namespace,
            extra_matchers: if extra_matchers == '' then '' else ',%s' % extra_matchers,
            pvc_name_pattern: pat,
          },
          metric_type: 'Value',
          threshold: std.toString(usage_threshold),
          // Disable ignoring null values. This allows HPAs to effectively pause when metrics are unavailable rather than scaling
          // up or down unexpectedly. See https://keda.sh/docs/2.13/scalers/prometheus/ for more info.
          ignore_null_values: false,
        },
      ],
    }, kind='StatefulSet') +

    // Allow 50% increase of pods - this larger percentage is due to replica counts being divided by zones
    // check every 10 minutes with a cooldown of 1h after scaling
    scaleUp.withPolicies(
      scaleUpPolicies.withType('Percent') +
      scaleUpPolicies.withValue(50) +
      scaleUpPolicies.withPeriodSeconds(600)
    ) + scaleUp.withStabilizationWindowSeconds(1800) +  // long stabilization period to reduce risk of flapping

    // Allow 1 pod per leader zone downscaling of pods
    // check every 10 minutes with a 1h cooldown after scaling
    scaleDown.withPolicies(
      scaleDownPolicies.withType('Pods') +
      scaleDownPolicies.withValue(1) +
      scaleDownPolicies.withPeriodSeconds(600)
    ) + scaleDown.withStabilizationWindowSeconds(3600),  // long stabilization period to reduce risk of flapping

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

  // Downscale-leader annotations telling the rollout-operator which zone each follower tracks.
  // compartmentIdx is null for the non-compartment store-gateways and the read compartment index
  // for the per-compartment ones.
  local storeGatewayDownscaleLeaderMixin(zone, compartmentIdx=null) =
    local suffix = if compartmentIdx == null then '' else '-rc-%d' % compartmentIdx;
    // - zone-b follows zone-a
    // - zone-c follows zone-b
    // - backup zones follow their primary zone (a-backup→a, b-backup→b), since they
    //   run on spot nodes and must follow the standard-node leader (so none follows a
    //   potentially-missing spot zone)
    // - zone-a is the leader and gets no annotation
    local leader =
      if zone == 'b' || zone == 'a-backup' then 'store-gateway-zone-a' + suffix
      else if zone == 'c' || zone == 'b-backup' then 'store-gateway-zone-b' + suffix
      else null;
    if leader == null then {} else statefulSet.mixin.metadata.withAnnotationsMixin({
      'grafana.com/rollout-downscale-leader': leader,
      'grafana.com/rollout-upscale-only-when-leader-ready': 'true',
    }),

  store_gateway_zone_a_scaled_object:
    if $._config.compartments_store_gateway_enabled && !$._config.no_compartments_store_gateway_enabled
    // When the per-compartment store-gateways own the read path and the non-compartment ones are removed,
    // there's no non-compartment zone-a to autoscale.
    then null
    else if !$._config.autoscaling_store_gateway_enabled || !$._config.multi_zone_store_gateway_enabled || !$._config.store_gateway_automated_downscale_zone_a_enabled
    then null
    else $.newLeaderStoreGatewayScaledObject(
      'store-gateway-zone-a',
      $._config.autoscaling_store_gateway_min_replicas_per_zone,
      $._config.autoscaling_store_gateway_max_replicas_per_zone,
      $._config.autoscaling_store_gateway_disk_usage_threshold,
      null,
      // In mixed mode the per-compartment store-gateways share the "store-gateway-.*" PVC prefix, so exclude
      // their "-rc-<idx>-" PVCs to size the non-compartment autoscaler off the non-compartment disks only.
      extra_matchers=if $._config.compartments_store_gateway_enabled then 'persistentvolumeclaim!~"store-gateway-data-store-gateway-zone-.*-rc-.*"' else '',
    ),

  // Store-gateway prepare-downscale configuration
  store_gateway_zone_a_statefulset: overrideSuperIfExists(
    'store_gateway_zone_a_statefulset',
    if !$._config.store_gateway_automated_downscale_zone_a_enabled || !$._config.multi_zone_store_gateway_enabled then {} else
      prepareDownscaleLabelsAnnotations + (if !$._config.autoscaling_store_gateway_enabled then {} else $.removeReplicasFromSpec)
  ),

  store_gateway_zone_b_statefulset: overrideSuperIfExists(
    'store_gateway_zone_b_statefulset',
    if !$._config.store_gateway_automated_downscale_zone_b_enabled || !$._config.multi_zone_store_gateway_enabled then {} else
      prepareDownscaleLabelsAnnotations +
      $.removeReplicasFromSpec +
      storeGatewayDownscaleLeaderMixin('b'),
  ),

  store_gateway_zone_c_statefulset: overrideSuperIfExists(
    'store_gateway_zone_c_statefulset',
    if !$._config.store_gateway_automated_downscale_zone_c_enabled || !$._config.multi_zone_store_gateway_enabled then {} else
      prepareDownscaleLabelsAnnotations +
      $.removeReplicasFromSpec +
      storeGatewayDownscaleLeaderMixin('c'),
  ),

  store_gateway_zone_a_backup_statefulset: overrideSuperIfExists(
    'store_gateway_zone_a_backup_statefulset',
    if !$._config.store_gateway_automated_downscale_zone_a_backup_enabled || !$._config.multi_zone_store_gateway_enabled then {} else
      prepareDownscaleLabelsAnnotations +
      $.removeReplicasFromSpec +
      storeGatewayDownscaleLeaderMixin('a-backup')
  ),

  store_gateway_zone_b_backup_statefulset: overrideSuperIfExists(
    'store_gateway_zone_b_backup_statefulset',
    if !$._config.store_gateway_automated_downscale_zone_b_backup_enabled || !$._config.multi_zone_store_gateway_enabled then {} else
      prepareDownscaleLabelsAnnotations +
      $.removeReplicasFromSpec +
      storeGatewayDownscaleLeaderMixin('b-backup'),
  ),

  // When prepare-downscale webhook is in use, we don't need the auto-forget feature to ensure
  // store-gateways are removed from the ring, because the shutdown endpoint (called by the
  // rollout-operator) will do it. For this reason, we disable the auto-forget which has the benefit
  // of not causing some store-gateways getting overwhelmed (due to increased owned blocks) when several
  // store-gateways in a zone are unhealthy for an extended period of time (e.g. when running 1 out of 3
  // zones on spot VMs).
  store_gateway_zone_a_args+:: if !$._config.store_gateway_automated_downscale_zone_a_enabled || !$._config.multi_zone_store_gateway_enabled then {} else {
    'store-gateway.sharding-ring.auto-forget-enabled': false,
  },
  store_gateway_zone_b_args+:: if !$._config.store_gateway_automated_downscale_zone_b_enabled || !$._config.multi_zone_store_gateway_enabled then {} else {
    'store-gateway.sharding-ring.auto-forget-enabled': false,
  },
  store_gateway_zone_c_args+:: if !$._config.store_gateway_automated_downscale_zone_c_enabled || !$._config.multi_zone_store_gateway_enabled then {} else {
    'store-gateway.sharding-ring.auto-forget-enabled': false,
  },
  store_gateway_zone_a_backup_args+:: if !$._config.store_gateway_automated_downscale_zone_a_backup_enabled || !$._config.multi_zone_store_gateway_enabled then {} else {
    'store-gateway.sharding-ring.auto-forget-enabled': false,
  },
  store_gateway_zone_b_backup_args+:: if !$._config.store_gateway_automated_downscale_zone_b_backup_enabled || !$._config.multi_zone_store_gateway_enabled then {} else {
    'store-gateway.sharding-ring.auto-forget-enabled': false,
  },

  //
  // Per-compartment store-gateway autoscaling. Mirrors the non-compartment autoscaling above, layered on top of
  // the per-compartment StatefulSet maps built in compartments-store-gateway.libsonnet. Gated on
  // autoscaling_store_gateway_enabled; when disabled the per-compartment store-gateways run fixed replicas.
  //

  local compartmentsStoreGatewayAutoscalingEnabled = $._config.compartments_store_gateway_enabled && $._config.autoscaling_store_gateway_enabled,

  // Applied to every per-compartment StatefulSet: the autoscaler owns the replicas and the rollout-operator
  // drives safe downscales via the prepare-shutdown endpoint (zones follow their leader per compartment).
  local storeGatewayCompartmentAutoscaleMixin(zone) = function(compartmentIdx)
    $.removeReplicasFromSpec +
    prepareDownscaleLabelsAnnotations +
    storeGatewayDownscaleLeaderMixin(zone, compartmentIdx),

  // Per-compartment store-gateways always use prepare-downscale when autoscaled (the shutdown endpoint removes
  // them from the ring), so disable auto-forget.
  local storeGatewayCompartmentAutoForgetArgs = if !compartmentsStoreGatewayAutoscalingEnabled then {} else {
    'store-gateway.sharding-ring.auto-forget-enabled': false,
  },

  store_gateway_zone_a_compartments_args+:: $.mimirCompartmentsOverrides(super.store_gateway_zone_a_compartments_args, storeGatewayCompartmentAutoForgetArgs),
  store_gateway_zone_b_compartments_args+:: $.mimirCompartmentsOverrides(super.store_gateway_zone_b_compartments_args, storeGatewayCompartmentAutoForgetArgs),
  store_gateway_zone_c_compartments_args+:: $.mimirCompartmentsOverrides(super.store_gateway_zone_c_compartments_args, storeGatewayCompartmentAutoForgetArgs),
  store_gateway_zone_a_backup_compartments_args+:: $.mimirCompartmentsOverrides(super.store_gateway_zone_a_backup_compartments_args, storeGatewayCompartmentAutoForgetArgs),
  store_gateway_zone_b_backup_compartments_args+:: $.mimirCompartmentsOverrides(super.store_gateway_zone_b_backup_compartments_args, storeGatewayCompartmentAutoForgetArgs),

  store_gateway_zone_a_statefulsets+: if !compartmentsStoreGatewayAutoscalingEnabled then {} else $.mimirCompartmentsOverrides(super.store_gateway_zone_a_statefulsets, storeGatewayCompartmentAutoscaleMixin('a')),
  store_gateway_zone_b_statefulsets+: if !compartmentsStoreGatewayAutoscalingEnabled then {} else $.mimirCompartmentsOverrides(super.store_gateway_zone_b_statefulsets, storeGatewayCompartmentAutoscaleMixin('b')),
  store_gateway_zone_c_statefulsets+: if !compartmentsStoreGatewayAutoscalingEnabled then {} else $.mimirCompartmentsOverrides(super.store_gateway_zone_c_statefulsets, storeGatewayCompartmentAutoscaleMixin('c')),
  store_gateway_zone_a_backup_statefulsets+: if !compartmentsStoreGatewayAutoscalingEnabled then {} else $.mimirCompartmentsOverrides(super.store_gateway_zone_a_backup_statefulsets, storeGatewayCompartmentAutoscaleMixin('a-backup')),
  store_gateway_zone_b_backup_statefulsets+: if !compartmentsStoreGatewayAutoscalingEnabled then {} else $.mimirCompartmentsOverrides(super.store_gateway_zone_b_backup_statefulsets, storeGatewayCompartmentAutoscaleMixin('b-backup')),

  // Zone-a leader ScaledObject per compartment; the other zones follow it via the downscale-leader annotations.
  store_gateway_zone_a_scaled_objects: $.mimirCompartmentsCreateIf(compartmentsStoreGatewayAutoscalingEnabled, $._config.compartments_read_count, function(c)
    $.newLeaderStoreGatewayScaledObject(
      'store-gateway-zone-a-rc-%d' % c,
      $._config.autoscaling_store_gateway_min_replicas_per_compartment_zone,
      $._config.autoscaling_store_gateway_max_replicas_per_compartment_zone,
      $._config.autoscaling_store_gateway_disk_usage_threshold,
      'store-gateway-data-store-gateway-zone-.*-rc-%d-.*' % c,
    )),
}
