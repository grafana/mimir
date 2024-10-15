{
  // Allow ingesters to be safely downscaled using the "prepare-downscale" and "resource-mirroring" features
  // of the rollout-operator. Only available when using multi-zone ingesters. This feature can be enabled
  // via the ingester_automated_downscale_v2_enabled flag.
  //
  // When enabled, ReplicaTemplate/ingester-zone-a object is installed into the namespace. This object holds desired
  // number of replicas for each ingester zone in spec.replicas field. (There is only one ReplicaTemplate object,
  // and all ingester zones use it.)
  //
  // Scaledown is initiated by decreasing spec.replicas field of ReplicaTemplate/ingester-zone-a object.
  // Rollout-operator will notice the change, set ingesters that are going to be downscaled (in all zones)
  // to read-only mode, and wait until ingesters were in read-only mode for pre-configured time
  // (ingester_automated_downscale_delay).
  //
  // After this time is reached, rollout-operator will finally adjust number of replicas in ingester StatefulSets
  // (for all zones).
  //
  // The number of replicas is still controlled by $._config.multi_zone_ingester_replicas until a reasonable metric to scale it
  // automatically is determined and tested.
  _config+: {
    ingester_automated_downscale_v2_enabled: false,

    // Number of ingester replicas per zone. All zones will have the same number of replicas.
    ingester_automated_downscale_v2_ingester_zones: 3,
    ingester_automated_downscale_v2_replicas_per_zone: std.ceil($._config.multi_zone_ingester_replicas / $._config.ingester_automated_downscale_v2_ingester_zones),

    // How long to wait before terminating an ingester after it has been notified about the scale down.
    ingester_automated_downscale_v2_delay: if 'querier.query-ingesters-within' in $.querier_args then
      $.querier_args['querier.query-ingesters-within']
    else
      // The default -querier.query-ingesters-within in Mimir is 13 hours.
      '13h',
  },

  // Validate the configuration.
  assert !$._config.ingester_automated_downscale_v2_enabled || $._config.multi_zone_ingester_enabled : 'ingester downscaling requires multi_zone_ingester_enabled in namespace %s' % $._config.namespace,
  assert !$._config.ingester_automated_downscale_v2_enabled || !$._config.ingester_automated_downscale_enabled : 'ingester_automated_downscale_enabled_v2 and ingester_automated_downscale_enabled are mutually exclusive in namespace %s' % $._config.namespace,

  // Utility used to override a field only if exists in super.
  local overrideSuperIfExists(name, override) = if !( name in super) || super[name] == null || super[name] == {} then null else
    super[name] + override,

  local updateIngesterZone(enabled, statefulsetName, primary=true) = overrideSuperIfExists(
    statefulsetName,
    local statefulSet = $.apps.v1.statefulSet;

    if !enabled then {} else (
      $.removeReplicasFromSpec +
      statefulSet.mixin.metadata.withLabelsMixin({
        // Let the rollout-operator know that it must call the prepare-shutdown endpoint before
        // scaling down ingesters in this statefulset.
        'grafana.com/prepare-downscale': 'true',
        // Zones are scaled down at the same time, with no delay between the zones.
        'grafana.com/min-time-between-zones-downscale': '0',
      }) +
      statefulSet.mixin.metadata.withAnnotationsMixin({
        // Endpoint for telling ingester that it's going to be scaled down. Ingester will flush the
        // data and unregister from the ring on shutdown.
        'grafana.com/prepare-downscale-http-path': 'ingester/prepare-shutdown',
        'grafana.com/prepare-downscale-http-port': '80',
        // Ingester statefulset will follow number of replicas from ReplicaTemplate/ingester-zone-a.
        'grafana.com/rollout-mirror-replicas-from-resource-api-version': 'rollout-operator.grafana.com/v1',
        'grafana.com/rollout-mirror-replicas-from-resource-kind': 'ReplicaTemplate',
        'grafana.com/rollout-mirror-replicas-from-resource-name': 'ingester-zone-a',
        'grafana.com/rollout-delayed-downscale': $._config.ingester_automated_downscale_v2_delay,
        'grafana.com/rollout-prepare-delayed-downscale-url': 'http://pod/ingester/prepare-instance-ring-downscale',
      } + (
        if !primary then {
          // Disable updates of ReplicaTemplate/ingester-zone-a from non-primary (zone-b, zone-c) statefulsets.
          'grafana.com/rollout-mirror-replicas-from-resource-write-back': 'false',
        }
        else {}
      ))
    )
  ),

  // Create resource that will be targetted by ScaledObject. A single ReplicaTemplate is used for all zones.
  // HPA requires that label selector exists and is valid, but it will not be used for target type of AverageValue.
  // In GKE however we see that selector is used to find pods and compute current usage, so we set it to target pods with given name.
  ingester_zone_a_replica_template:
    if !$._config.ingester_automated_downscale_v2_enabled then null
    else $.replicaTemplate('ingester-zone-a', $._config.ingester_automated_downscale_v2_replicas_per_zone, 'name=ingester-zone-a'),

  ingester_zone_a_statefulset: updateIngesterZone($._config.ingester_automated_downscale_v2_enabled, 'ingester_zone_a_statefulset', true),
  ingester_zone_b_statefulset: updateIngesterZone($._config.ingester_automated_downscale_v2_enabled, 'ingester_zone_b_statefulset', false),
  ingester_zone_c_statefulset: updateIngesterZone($._config.ingester_automated_downscale_v2_enabled, 'ingester_zone_c_statefulset', false),
}
