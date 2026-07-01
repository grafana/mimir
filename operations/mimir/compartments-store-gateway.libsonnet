{
  _config+:: {
    compartments_store_gateway_enabled: $._config.compartments_enabled,
    no_compartments_store_gateway_enabled: !self.compartments_store_gateway_enabled,

    // Per-compartment, per-zone autoscaling bounds (zones b/c follow the zone-a leader).
    autoscaling_store_gateway_min_replicas_per_compartment_zone: 3,
    autoscaling_store_gateway_max_replicas_per_compartment_zone: 10,
  },

  assert !$._config.compartments_store_gateway_enabled || $._config.compartments_compactor_enabled
         : 'compartments_store_gateway_enabled requires compartments_compactor_enabled (per-compartment blocks buckets)',
  assert !$._config.compartments_store_gateway_enabled || $._config.multi_zone_store_gateway_enabled
         : 'compartments_store_gateway_enabled requires multi_zone_store_gateway_enabled',
  assert !$._config.compartments_store_gateway_enabled || $._config.autoscaling_store_gateway_enabled
         : 'compartments_store_gateway_enabled requires autoscaling_store_gateway_enabled',

  local statefulSet = $.apps.v1.statefulSet,
  local podDisruptionBudget = $.policy.v1.podDisruptionBudget,
  local podAntiAffinity = statefulSet.mixin.spec.template.spec.affinity.podAntiAffinity,

  local isEnabled = $._config.compartments_store_gateway_enabled,
  local isNoCompartmentsEnabled = $._config.no_compartments_store_gateway_enabled,
  local numCompartments = $._config.compartments_read_count,

  local isZoneAEnabled = $._config.multi_zone_store_gateway_enabled,
  local isZoneBEnabled = $._config.multi_zone_store_gateway_enabled,
  local isZoneCEnabled = $._config.multi_zone_store_gateway_zone_c_enabled,
  local isZoneABackupEnabled = $._config.multi_zone_store_gateway_zone_a_backup_enabled,
  local isZoneBBackupEnabled = $._config.multi_zone_store_gateway_zone_b_backup_enabled,
  local isZoneAMultiAZ = $._config.multi_zone_store_gateway_zone_a_multi_az_enabled && std.length($._config.multi_zone_availability_zones) >= 1,
  local isZoneBMultiAZ = $._config.multi_zone_store_gateway_zone_b_multi_az_enabled && std.length($._config.multi_zone_availability_zones) >= 2,
  local isZoneCMultiAZ = $._config.multi_zone_store_gateway_zone_c_multi_az_enabled && std.length($._config.multi_zone_availability_zones) >= 3,
  local isZoneABackupMultiAZ = $._config.multi_zone_store_gateway_zone_a_backup_multi_az_enabled && std.length($._config.multi_zone_availability_zones) >= 1,
  local isZoneBBackupMultiAZ = $._config.multi_zone_store_gateway_zone_b_backup_multi_az_enabled && std.length($._config.multi_zone_availability_zones) >= 2,

  // Args.
  local perCompartmentStoreGatewayArgs(compartmentIdx) = {
    [$.mimirBlocksStorageBucketNameFlag]: $.mimirBlocksStorageCompartmentBucketName(compartmentIdx),

    'compartments.enabled': true,
    'compartments.read.num-compartments': $._config.compartments_read_count,
    'compartments.write.num-compartments': $._config.compartments_write_count,
    'store-gateway.read-compartment-id': compartmentIdx,

    // The store-gateway doesn't consume Kafka, but it inherits the ingest-storage args from the common config,
    // so set them to this read compartment's values for consistency (and so the compartments config
    // validation passes).
    'ingest-storage.kafka.address': $._config.compartments_ingest_storage_kafka_address,
    'ingest-storage.kafka.topic': $.mimirIngestStorageCompartmentKafkaTopic(compartmentIdx),
  },

  store_gateway_zone_a_compartments_args:: $.mimirCompartmentsCreateIf(isEnabled && isZoneAEnabled, numCompartments, function(c) $.store_gateway_zone_a_args + perCompartmentStoreGatewayArgs(c)),
  store_gateway_zone_b_compartments_args:: $.mimirCompartmentsCreateIf(isEnabled && isZoneBEnabled, numCompartments, function(c) $.store_gateway_zone_b_args + perCompartmentStoreGatewayArgs(c)),
  store_gateway_zone_c_compartments_args:: $.mimirCompartmentsCreateIf(isEnabled && isZoneCEnabled, numCompartments, function(c) $.store_gateway_zone_c_args + perCompartmentStoreGatewayArgs(c)),
  store_gateway_zone_a_backup_compartments_args:: $.mimirCompartmentsCreateIf(isEnabled && isZoneABackupEnabled, numCompartments, function(c) $.store_gateway_zone_a_backup_args + perCompartmentStoreGatewayArgs(c)),
  store_gateway_zone_b_backup_compartments_args:: $.mimirCompartmentsCreateIf(isEnabled && isZoneBBackupEnabled, numCompartments, function(c) $.store_gateway_zone_b_backup_args + perCompartmentStoreGatewayArgs(c)),

  // Prepare-downscale labels/annotations, so the rollout-operator calls the shutdown endpoint before scaling down.
  local prepareDownscaleLabelsAnnotations =
    statefulSet.mixin.metadata.withLabelsMixin({
      'grafana.com/prepare-downscale': 'true',
      'grafana.com/min-time-between-zones-downscale': $._config.store_gateway_automated_downscale_min_time_between_zones,
    }) +
    statefulSet.mixin.metadata.withAnnotationsMixin({
      'grafana.com/prepare-downscale-http-path': 'store-gateway/prepare-shutdown',
      'grafana.com/prepare-downscale-http-port': '80',
    }),

  // Zone-a is the autoscaled leader; zone-b follows its compartment's zone-a, zone-c follows its compartment's
  // zone-b. Backup zones follow their primary zone (a-backup→a, b-backup→b), matching
  // store-gateway-autoscaling.libsonnet: backups run on spot nodes and follow the standard-node leader so none
  // follows a (potentially missing) spot zone.
  local storeGatewayCompartmentDownscaleMixin(zone, compartmentIdx) =
    if zone == 'b' then statefulSet.mixin.metadata.withAnnotationsMixin({
      'grafana.com/rollout-downscale-leader': 'store-gateway-zone-a-rc-%d' % compartmentIdx,
      'grafana.com/rollout-upscale-only-when-leader-ready': 'true',
    })
    else if zone == 'c' then statefulSet.mixin.metadata.withAnnotationsMixin({
      'grafana.com/rollout-downscale-leader': 'store-gateway-zone-b-rc-%d' % compartmentIdx,
      'grafana.com/rollout-upscale-only-when-leader-ready': 'true',
    })
    else if zone == 'a-backup' then statefulSet.mixin.metadata.withAnnotationsMixin({
      'grafana.com/rollout-downscale-leader': 'store-gateway-zone-a-rc-%d' % compartmentIdx,
      'grafana.com/rollout-upscale-only-when-leader-ready': 'true',
    })
    else if zone == 'b-backup' then statefulSet.mixin.metadata.withAnnotationsMixin({
      'grafana.com/rollout-downscale-leader': 'store-gateway-zone-b-rc-%d' % compartmentIdx,
      'grafana.com/rollout-upscale-only-when-leader-ready': 'true',
    })
    else {},

  newStoreGatewayCompartmentStatefulSet(zone, compartmentIdx, container, nodeAffinityMatchers=[], multiAZ=false)::
    local name = 'store-gateway-zone-%s-rc-%d' % [zone, compartmentIdx];
    local compartmentIdxStr = std.toString(compartmentIdx);
    local rolloutGroup = 'store-gateway-rc-%d' % compartmentIdx;
    $.newStoreGatewayZoneStatefulSet(zone, container, nodeAffinityMatchers, rolloutGroup) +
    statefulSet.mixin.metadata.withName(name) +
    statefulSet.mixin.spec.withServiceName(name) +
    // Label identifying the read compartment.
    statefulSet.mixin.metadata.withLabelsMixin({ name: name, 'mimir-rc': compartmentIdxStr, 'rollout-group': rolloutGroup }) +
    // Mixin (not a replace) so the base pod labels set by newStoreGatewayZoneStatefulSet are kept; the
    // gossip-ring membership label is added by memberlist.libsonnet.
    statefulSet.mixin.spec.template.metadata.withLabelsMixin({ name: name, 'mimir-rc': compartmentIdxStr, 'rollout-group': rolloutGroup }) +
    statefulSet.mixin.spec.selector.withMatchLabels({ name: name, 'rollout-group': rolloutGroup }) +
    // Replicas are owned by the autoscaler.
    $.removeReplicasFromSpec +
    prepareDownscaleLabelsAnnotations +
    storeGatewayCompartmentDownscaleMixin(zone, compartmentIdx) +
    (if multiAZ then statefulSet.spec.template.spec.withTolerationsMixin($.newMimirMultiZoneToleration()) else {}) +
    // Replace the base cross-zone anti-affinity (baked with the non-compartment name): keep different zones of
    // the same compartment off the same node, while allowing same-zone replicas to share one.
    (if $._config.store_gateway_allow_multiple_replicas_on_same_node then {} else {
       spec+: podAntiAffinity.withRequiredDuringSchedulingIgnoredDuringExecution([
         podAntiAffinity.requiredDuringSchedulingIgnoredDuringExecutionType.new() +
         podAntiAffinity.requiredDuringSchedulingIgnoredDuringExecutionType.mixin.labelSelector.withMatchExpressions([
           { key: 'rollout-group', operator: 'In', values: [rolloutGroup] },
           { key: 'name', operator: 'NotIn', values: [name] },
         ]) +
         podAntiAffinity.requiredDuringSchedulingIgnoredDuringExecutionType.withTopologyKey('kubernetes.io/hostname'),
       ]).spec,
     }),

  // Containers.
  store_gateway_zone_a_containers:: $.mimirCompartmentsCreateIf(isEnabled && isZoneAEnabled, numCompartments, function(c) $.newStoreGatewayZoneContainer('a', $.store_gateway_zone_a_compartments_args['compartment_%d' % c], $.store_gateway_zone_a_env_map)),
  store_gateway_zone_b_containers:: $.mimirCompartmentsCreateIf(isEnabled && isZoneBEnabled, numCompartments, function(c) $.newStoreGatewayZoneContainer('b', $.store_gateway_zone_b_compartments_args['compartment_%d' % c], $.store_gateway_zone_b_env_map)),
  store_gateway_zone_c_containers:: $.mimirCompartmentsCreateIf(isEnabled && isZoneCEnabled, numCompartments, function(c) $.newStoreGatewayZoneContainer('c', $.store_gateway_zone_c_compartments_args['compartment_%d' % c], $.store_gateway_zone_c_env_map)),
  store_gateway_zone_a_backup_containers:: $.mimirCompartmentsCreateIf(isEnabled && isZoneABackupEnabled, numCompartments, function(c) $.newStoreGatewayZoneContainer('a-backup', $.store_gateway_zone_a_backup_compartments_args['compartment_%d' % c], $.store_gateway_zone_a_backup_env_map)),
  store_gateway_zone_b_backup_containers:: $.mimirCompartmentsCreateIf(isEnabled && isZoneBBackupEnabled, numCompartments, function(c) $.newStoreGatewayZoneContainer('b-backup', $.store_gateway_zone_b_backup_compartments_args['compartment_%d' % c], $.store_gateway_zone_b_backup_env_map)),

  // StatefulSets.
  store_gateway_zone_a_statefulsets: $.mimirCompartmentsCreateIf(isEnabled && isZoneAEnabled, numCompartments, function(c) $.newStoreGatewayCompartmentStatefulSet('a', c, $.store_gateway_zone_a_containers['compartment_%d' % c], $.store_gateway_zone_a_node_affinity_matchers, isZoneAMultiAZ)),
  store_gateway_zone_b_statefulsets: $.mimirCompartmentsCreateIf(isEnabled && isZoneBEnabled, numCompartments, function(c) $.newStoreGatewayCompartmentStatefulSet('b', c, $.store_gateway_zone_b_containers['compartment_%d' % c], $.store_gateway_zone_b_node_affinity_matchers, isZoneBMultiAZ)),
  store_gateway_zone_c_statefulsets: $.mimirCompartmentsCreateIf(isEnabled && isZoneCEnabled, numCompartments, function(c) $.newStoreGatewayCompartmentStatefulSet('c', c, $.store_gateway_zone_c_containers['compartment_%d' % c], $.store_gateway_zone_c_node_affinity_matchers, isZoneCMultiAZ)),
  store_gateway_zone_a_backup_statefulsets: $.mimirCompartmentsCreateIf(isEnabled && isZoneABackupEnabled, numCompartments, function(c) $.newStoreGatewayCompartmentStatefulSet('a-backup', c, $.store_gateway_zone_a_backup_containers['compartment_%d' % c], $.store_gateway_zone_a_backup_node_affinity_matchers, isZoneABackupMultiAZ)),
  store_gateway_zone_b_backup_statefulsets: $.mimirCompartmentsCreateIf(isEnabled && isZoneBBackupEnabled, numCompartments, function(c) $.newStoreGatewayCompartmentStatefulSet('b-backup', c, $.store_gateway_zone_b_backup_containers['compartment_%d' % c], $.store_gateway_zone_b_backup_node_affinity_matchers, isZoneBBackupMultiAZ)),

  // Services.
  store_gateway_zone_a_services: $.mimirCompartmentsCreateIf(isEnabled && isZoneAEnabled, numCompartments, function(c) $.newStoreGatewayZoneService($.store_gateway_zone_a_statefulsets['compartment_%d' % c])),
  store_gateway_zone_b_services: $.mimirCompartmentsCreateIf(isEnabled && isZoneBEnabled, numCompartments, function(c) $.newStoreGatewayZoneService($.store_gateway_zone_b_statefulsets['compartment_%d' % c])),
  store_gateway_zone_c_services: $.mimirCompartmentsCreateIf(isEnabled && isZoneCEnabled, numCompartments, function(c) $.newStoreGatewayZoneService($.store_gateway_zone_c_statefulsets['compartment_%d' % c])),
  store_gateway_zone_a_backup_services: $.mimirCompartmentsCreateIf(isEnabled && isZoneABackupEnabled, numCompartments, function(c) $.newStoreGatewayZoneService($.store_gateway_zone_a_backup_statefulsets['compartment_%d' % c])),
  store_gateway_zone_b_backup_services: $.mimirCompartmentsCreateIf(isEnabled && isZoneBBackupEnabled, numCompartments, function(c) $.newStoreGatewayZoneService($.store_gateway_zone_b_backup_statefulsets['compartment_%d' % c])),

  // PDBs.
  local newStoreGatewayRolloutCompartmentPdb(compartmentIdx) =
    local name = 'store-gateway-rollout-rc-%d' % compartmentIdx;
    local rolloutGroup = 'store-gateway-rc-%d' % compartmentIdx;
    (
      if $._config.multi_zone_store_gateway_zpdb_enabled then
        $.newZPDB(name, rolloutGroup, $._config.multi_zone_store_gateway_zpdb_max_unavailable)
      else
        podDisruptionBudget.new(name) +
        podDisruptionBudget.mixin.spec.withMaxUnavailable(1) +
        podDisruptionBudget.mixin.spec.selector.withMatchLabels({ 'rollout-group': rolloutGroup })
    )
    + podDisruptionBudget.mixin.metadata.withLabels({ name: name }),

  store_gateway_rollout_pdbs: $.mimirCompartmentsCreateIf(isEnabled, numCompartments, function(c) newStoreGatewayRolloutCompartmentPdb(c)),

  // Scaled objects.
  store_gateway_zone_a_scaled_objects: $.mimirCompartmentsCreateIf(isEnabled && isZoneAEnabled, numCompartments, function(c)
    $.newLeaderStoreGatewayScaledObject(
      'store-gateway-zone-a-rc-%d' % c,
      $._config.autoscaling_store_gateway_min_replicas_per_compartment_zone,
      $._config.autoscaling_store_gateway_max_replicas_per_compartment_zone,
      $._config.autoscaling_store_gateway_disk_usage_threshold,
      'store-gateway-data-store-gateway-zone-.*-rc-%d-.*' % c,
    )),

  // Null out the non-compartments store-gateway resources.
  store_gateway_zone_a_statefulset: if !isNoCompartmentsEnabled && isZoneAEnabled then null else super.store_gateway_zone_a_statefulset,
  store_gateway_zone_b_statefulset: if !isNoCompartmentsEnabled && isZoneBEnabled then null else super.store_gateway_zone_b_statefulset,
  store_gateway_zone_c_statefulset: if !isNoCompartmentsEnabled && isZoneCEnabled then null else super.store_gateway_zone_c_statefulset,
  store_gateway_zone_a_backup_statefulset: if !isNoCompartmentsEnabled && isZoneABackupEnabled then null else super.store_gateway_zone_a_backup_statefulset,
  store_gateway_zone_b_backup_statefulset: if !isNoCompartmentsEnabled && isZoneBBackupEnabled then null else super.store_gateway_zone_b_backup_statefulset,

  store_gateway_zone_a_service: if !isNoCompartmentsEnabled && isZoneAEnabled then null else super.store_gateway_zone_a_service,
  store_gateway_zone_b_service: if !isNoCompartmentsEnabled && isZoneBEnabled then null else super.store_gateway_zone_b_service,
  store_gateway_zone_c_service: if !isNoCompartmentsEnabled && isZoneCEnabled then null else super.store_gateway_zone_c_service,
  store_gateway_zone_a_backup_service: if !isNoCompartmentsEnabled && isZoneABackupEnabled then null else super.store_gateway_zone_a_backup_service,
  store_gateway_zone_b_backup_service: if !isNoCompartmentsEnabled && isZoneBBackupEnabled then null else super.store_gateway_zone_b_backup_service,

  store_gateway_multi_zone_service: if !isNoCompartmentsEnabled then null else super.store_gateway_multi_zone_service,
  store_gateway_rollout_pdb: if !isNoCompartmentsEnabled then null else super.store_gateway_rollout_pdb,
  // The non-compartment zone-a autoscaler's default PVC selector ("store-gateway-.*") also matches the
  // per-compartment "-rc-<idx>-" PVCs, so when the non-compartment store-gateways are also deployed it would
  // size off their disks too. Exclude those PVCs so it sizes off the non-compartment store-gateways only
  // (mirrors the "-partition" exclusion in ingester-autoscaling.libsonnet; the per-compartment autoscalers
  // are already scoped to their own PVCs).
  local scaleUp = $.keda.v1alpha1.scaledObject.spec.advanced.horizontalPodAutoscalerConfig.behavior.scaleUp,
  local inheritedStoreGatewayZoneAScaledObject = super.store_gateway_zone_a_scaled_object,
  store_gateway_zone_a_scaled_object:
    if !isNoCompartmentsEnabled then null
    else if !isEnabled then inheritedStoreGatewayZoneAScaledObject
    else $.newLeaderStoreGatewayScaledObject(
      'store-gateway-zone-a',
      $._config.autoscaling_store_gateway_min_replicas_per_zone,
      $._config.autoscaling_store_gateway_max_replicas_per_zone,
      $._config.autoscaling_store_gateway_disk_usage_threshold,
      extra_matchers='persistentvolumeclaim!~"store-gateway-data-store-gateway-zone-.*-rc-.*"',
      // Rebuilding via newLeaderStoreGatewayScaledObject resets the scale-up stabilization window to the OSS
      // default; re-apply the inherited one in case it was overridden.
    ) + scaleUp.withStabilizationWindowSeconds(
      inheritedStoreGatewayZoneAScaledObject.spec.advanced.horizontalPodAutoscalerConfig.behavior.scaleUp.stabilizationWindowSeconds
    ),

  // Config validation.
  local storeGatewayCompartmentZoneAError = if isZoneAMultiAZ then $.validateMimirMultiZoneConfig(['store_gateway_zone_a_statefulsets']) else null,
  assert storeGatewayCompartmentZoneAError == null : storeGatewayCompartmentZoneAError,

  local storeGatewayCompartmentZoneBError = if isZoneBMultiAZ then $.validateMimirMultiZoneConfig(['store_gateway_zone_b_statefulsets']) else null,
  assert storeGatewayCompartmentZoneBError == null : storeGatewayCompartmentZoneBError,

  local storeGatewayCompartmentZoneCError = if isZoneCMultiAZ then $.validateMimirMultiZoneConfig(['store_gateway_zone_c_statefulsets']) else null,
  assert storeGatewayCompartmentZoneCError == null : storeGatewayCompartmentZoneCError,

  local storeGatewayCompartmentZoneABackupError = if isZoneABackupMultiAZ then $.validateMimirMultiZoneConfig(['store_gateway_zone_a_backup_statefulsets']) else null,
  assert storeGatewayCompartmentZoneABackupError == null : storeGatewayCompartmentZoneABackupError,

  local storeGatewayCompartmentZoneBBackupError = if isZoneBBackupMultiAZ then $.validateMimirMultiZoneConfig(['store_gateway_zone_b_backup_statefulsets']) else null,
  assert storeGatewayCompartmentZoneBBackupError == null : storeGatewayCompartmentZoneBBackupError,

  local storeGatewayCompartmentConfigError = $.validateMimirCompartmentsConfig([
    'store_gateway_zone_a_statefulsets',
    'store_gateway_zone_b_statefulsets',
    'store_gateway_zone_c_statefulsets',
    'store_gateway_zone_a_backup_statefulsets',
    'store_gateway_zone_b_backup_statefulsets',
  ]),
  assert storeGatewayCompartmentConfigError == null : storeGatewayCompartmentConfigError,
}
