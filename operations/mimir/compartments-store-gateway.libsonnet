{
  _config+:: {
    compartments_store_gateway_enabled: $._config.compartments_enabled,
    no_compartments_store_gateway_enabled: !self.compartments_store_gateway_enabled,
  },

  assert !$._config.compartments_store_gateway_enabled || $._config.compartments_compactor_enabled
         : 'compartments_store_gateway_enabled requires compartments_compactor_enabled (per-compartment blocks buckets)',
  assert !$._config.compartments_store_gateway_enabled || $._config.multi_zone_store_gateway_enabled
         : 'compartments_store_gateway_enabled requires multi_zone_store_gateway_enabled',

  local statefulSet = $.apps.v1.statefulSet,
  local podDisruptionBudget = $.policy.v1.podDisruptionBudget,
  local podAntiAffinity = statefulSet.mixin.spec.template.spec.affinity.podAntiAffinity,

  local isEnabled = $._config.compartments_store_gateway_enabled,
  local isNoCompartmentsEnabled = $._config.no_compartments_store_gateway_enabled,
  local numCompartments = $._config.compartments_read_count,

  // Primary zones (a/b/c) plus the optional multi-AZ backup zones (a-backup/b-backup), used for the RF-4
  // topology [a, a-backup, b, b-backup] (zone-c disabled). Backups follow their primary zone, mirroring the
  // non-compartment multi-zone-store-gateway / store-gateway-autoscaling libs.
  local isZoneAEnabled = $._config.multi_zone_store_gateway_enabled,
  local isZoneBEnabled = $._config.multi_zone_store_gateway_enabled,
  local isZoneCEnabled = $._config.multi_zone_store_gateway_zone_c_enabled,
  local isZoneABackupEnabled = $._config.multi_zone_store_gateway_zone_a_backup_enabled,
  local isZoneBBackupEnabled = $._config.multi_zone_store_gateway_zone_b_backup_enabled,
  // Whether each zone runs multi-AZ, used to apply the multi-zone toleration and gate the zonal-address validation.
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
    // Backup zones default to 0 replicas; when autoscaled the rollout-operator scales them to follow their
    // leader (they run on spot nodes). Mirrors the non-compartment backup store-gateways.
    (if zone == 'a-backup' || zone == 'b-backup' then statefulSet.mixin.spec.withReplicas(0) else {}) +
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

  // StatefulSets. The per-compartment autoscaling (ScaledObjects, prepare-downscale, removeReplicasFromSpec) is
  // layered on top in store-gateway-autoscaling.libsonnet, mirroring the non-compartment store-gateway.
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
