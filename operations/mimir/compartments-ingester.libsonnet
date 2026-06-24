{
  _config+:: {
    compartments_ingester_enabled: $._config.compartments_enabled,
    no_compartments_ingester_enabled: !self.compartments_ingester_enabled,

    // Compartment ingesters are autoscaled: min/max replicas apply per compartment, per zone.
    compartments_ingester_autoscaling_min_replicas_per_compartment_zone: 1,
    compartments_ingester_autoscaling_max_replicas_per_compartment_zone: 10,

    // The regex to extract the ingester partition identifier from a pod name.
    compartments_ingester_zpdb_partition_regex: '[a-z\\-]+-zone-[a-z]-rc-[0-9]+-([0-9]+)',

    // The above regex has a single capturing group, so the partition is always group 1.
    compartments_ingester_zpdb_partition_group: 1,
  },

  assert !$._config.compartments_ingester_enabled || $._config.multi_zone_ingester_enabled
         : 'compartments_ingester_enabled requires multi_zone_ingester_enabled',
  assert !$._config.compartments_ingester_enabled || $._config.multi_zone_ingester_multi_az_enabled
         : 'compartments_ingester_enabled requires multi_zone_ingester_multi_az_enabled',
  assert !$._config.compartments_ingester_enabled || $._config.ingest_storage_enabled
         : 'compartments_ingester_enabled requires ingest_storage_enabled',
  assert !$._config.compartments_ingester_enabled || $._config.ingest_storage_ingester_autoscaling_enabled
         : 'compartments_ingester_enabled requires ingest_storage_ingester_autoscaling_enabled',

  local statefulSet = $.apps.v1.statefulSet,
  local podAntiAffinity = statefulSet.mixin.spec.template.spec.affinity.podAntiAffinity,
  local podDisruptionBudget = $.policy.v1.podDisruptionBudget,

  local isEnabled = $._config.compartments_ingester_enabled,
  local numCompartments = $._config.compartments_read_count,
  local isNoCompartmentsEnabled = $._config.no_compartments_ingester_enabled,
  local isZoneAEnabled = $._config.multi_zone_ingester_enabled && std.length($._config.multi_zone_availability_zones) >= 1,
  local isZoneBEnabled = $._config.multi_zone_ingester_enabled && std.length($._config.multi_zone_availability_zones) >= 2,
  local isZoneCEnabled = $._config.multi_zone_ingester_enabled && std.length($._config.multi_zone_availability_zones) >= 3,

  newIngesterCompartmentContainer(zone, compartmentIdx, args, extraEnvVarMap={})::
    $.newIngesterZoneContainer(zone, args, extraEnvVarMap),

  newIngesterCompartmentStatefulSet(zone, compartmentIdx, container, nodeAffinityMatchers=[])::
    local name = 'ingester-zone-%s-rc-%d' % [zone, compartmentIdx];
    local compartmentIdxStr = std.toString(compartmentIdx);
    local rolloutGroup = 'ingester-rc-%d' % compartmentIdx;
    $.newIngesterZoneStatefulSet(zone, container, nodeAffinityMatchers) +
    statefulSet.mixin.metadata.withName(name) +
    statefulSet.mixin.spec.withServiceName(name) +
    statefulSet.mixin.metadata.withLabelsMixin({ name: name, 'mimir-rc': compartmentIdxStr, 'rollout-group': rolloutGroup }) +
    statefulSet.mixin.spec.template.metadata.withLabels({ name: name, 'rollout-group': rolloutGroup }) +
    statefulSet.mixin.spec.selector.withMatchLabels({ name: name, 'rollout-group': rolloutGroup }) +
    statefulSet.mixin.spec.withReplicas($._config.compartments_ingester_autoscaling_min_replicas_per_compartment_zone) +
    // Label identifying the read compartment — used by the read-path anti-affinity below.
    statefulSet.mixin.spec.template.metadata.withLabelsMixin({ 'mimir-rc': compartmentIdxStr }) +
    // Multi-compartment ingesters require multi-AZ too, so we can always configure the multi-zone toleration.
    statefulSet.spec.template.spec.withTolerationsMixin($.newMimirMultiZoneToleration()) +
    // Per-compartment node anti-affinity, replacing the base ingester rule. Keep different read
    // compartments off the same node so a node failure only affects one compartment, and keep different
    // zones of the same compartment off the same node. The latter is already enforced by per-AZ node
    // affinity today (compartments require multi-AZ), but we keep the rule as a safety net in case that
    // requirement is ever relaxed.
    { spec+: podAntiAffinity.withRequiredDuringSchedulingIgnoredDuringExecution([
      podAntiAffinity.requiredDuringSchedulingIgnoredDuringExecutionType.new() +
      podAntiAffinity.requiredDuringSchedulingIgnoredDuringExecutionType.mixin.labelSelector.withMatchExpressions([
        { key: 'mimir-rc', operator: 'Exists' },
        { key: 'mimir-rc', operator: 'NotIn', values: [compartmentIdxStr] },
      ]) +
      podAntiAffinity.requiredDuringSchedulingIgnoredDuringExecutionType.withTopologyKey('kubernetes.io/hostname'),
    ] + (
      if $._config.ingester_allow_multiple_replicas_on_same_node then [] else [
        podAntiAffinity.requiredDuringSchedulingIgnoredDuringExecutionType.new() +
        podAntiAffinity.requiredDuringSchedulingIgnoredDuringExecutionType.mixin.labelSelector.withMatchExpressions([
          { key: 'rollout-group', operator: 'In', values: [rolloutGroup] },
          { key: 'name', operator: 'NotIn', values: [name] },
        ]) +
        podAntiAffinity.requiredDuringSchedulingIgnoredDuringExecutionType.withTopologyKey('kubernetes.io/hostname'),
      ]
    )).spec },

  // Null out no-compartment StatefulSets, Services, PDBs, and ScaledObjects when decommissioning.
  ingester_zone_a_statefulset: if !isNoCompartmentsEnabled && isZoneAEnabled then null else super.ingester_zone_a_statefulset,
  ingester_zone_b_statefulset: if !isNoCompartmentsEnabled && isZoneBEnabled then null else super.ingester_zone_b_statefulset,
  ingester_zone_c_statefulset: if !isNoCompartmentsEnabled && isZoneCEnabled then null else super.ingester_zone_c_statefulset,

  ingester_zone_a_service: if !isNoCompartmentsEnabled && isZoneAEnabled then null else super.ingester_zone_a_service,
  ingester_zone_b_service: if !isNoCompartmentsEnabled && isZoneBEnabled then null else super.ingester_zone_b_service,
  ingester_zone_c_service: if !isNoCompartmentsEnabled && isZoneCEnabled then null else super.ingester_zone_c_service,

  ingester_rollout_pdb: if !isNoCompartmentsEnabled then null else super.ingester_rollout_pdb,

  ingester_primary_zone_replica_template: if !isNoCompartmentsEnabled then null else super.ingester_primary_zone_replica_template,
  ingest_storage_ingester_primary_zone_scaling: if !isNoCompartmentsEnabled then null else super.ingest_storage_ingester_primary_zone_scaling,

  // Args.
  local perCompartmentIngesterArgs(compartmentIdx) =
    $.mimirCompartmentsCommonArgs {
      'ingester.read-compartment-id': compartmentIdx,

      // The ingester consumes its own read compartment's topic, so its topic resolves the
      // '<read-compartment-id>' placeholder to the compartment id. The address keeps the write-compartment
      // placeholder because the ingester consumes from every write compartment's Kafka cluster.
      'ingest-storage.kafka.address': $.compartments_ingest_storage_kafka_address,
      'ingest-storage.kafka.topic': std.strReplace($._config.compartments_ingest_storage_kafka_topic, '<read-compartment-id>', std.toString(compartmentIdx)),
    },

  ingester_zone_a_compartments_args:: $.mimirCompartmentsCreateIf(isEnabled && isZoneAEnabled, numCompartments, function(compartment) $.ingester_zone_a_args + perCompartmentIngesterArgs(compartment)),
  ingester_zone_b_compartments_args:: $.mimirCompartmentsCreateIf(isEnabled && isZoneBEnabled, numCompartments, function(compartment) $.ingester_zone_b_args + perCompartmentIngesterArgs(compartment)),
  ingester_zone_c_compartments_args:: $.mimirCompartmentsCreateIf(isEnabled && isZoneCEnabled, numCompartments, function(compartment) $.ingester_zone_c_args + perCompartmentIngesterArgs(compartment)),

  // Containers.
  ingester_zone_a_containers:: $.mimirCompartmentsCreateIf(isEnabled && isZoneAEnabled, numCompartments, function(compartment) $.newIngesterCompartmentContainer('a', compartment, $.ingester_zone_a_compartments_args['compartment_%d' % compartment], $.ingester_zone_a_env_map)),
  ingester_zone_b_containers:: $.mimirCompartmentsCreateIf(isEnabled && isZoneBEnabled, numCompartments, function(compartment) $.newIngesterCompartmentContainer('b', compartment, $.ingester_zone_b_compartments_args['compartment_%d' % compartment], $.ingester_zone_b_env_map)),
  ingester_zone_c_containers:: $.mimirCompartmentsCreateIf(isEnabled && isZoneCEnabled, numCompartments, function(compartment) $.newIngesterCompartmentContainer('c', compartment, $.ingester_zone_c_compartments_args['compartment_%d' % compartment], $.ingester_zone_c_env_map)),

  // StatefulSets.
  ingester_zone_a_statefulsets: $.mimirCompartmentsCreateIf(isEnabled && isZoneAEnabled, numCompartments, function(compartment) $.newIngesterCompartmentStatefulSet('a', compartment, $.ingester_zone_a_containers['compartment_%d' % compartment], $.ingester_zone_a_node_affinity_matchers) + ingesterCompartmentAutoscalingStatefulSetMixin('a', compartment)),
  ingester_zone_b_statefulsets: $.mimirCompartmentsCreateIf(isEnabled && isZoneBEnabled, numCompartments, function(compartment) $.newIngesterCompartmentStatefulSet('b', compartment, $.ingester_zone_b_containers['compartment_%d' % compartment], $.ingester_zone_b_node_affinity_matchers) + ingesterCompartmentAutoscalingStatefulSetMixin('b', compartment)),
  ingester_zone_c_statefulsets: $.mimirCompartmentsCreateIf(isEnabled && isZoneCEnabled, numCompartments, function(compartment) $.newIngesterCompartmentStatefulSet('c', compartment, $.ingester_zone_c_containers['compartment_%d' % compartment], $.ingester_zone_c_node_affinity_matchers) + ingesterCompartmentAutoscalingStatefulSetMixin('c', compartment)),

  // PDBs.
  local newIngesterRolloutCompartmentPdb(compartmentIdx) =
    local name = 'ingester-rollout-rc-%d' % compartmentIdx;
    (
      if $._config.multi_zone_ingester_zpdb_enabled then
        $.newZPDB(
          name,
          'ingester',
          $._config.multi_zone_ingester_zpdb_max_unavailable,
          $._config.compartments_ingester_zpdb_partition_regex,
          $._config.compartments_ingester_zpdb_partition_group,
        )
      else
        podDisruptionBudget.new(name) +
        podDisruptionBudget.mixin.spec.withMaxUnavailable(1)
    )
    + podDisruptionBudget.mixin.metadata.withLabels({ name: name })
    + podDisruptionBudget.mixin.spec.selector.withMatchLabels({
      'rollout-group': 'ingester-rc-%d' % compartmentIdx,
      'mimir-rc': std.toString(compartmentIdx),
    }),

  ingester_rollout_pdbs: $.mimirCompartmentsCreateIf(isEnabled, numCompartments, function(compartment) newIngesterRolloutCompartmentPdb(compartment)),

  local autoscalingEnabled = isEnabled && $._config.ingest_storage_ingester_autoscaling_enabled,
  local autoscaledCompartments = if autoscalingEnabled then std.range(0, numCompartments - 1) else [],
  local compartmentLeaderName(compartmentIdx) = 'ingester-zone-a-rc-%d' % compartmentIdx,

  ingester_primary_zone_replica_templates: {
    ['compartment_%d' % compartment]: $.replicaTemplate(compartmentLeaderName(compartment), -1, 'name=%s' % compartmentLeaderName(compartment))
    for compartment in autoscaledCompartments
  },

  ingest_storage_ingester_primary_zone_scalings: {
    ['compartment_%d' % compartment]: $.newPartitionsPrimaryIngesterZoneScaledObject(
      compartmentLeaderName(compartment),
      $._config.compartments_ingester_autoscaling_min_replicas_per_compartment_zone,
      $._config.compartments_ingester_autoscaling_max_replicas_per_compartment_zone,
      // Per-compartment partition ring name: ingester.PartitionRingName ("ingester-partitions") with
      // the "-rc-<id>" suffix from compartments.ReadCompartmentRingName.
      [$.ingesterPartitionAutoscalingOwnedSeriesTrigger(compartmentLeaderName(compartment), 'ingester-partitions-rc-%d' % compartment)],
      $.ingester_primary_zone_replica_templates['compartment_%d' % compartment],
      $._config.ingest_storage_ingester_autoscaling_index_metrics,
    )
    for compartment in autoscaledCompartments
  },

  local ingesterCompartmentAutoscalingStatefulSetMixin(zone, compartmentIdx) =
    if !autoscalingEnabled then {} else
      $.ingesterPartitionAutoscalingStatefulSetMixin(compartmentLeaderName(compartmentIdx), zone == 'a', $.ingester_primary_zone_replica_templates['compartment_%d' % compartmentIdx]),

  // Governing headless services (required by Kubernetes for StatefulSet pod DNS).
  ingester_zone_a_services: $.mimirCompartmentsCreateIf(isEnabled && isZoneAEnabled, numCompartments, function(compartment) $.newIngesterZoneService($.ingester_zone_a_statefulsets['compartment_%d' % compartment])),
  ingester_zone_b_services: $.mimirCompartmentsCreateIf(isEnabled && isZoneBEnabled, numCompartments, function(compartment) $.newIngesterZoneService($.ingester_zone_b_statefulsets['compartment_%d' % compartment])),
  ingester_zone_c_services: $.mimirCompartmentsCreateIf(isEnabled && isZoneCEnabled, numCompartments, function(compartment) $.newIngesterZoneService($.ingester_zone_c_statefulsets['compartment_%d' % compartment])),

  // Config validation.
  local ingesterCompartmentMultiZoneError = $.validateMimirMultiZoneConfig([
    'ingester_zone_a_statefulsets',
    'ingester_zone_b_statefulsets',
    'ingester_zone_c_statefulsets',
  ]),
  assert ingesterCompartmentMultiZoneError == null : ingesterCompartmentMultiZoneError,

  local ingesterCompartmentConfigError = $.validateMimirCompartmentsConfig([
    'ingester_zone_a_statefulsets',
    'ingester_zone_b_statefulsets',
    'ingester_zone_c_statefulsets',
  ]),
  assert ingesterCompartmentConfigError == null : ingesterCompartmentConfigError,
}
