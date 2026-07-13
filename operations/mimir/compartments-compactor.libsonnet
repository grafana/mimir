{
  _config+:: {
    compartments_compactor_enabled: $._config.compartments_enabled,
    no_compartments_compactor_enabled: !self.compartments_compactor_enabled,

    // Per-compartment compactor autoscaling bounds (per read compartment).
    autoscaling_compactor_min_replicas_per_compartment: 1,
    autoscaling_compactor_max_replicas_per_compartment: 10,
  },

  assert !$._config.compartments_compactor_enabled || $._config.compactor_scheduler_enabled
         : 'compartments_compactor_enabled requires compactor_scheduler_enabled',
  assert !$._config.compartments_compactor_enabled || $._config.autoscaling_compactor_enabled
         : 'compartments_compactor_enabled requires autoscaling_compactor_enabled',
  assert !$._config.compartments_compactor_enabled || $._config.cortex_compactor_concurrent_rollout_enabled
         : 'compartments_compactor_enabled requires cortex_compactor_concurrent_rollout_enabled',

  local container = $.core.v1.container,

  local isEnabled = $._config.compartments_compactor_enabled,
  local numCompartments = $._config.compartments_read_count,
  local isNoCompartmentsEnabled = $._config.no_compartments_compactor_enabled,

  local compartmentBlocksBucketArg(compartmentIdx) = {
    [$.mimirBlocksStorageBucketNameFlag]: $.mimirBlocksStorageCompartmentBucketName(compartmentIdx),
  },

  // Args.
  compactor_compartments_args:: $.mimirCompartmentsCreateIf(isEnabled, numCompartments, function(compartmentIdx)
    $.compactor_args + compartmentBlocksBucketArg(compartmentIdx) {
      'compartments.enabled': true,
      'compartments.read.num-compartments': $._config.compartments_read_count,
      'compartments.write.num-compartments': $._config.compartments_write_count,
      'compactor.read-compartment-id': compartmentIdx,

      // The compactor doesn't consume Kafka, but it inherits the ingest-storage args from the common config,
      // so set them to this read compartment's values for consistency (and so the compartments config
      // validation passes).
      'ingest-storage.kafka.address': $._config.compartments_ingest_storage_kafka_address,
      'ingest-storage.kafka.topic': $.mimirIngestStorageCompartmentKafkaTopic(compartmentIdx),
    }),

  // Containers.
  newCompactorCompartmentContainer(compartmentIdx)::
    $.compactor_container +
    container.withArgs($.util.mapToFlags($.compactor_compartments_args['compartment_%d' % compartmentIdx])),

  local compartmentCompactorMaxUnavailable = std.max(std.floor($._config.autoscaling_compactor_min_replicas_per_compartment / 2), 1),

  newCompactorCompartmentStatefulSet(compartmentIdx)::
    $.newCompactorStatefulSet(
      'compactor-rc-%d' % compartmentIdx,
      $.newCompactorCompartmentContainer(compartmentIdx),
      $.compactor_node_affinity_matchers,
      true,
      compartmentCompactorMaxUnavailable,
    ) +
    // The per-compartment ScaledObject owns the replica count.
    $.removeReplicasFromSpec,

  // StatefulSets, Services and PDBs.
  compactor_statefulsets: $.mimirCompartmentsCreateIf(isEnabled, numCompartments, function(compartment) $.newCompactorCompartmentStatefulSet(compartment)),
  compactor_services: $.mimirCompartmentsCreateIf(isEnabled, numCompartments, function(compartment)
    $.util.serviceFor($.compactor_statefulsets['compartment_%d' % compartment], $._config.service_ignored_labels) +
    $.core.v1.service.mixin.spec.withClusterIp('None')),
  compactor_pdbs: $.mimirCompartmentsCreateIf(isEnabled, numCompartments, function(compartment) $.newMimirPdb('compactor-rc-%d' % compartment)),

  // Scaled objects.
  compactor_scaled_objects: $.mimirCompartmentsCreateIf(isEnabled, numCompartments, function(compartment)
    $.newCompactorSchedulerDrainScaledObject(
      'compactor-rc-%d' % compartment,
      'pod=~"compactor-scheduler-rc-%d-.*"' % compartment,
      'pod=~"compactor-rc-%d-.*"' % compartment,
      $._config.autoscaling_compactor_min_replicas_per_compartment,
      $._config.autoscaling_compactor_max_replicas_per_compartment,
    )),

  // Null out the non-compartments compactor resources.
  compactor_statefulset: if !isNoCompartmentsEnabled then null else super.compactor_statefulset,
  compactor_service: if !isNoCompartmentsEnabled then null else super.compactor_service,
  compactor_pdb: if !isNoCompartmentsEnabled then null else super.compactor_pdb,

  compactor_scaled_object:
    // When the non-compartments compactor also runs (e.g. during a migration), scope its drain autoscaler to
    // exclude the per-compartment pods.
    if isEnabled && isNoCompartmentsEnabled && $._config.compactor_scheduler_enabled then
      $.newCompactorSchedulerDrainScaledObject(
        'compactor',
        'pod!~"compactor-scheduler-rc-.*"',
        'pod!~"compactor-rc-.*"',
        $._config.autoscaling_compactor_min_replicas,
        $._config.autoscaling_compactor_max_replicas,
      )
    else if !isNoCompartmentsEnabled then null
    else super.compactor_scaled_object,

  // Config validation.
  local compactorCompartmentConfigError = $.validateMimirCompartmentsConfig(['compactor_statefulsets']),
  assert compactorCompartmentConfigError == null : compactorCompartmentConfigError,
}
