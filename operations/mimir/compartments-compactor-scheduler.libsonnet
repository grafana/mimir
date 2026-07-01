{
  local statefulSet = $.apps.v1.statefulSet,

  assert !$._config.compartments_compactor_enabled || $._config.compactor_scheduler_enabled
         : 'compartments_compactor_enabled requires compactor_scheduler_enabled',

  local isEnabled = $._config.compartments_compactor_enabled,
  local numCompartments = $._config.compartments_read_count,
  local isNoCompartmentsEnabled = $._config.no_compartments_compactor_enabled,

  compactorSchedulerCompartmentEndpoint(compartmentIdx)::
    'dns:///compactor-scheduler-rc-%d.%s.svc.%s:9095' % [compartmentIdx, $._config.namespace, $._config.cluster_domain],

  // Per-compartment compactors lease from their own compactor-scheduler, so overwrite the single-scheduler
  // endpoint they inherit (via compactor_args) with the per-compartment one.
  compactor_compartments_args+:: $.mimirCompartmentsOverrides(super.compactor_compartments_args, function(compartmentIdx) {
    'compactor.scheduler-client.scheduler-endpoint': $.compactorSchedulerCompartmentEndpoint(compartmentIdx),
  }),

  // Args.
  compactor_scheduler_compartments_args:: $.mimirCompartmentsCreateIf(isEnabled, numCompartments, function(compartmentIdx)
    $.compactor_scheduler_args {
      [$.mimirBlocksStorageBucketNameFlag]: $.mimirBlocksStorageCompartmentBucketName(compartmentIdx),

      // The scheduler doesn't consume Kafka, but it inherits the ingest-storage args from the common config,
      // so set them to this read compartment's values for consistency (and so the compartments config
      // validation passes).
      'ingest-storage.kafka.address': $._config.compartments_ingest_storage_kafka_address,
      'ingest-storage.kafka.topic': $.mimirIngestStorageCompartmentKafkaTopic(compartmentIdx),
    }),

  // Containers.
  newCompactorSchedulerCompartmentContainer(compartmentIdx)::
    $.newCompactorSchedulerContainer('compactor-scheduler', $.compactor_scheduler_compartments_args['compartment_%d' % compartmentIdx], $.compactor_scheduler_env_map),

  compactor_scheduler_containers:: $.mimirCompartmentsCreateIf(isEnabled, numCompartments, function(compartment) $.newCompactorSchedulerCompartmentContainer(compartment)),

  // StatefulSets, Services and PDBs.
  newCompactorSchedulerCompartmentStatefulSet(compartmentIdx)::
    local name = 'compactor-scheduler-rc-%d' % compartmentIdx;
    $.newCompactorSchedulerStatefulSet(name, $.compactor_scheduler_containers['compartment_%d' % compartmentIdx]) +
    statefulSet.mixin.metadata.withLabelsMixin({ 'mimir-rc': std.toString(compartmentIdx) }) +
    statefulSet.mixin.spec.template.metadata.withLabelsMixin({ 'mimir-rc': std.toString(compartmentIdx) }),

  compactor_scheduler_statefulsets: $.mimirCompartmentsCreateIf(isEnabled, numCompartments, function(compartment) $.newCompactorSchedulerCompartmentStatefulSet(compartment)),
  compactor_scheduler_services: $.mimirCompartmentsCreateIf(isEnabled, numCompartments, function(compartment) $.newCompactorSchedulerService('compactor-scheduler-rc-%d' % compartment, $.compactor_scheduler_statefulsets['compartment_%d' % compartment])),
  compactor_scheduler_pdbs: $.mimirCompartmentsCreateIf(isEnabled, numCompartments, function(compartment) $.newMimirPdb('compactor-scheduler-rc-%d' % compartment)),

  // Per-compartment compactors are ephemeral workers when the scheduler is enabled (the scheduler re-leases
  // jobs from compactors that go away), so delete their data volumes on scale-down and deletion. Mirrors the
  // singular compactor in compactor-scheduler.libsonnet.
  local schedulerWorkerPvcRetention = if !$._config.compactor_scheduler_enabled then {} else
    statefulSet.spec.persistentVolumeClaimRetentionPolicy.withWhenScaled('Delete') +
    statefulSet.spec.persistentVolumeClaimRetentionPolicy.withWhenDeleted('Delete'),
  compactor_statefulsets+: $.mimirCompartmentsOverrides(super.compactor_statefulsets, schedulerWorkerPvcRetention),

  // Null out the non-compartments compactor-scheduler when retired.
  compactor_scheduler_statefulset: if isEnabled && !isNoCompartmentsEnabled then null else super.compactor_scheduler_statefulset,
  compactor_scheduler_service: if isEnabled && !isNoCompartmentsEnabled then null else super.compactor_scheduler_service,
  compactor_scheduler_pdb: if isEnabled && !isNoCompartmentsEnabled then null else super.compactor_scheduler_pdb,

  // Config validation.
  local schedulerCompartmentConfigError = $.validateMimirCompartmentsConfig(['compactor_scheduler_statefulsets']),
  assert schedulerCompartmentConfigError == null : schedulerCompartmentConfigError,
}
