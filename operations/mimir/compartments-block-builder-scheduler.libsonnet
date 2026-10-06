{
  local statefulSet = $.apps.v1.statefulSet,

  local isEnabled = $._config.compartments_block_builder_enabled,
  local numCompartments = $._config.compartments_read_count,
  local isNoCompartmentsEnabled = $._config.no_compartments_block_builder_enabled,

  // Per-compartment scheduler endpoint the compartment's block-builders lease jobs from.
  blockBuilderSchedulerCompartmentEndpoint(compartmentIdx)::
    'block-builder-scheduler-rc-%d.%s.svc.%s:9095' % [compartmentIdx, $._config.namespace, $._config.cluster_domain],

  // Args. Each read compartment's scheduler plans jobs for that compartment's topic across every write
  // compartment's Kafka cluster, tracking offsets in a per-compartment consumer group.
  block_builder_scheduler_compartments_args:: $.mimirCompartmentsCreateIf(isEnabled, numCompartments, function(compartmentIdx)
    $.block_builder_scheduler_args +
    $.mimirCompartmentsCommonArgs +
    {
      'ingest-storage.kafka.address': $._config.compartments_ingest_storage_kafka_address,
      'ingest-storage.kafka.topic': $.mimirIngestStorageCompartmentKafkaTopic(compartmentIdx),
      'block-builder-scheduler.consumer-group': 'block-builder-rc-%d' % compartmentIdx,
    }),

  // Containers.
  newBlockBuilderSchedulerCompartmentContainer(compartmentIdx)::
    $.newBlockBuilderSchedulerContainer('block-builder-scheduler', $.block_builder_scheduler_compartments_args['compartment_%d' % compartmentIdx], $.block_builder_scheduler_env_map),

  block_builder_scheduler_containers:: $.mimirCompartmentsCreateIf(isEnabled, numCompartments, function(compartment) $.newBlockBuilderSchedulerCompartmentContainer(compartment)),

  // StatefulSets, Services and PDBs.
  newBlockBuilderSchedulerCompartmentStatefulSet(compartmentIdx)::
    local name = 'block-builder-scheduler-rc-%d' % compartmentIdx;
    $.newBlockBuilderSchedulerStatefulSet(name, $.block_builder_scheduler_containers['compartment_%d' % compartmentIdx], $.block_builder_scheduler_node_affinity_matchers) +
    statefulSet.mixin.metadata.withLabelsMixin({ 'mimir-rc': std.toString(compartmentIdx) }) +
    statefulSet.mixin.spec.template.metadata.withLabelsMixin({ 'mimir-rc': std.toString(compartmentIdx) }),

  block_builder_scheduler_statefulsets: $.mimirCompartmentsCreateIf(isEnabled, numCompartments, function(compartment) $.newBlockBuilderSchedulerCompartmentStatefulSet(compartment)),
  block_builder_scheduler_services: $.mimirCompartmentsCreateIf(isEnabled, numCompartments, function(compartment) $.newBlockBuilderSchedulerService('block-builder-scheduler-rc-%d' % compartment, $.block_builder_scheduler_statefulsets['compartment_%d' % compartment])),
  block_builder_scheduler_pdbs: $.mimirCompartmentsCreateIf(isEnabled, numCompartments, function(compartment) $.newMimirPdb('block-builder-scheduler-rc-%d' % compartment)),

  // Null out the non-compartments block-builder-scheduler when retired.
  block_builder_scheduler_statefulset: if isEnabled && !isNoCompartmentsEnabled then null else super.block_builder_scheduler_statefulset,
  block_builder_scheduler_service: if isEnabled && !isNoCompartmentsEnabled then null else super.block_builder_scheduler_service,
  block_builder_scheduler_pdb: if isEnabled && !isNoCompartmentsEnabled then null else super.block_builder_scheduler_pdb,

  // Config validation.
  local schedulerCompartmentConfigError = $.validateMimirCompartmentsConfig(['block_builder_scheduler_statefulsets']),
  assert schedulerCompartmentConfigError == null : schedulerCompartmentConfigError,
}
