{
  _config+:: {
    compartments_block_builder_enabled: $._config.compartments_enabled && $._config.block_builder.enabled,
    no_compartments_block_builder_enabled: !self.compartments_block_builder_enabled,

    // Per-compartment block-builder autoscaling bounds (per read compartment).
    autoscaling_block_builder_min_replicas_per_compartment: 1,
    autoscaling_block_builder_max_replicas_per_compartment: 10,

    // Read compartment indexes whose KEDA autoscaling is disabled: the compartment's ScaledObject
    // is not rendered and its Deployment runs the static replica count configured below. Only
    // subtractive within an enabled component — it cannot re-enable single compartments when
    // block_builder.autoscaling_enabled is off.
    autoscaling_block_builder_disabled_compartments: [],

    // Explicit replica count per disabled compartment, e.g. { compartment_0: 3 }. Required for
    // every compartment listed above (the render fails otherwise).
    block_builder_compartment_static_replicas: {},
  },

  assert !$._config.compartments_block_builder_enabled || $._config.block_builder.enabled
         : 'compartments_block_builder_enabled requires block_builder.enabled',
  // Each read compartment's block-builders upload into that compartment's blocks bucket, which only
  // the per-compartment compactors compact.
  assert !$._config.compartments_block_builder_enabled || $._config.compartments_compactor_enabled
         : 'compartments_block_builder_enabled requires compartments_compactor_enabled',

  local container = $.core.v1.container,
  local deployment = $.apps.v1.deployment,

  local isEnabled = $._config.compartments_block_builder_enabled,
  local numCompartments = $._config.compartments_read_count,
  local isNoCompartmentsEnabled = $._config.no_compartments_block_builder_enabled,
  local isAutoscalingEnabled = $._config.block_builder.autoscaling_enabled,
  local isCompartmentAutoscalingDisabled(compartment) = std.member($._config.autoscaling_block_builder_disabled_compartments, compartment),
  local compartmentStaticReplicas(compartment) = $._config.block_builder_compartment_static_replicas['compartment_%d' % compartment],

  // Args. Each read compartment's block-builders consume that compartment's topic from every write
  // compartment's Kafka cluster and upload into the compartment's blocks bucket.
  local perCompartmentBlockBuilderArgs(compartmentIdx) =
    $.mimirCompartmentsCommonArgs {
      'ingest-storage.kafka.address': $._config.compartments_ingest_storage_kafka_address,
      'ingest-storage.kafka.topic': $.mimirIngestStorageCompartmentKafkaTopic(compartmentIdx),
      [$.mimirBlocksStorageBucketNameFlag]: $.mimirBlocksStorageCompartmentBucketName(compartmentIdx),
    },

  block_builder_compartments_args:: $.mimirCompartmentsCreateIf(isEnabled, numCompartments, function(compartment) $.block_builder_args + perCompartmentBlockBuilderArgs(compartment)),

  // Containers. Built on top of block_builder_container so container patches (resources, env, ...)
  // layered onto it apply to every compartment.
  newBlockBuilderCompartmentContainer(compartmentIdx)::
    $.block_builder_container +
    container.withArgs($.util.mapToFlags($.block_builder_compartments_args['compartment_%d' % compartmentIdx])),

  block_builder_containers:: $.mimirCompartmentsCreateIf(isEnabled, numCompartments, function(compartment) $.newBlockBuilderCompartmentContainer(compartment)),

  // Deployments.
  newBlockBuilderCompartmentDeployment(compartmentIdx)::
    local compartmentIdxStr = std.toString(compartmentIdx);
    $.newBlockBuilderDeployment(
      'block-builder-rc-%d' % compartmentIdx,
      // Referenced through the compartment map (not built inline) so per-compartment container
      // patches layered onto block_builder_containers propagate into the rendered Deployment.
      $.block_builder_containers['compartment_%d' % compartmentIdx],
      $.block_builder_node_affinity_matchers,
    ) +
    deployment.mixin.metadata.withLabelsMixin({ 'mimir-rc': compartmentIdxStr }) +
    deployment.mixin.spec.template.metadata.withLabelsMixin({ 'mimir-rc': compartmentIdxStr }) +
    (
      if isCompartmentAutoscalingDisabled(compartmentIdx)
      // Autoscaling is disabled for this compartment (no ScaledObject rendered), so the
      // Deployment runs an explicit static replica count.
      then deployment.mixin.spec.withReplicas(compartmentStaticReplicas(compartmentIdx))
      // The per-compartment ScaledObject owns the replica count.
      else if isAutoscalingEnabled then $.removeReplicasFromSpec
      // No autoscaling: run block_builder.replicas, like the non-compartments block-builder.
      else {}
    ),

  block_builder_deployments: $.mimirCompartmentsCreateIf(isEnabled, numCompartments, function(compartment) $.newBlockBuilderCompartmentDeployment(compartment)),
  block_builder_pdbs: $.mimirCompartmentsCreateIf(isEnabled, numCompartments, function(compartment) $.newMimirPdb('block-builder-rc-%d' % compartment)),

  // Scaled objects.
  block_builder_scaled_objects: $.mimirCompartmentsCreateIf(isEnabled && isAutoscalingEnabled, numCompartments, function(compartment)
    $.newBlockBuilderScaledObject(
      service_name='block-builder-rc-%d' % compartment,
      min_replicas=$._config.autoscaling_block_builder_min_replicas_per_compartment,
      max_replicas=$._config.autoscaling_block_builder_max_replicas_per_compartment,
      target_kind='Deployment',
      scheduler_extra_matchers='pod=~"block-builder-scheduler-rc-%d-.*"' % compartment,
      blockbuilder_extra_matchers='pod=~"block-builder-rc-%d-.*"' % compartment,
    ), $._config.autoscaling_block_builder_disabled_compartments),

  // Null out the non-compartments block-builder resources.
  block_builder_deployment: if isEnabled && !isNoCompartmentsEnabled then null else super.block_builder_deployment,
  block_builder_pdb: if isEnabled && !isNoCompartmentsEnabled then null else super.block_builder_pdb,

  block_builder_scaled_object:
    // When the non-compartments block-builder also runs (e.g. during a migration), scope its autoscaler to
    // exclude the per-compartment pods, which share the same container names.
    if isEnabled && isNoCompartmentsEnabled && isAutoscalingEnabled then
      $.newBlockBuilderScaledObject(
        service_name='block-builder',
        min_replicas=$._config.block_builder.autoscaling_min_replicas,
        max_replicas=$._config.block_builder.autoscaling_max_replicas,
        target_kind='Deployment',
        scheduler_extra_matchers='pod!~"block-builder-scheduler-rc-.*"',
        blockbuilder_extra_matchers='pod!~"block-builder-rc-.*"',
      )
    else if isEnabled && !isNoCompartmentsEnabled then null
    else super.block_builder_scaled_object,

  // Config validation.
  local blockBuilderCompartmentConfigError = $.validateMimirCompartmentsConfig(['block_builder_deployments']),
  assert blockBuilderCompartmentConfigError == null : blockBuilderCompartmentConfigError,

  local blockBuilderDisabledKnobsError = if !isEnabled then null else $.validateMimirCompartmentsAutoscalingDisabledKnobs(
    'autoscaling_block_builder_disabled_compartments',
    $._config.autoscaling_block_builder_disabled_compartments,
    'block_builder_compartment_static_replicas',
    $._config.block_builder_compartment_static_replicas,
    numCompartments,
    'block_builder.autoscaling_enabled',
    isAutoscalingEnabled,
  ),
  assert blockBuilderDisabledKnobsError == null : blockBuilderDisabledKnobsError,
}
