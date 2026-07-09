{
  _config+:: {
    compartments_distributor_enabled: $._config.compartments_enabled,
    no_compartments_distributor_enabled: !self.compartments_distributor_enabled,
  },

  assert !$._config.compartments_distributor_enabled || $._config.multi_zone_distributor_enabled
         : 'compartments_distributor_enabled requires multi_zone_distributor_enabled',
  local distributorCompartmentZonesError = $.validateMimirCompartmentsDistributorZones(),
  assert distributorCompartmentZonesError == null : distributorCompartmentZonesError,
  assert !$._config.compartments_distributor_enabled || $._config.ingest_storage_enabled
         : 'compartments_distributor_enabled requires ingest_storage_enabled',
  assert !$._config.compartments_distributor_enabled || $._config.autoscaling_distributor_enabled
         : 'compartments_distributor_enabled requires autoscaling_distributor_enabled',

  local deployment = $.apps.v1.deployment,
  local service = $.core.v1.service,
  local podAntiAffinity = deployment.mixin.spec.template.spec.affinity.podAntiAffinity,

  local isEnabled = $._config.compartments_distributor_enabled,
  local numCompartments = $._config.compartments_write_count,
  local isZoneAEnabled = $._config.multi_zone_distributor_enabled && std.length($._config.multi_zone_availability_zones) >= 1,
  local isZoneBEnabled = $._config.multi_zone_distributor_enabled && std.length($._config.multi_zone_availability_zones) >= 2,
  local isZoneCEnabled = $._config.multi_zone_distributor_enabled && std.length($._config.multi_zone_availability_zones) >= 3,
  local isAutoscalingEnabled = $._config.autoscaling_distributor_enabled,
  local isNoCompartmentsEnabled = $._config.no_compartments_distributor_enabled,

  newDistributorCompartmentContainer(zone, compartmentIdx, args, extraEnvVarMap={})::
    $.newDistributorZoneContainer(zone, args, extraEnvVarMap),

  newDistributorCompartmentDeployment(zone, compartmentIdx, container, nodeAffinityMatchers=[])::
    local compartmentName = '%s-wc-%d' % [zone, compartmentIdx];
    local zoneName = 'distributor-zone-%s' % zone;

    $.newDistributorZoneDeployment(compartmentName, container, nodeAffinityMatchers) +
    // Add mimir-service label matching the zone so the zone service selects all compartments.
    deployment.mixin.metadata.withLabelsMixin({ 'mimir-service': zoneName }) +
    deployment.mixin.spec.template.metadata.withLabelsMixin({ 'mimir-service': zoneName }) +
    // Label identifying the write compartment — used by the write-path anti-affinity below.
    deployment.mixin.spec.template.metadata.withLabelsMixin({ 'mimir-wc': std.toString(compartmentIdx) }) +
    // Spread write compartments across nodes: don't schedule onto a node already running a pod with a different mimir-wc value.
    { spec+: podAntiAffinity.withRequiredDuringSchedulingIgnoredDuringExecution([
      podAntiAffinity.requiredDuringSchedulingIgnoredDuringExecutionType.new() +
      podAntiAffinity.requiredDuringSchedulingIgnoredDuringExecutionType.mixin.labelSelector.withMatchExpressions([
        { key: 'mimir-wc', operator: 'Exists' },
        { key: 'mimir-wc', operator: 'NotIn', values: [std.toString(compartmentIdx)] },
      ]) +
      podAntiAffinity.requiredDuringSchedulingIgnoredDuringExecutionType.withTopologyKey('kubernetes.io/hostname'),
    ]).spec } +
    (if !isAutoscalingEnabled then {} else $.removeReplicasFromSpec),

  newDistributorCompartmentScaledObject(name, zone, numCompartments)::
    $.newDistributorScaledObject(
      name=name,
      extra_matchers='pod=~"distributor-zone-%s.*"' % zone,
      weight=1.0 / numCompartments,  // scale each compartment to 1/N of the total zone load
    ),

  // Null out no-compartment Deployments, PDBs, and ScaledObjects when decommissioning.
  distributor_zone_a_deployment: if !isNoCompartmentsEnabled && isZoneAEnabled then null else super.distributor_zone_a_deployment,
  distributor_zone_b_deployment: if !isNoCompartmentsEnabled && isZoneBEnabled then null else super.distributor_zone_b_deployment,
  distributor_zone_c_deployment: if !isNoCompartmentsEnabled && isZoneCEnabled then null else super.distributor_zone_c_deployment,

  distributor_zone_a_pdb: if !isNoCompartmentsEnabled && isZoneAEnabled then null else super.distributor_zone_a_pdb,
  distributor_zone_b_pdb: if !isNoCompartmentsEnabled && isZoneBEnabled then null else super.distributor_zone_b_pdb,
  distributor_zone_c_pdb: if !isNoCompartmentsEnabled && isZoneCEnabled then null else super.distributor_zone_c_pdb,

  distributor_zone_a_scaled_object: if !isNoCompartmentsEnabled && isZoneAEnabled then null else super.distributor_zone_a_scaled_object,
  distributor_zone_b_scaled_object: if !isNoCompartmentsEnabled && isZoneBEnabled then null else super.distributor_zone_b_scaled_object,
  distributor_zone_c_scaled_object: if !isNoCompartmentsEnabled && isZoneCEnabled then null else super.distributor_zone_c_scaled_object,

  // When decommissioning, the zone deployment is null, so super.distributor_zone_X_service
  // can't be accessed (it calls util.serviceFor on the null deployment). Re-create the
  // service from scratch instead, deriving ports from compartment_0 and overriding the name/selector
  // so the service covers all compartments in the zone.
  local newZoneService(zone, compartmentDeploy) =
    $.util.serviceFor(compartmentDeploy, $._config.service_ignored_labels) +
    service.mixin.metadata.withName('distributor-zone-%s' % zone) +
    service.mixin.metadata.withLabels({ name: 'distributor-zone-%s' % zone }) +
    service.mixin.spec.withSelector({ 'mimir-service': 'distributor-zone-%s' % zone }) +
    service.mixin.spec.withClusterIp('None'),

  local newCompartmentsZoneService(zone, compartmentDeploy) =
    $.util.serviceFor(compartmentDeploy, $._config.service_ignored_labels) +
    service.mixin.metadata.withName('distributor-zone-%s-compartments' % zone) +
    service.mixin.metadata.withLabels({ name: 'distributor-zone-%s-compartments' % zone }) +
    service.mixin.spec.withSelector({ 'mimir-service': 'distributor-zone-%s' % zone }) +
    service.mixin.spec.withClusterIp('None'),

  distributor_zone_a_service: if !isNoCompartmentsEnabled && isZoneAEnabled then
    newZoneService('a', $.distributor_zone_a_deployments.compartment_0)
  else super.distributor_zone_a_service,
  distributor_zone_b_service: if !isNoCompartmentsEnabled && isZoneBEnabled then
    newZoneService('b', $.distributor_zone_b_deployments.compartment_0)
  else super.distributor_zone_b_service,
  distributor_zone_c_service: if !isNoCompartmentsEnabled && isZoneCEnabled then
    newZoneService('c', $.distributor_zone_c_deployments.compartment_0)
  else super.distributor_zone_c_service,

  distributor_zone_a_compartments_service: if isEnabled && isZoneAEnabled then
    newCompartmentsZoneService('a', $.distributor_zone_a_deployments.compartment_0)
  else null,
  distributor_zone_b_compartments_service: if isEnabled && isZoneBEnabled then
    newCompartmentsZoneService('b', $.distributor_zone_b_deployments.compartment_0)
  else null,
  distributor_zone_c_compartments_service: if isEnabled && isZoneCEnabled then
    newCompartmentsZoneService('c', $.distributor_zone_c_deployments.compartment_0)
  else null,

  // Args.
  local perCompartmentDistributorArgs(compartmentIdx) =
    $.mimirCompartmentsCommonArgs {
      'distributor.write-compartment-id': compartmentIdx,

      // The distributor targets its own write compartment's Kafka cluster, so its address resolves the
      // '<write-compartment-id>' placeholder to the compartment id. The topic keeps the read-compartment
      // placeholder because the distributor produces to every read compartment's topic.
      'ingest-storage.kafka.address': std.strReplace($._config.compartments_ingest_storage_kafka_address, '<write-compartment-id>', std.toString(compartmentIdx)),
    },

  distributor_zone_a_compartments_args:: $.mimirCompartmentsCreateIf(isEnabled && isZoneAEnabled, numCompartments, function(compartment) $.distributor_zone_a_args + perCompartmentDistributorArgs(compartment)),
  distributor_zone_b_compartments_args:: $.mimirCompartmentsCreateIf(isEnabled && isZoneBEnabled, numCompartments, function(compartment) $.distributor_zone_b_args + perCompartmentDistributorArgs(compartment)),
  distributor_zone_c_compartments_args:: $.mimirCompartmentsCreateIf(isEnabled && isZoneCEnabled, numCompartments, function(compartment) $.distributor_zone_c_args + perCompartmentDistributorArgs(compartment)),

  // Containers.
  distributor_zone_a_containers:: $.mimirCompartmentsCreateIf(isEnabled && isZoneAEnabled, numCompartments, function(compartment) $.newDistributorCompartmentContainer('a', compartment, $.distributor_zone_a_compartments_args['compartment_%d' % compartment], $.distributor_zone_a_env_map)),
  distributor_zone_b_containers:: $.mimirCompartmentsCreateIf(isEnabled && isZoneBEnabled, numCompartments, function(compartment) $.newDistributorCompartmentContainer('b', compartment, $.distributor_zone_b_compartments_args['compartment_%d' % compartment], $.distributor_zone_b_env_map)),
  distributor_zone_c_containers:: $.mimirCompartmentsCreateIf(isEnabled && isZoneCEnabled, numCompartments, function(compartment) $.newDistributorCompartmentContainer('c', compartment, $.distributor_zone_c_compartments_args['compartment_%d' % compartment], $.distributor_zone_c_env_map)),

  // Deployments.
  distributor_zone_a_deployments: $.mimirCompartmentsCreateIf(isEnabled && isZoneAEnabled, numCompartments, function(compartment) $.newDistributorCompartmentDeployment('a', compartment, $.distributor_zone_a_containers['compartment_%d' % compartment], $.distributor_zone_a_node_affinity_matchers)),
  distributor_zone_b_deployments: $.mimirCompartmentsCreateIf(isEnabled && isZoneBEnabled, numCompartments, function(compartment) $.newDistributorCompartmentDeployment('b', compartment, $.distributor_zone_b_containers['compartment_%d' % compartment], $.distributor_zone_b_node_affinity_matchers)),
  distributor_zone_c_deployments: $.mimirCompartmentsCreateIf(isEnabled && isZoneCEnabled, numCompartments, function(compartment) $.newDistributorCompartmentDeployment('c', compartment, $.distributor_zone_c_containers['compartment_%d' % compartment], $.distributor_zone_c_node_affinity_matchers)),

  // PDBs.
  distributor_zone_a_pdbs: $.mimirCompartmentsCreateIf(isEnabled && isZoneAEnabled, numCompartments, function(compartment) $.newMimirPdb('distributor-zone-a-wc-%d' % compartment)),
  distributor_zone_b_pdbs: $.mimirCompartmentsCreateIf(isEnabled && isZoneBEnabled, numCompartments, function(compartment) $.newMimirPdb('distributor-zone-b-wc-%d' % compartment)),
  distributor_zone_c_pdbs: $.mimirCompartmentsCreateIf(isEnabled && isZoneCEnabled, numCompartments, function(compartment) $.newMimirPdb('distributor-zone-c-wc-%d' % compartment)),

  // Scaled objects.
  distributor_zone_a_scaled_objects: $.mimirCompartmentsCreateIf(isEnabled && isZoneAEnabled && isAutoscalingEnabled, numCompartments, function(compartment) $.newDistributorCompartmentScaledObject('distributor-zone-a-wc-%d' % compartment, 'a', numCompartments)),
  distributor_zone_b_scaled_objects: $.mimirCompartmentsCreateIf(isEnabled && isZoneBEnabled && isAutoscalingEnabled, numCompartments, function(compartment) $.newDistributorCompartmentScaledObject('distributor-zone-b-wc-%d' % compartment, 'b', numCompartments)),
  distributor_zone_c_scaled_objects: $.mimirCompartmentsCreateIf(isEnabled && isZoneCEnabled && isAutoscalingEnabled, numCompartments, function(compartment) $.newDistributorCompartmentScaledObject('distributor-zone-c-wc-%d' % compartment, 'c', numCompartments)),

  // Config validation.
  local distributorCompartmentMultiZoneError = $.validateMimirMultiZoneConfig([
    'distributor_zone_a_deployments',
    'distributor_zone_b_deployments',
    'distributor_zone_c_deployments',
  ]),
  assert distributorCompartmentMultiZoneError == null : distributorCompartmentMultiZoneError,

  local distributorCompartmentConfigError = $.validateMimirCompartmentsConfig([
    'distributor_zone_a_deployments',
    'distributor_zone_b_deployments',
    'distributor_zone_c_deployments',
  ]),
  assert distributorCompartmentConfigError == null : distributorCompartmentConfigError,
}
