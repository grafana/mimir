{
  _config+:: {
    multi_zone_store_gateway_enabled: $._config.multi_zone_read_path_enabled,
    multi_zone_store_gateway_read_path_enabled: $._config.multi_zone_store_gateway_enabled,
    multi_zone_store_gateway_migration_enabled: false,
    multi_zone_store_gateway_replicas: 0,
    multi_zone_store_gateway_zpdb_enabled: $._config.multi_zone_store_gateway_enabled,

    // When store-gateway lazy loading is disabled, store-gateway may take a long time to startup.
    // To speed up rollouts, we increase the max unavailable to rollout all store-gateways in a zone in a single batch.
    multi_zone_store_gateway_max_unavailable: if $._config.store_gateway_lazy_loading_enabled then 50 else 1000,

    multi_zone_store_gateway_zpdb_max_unavailable: std.toString($._config.multi_zone_store_gateway_max_unavailable),

    // Controls whether the multi (virtual) zone store-gateway should also be deployed multi-AZ.
    multi_zone_store_gateway_multi_az_enabled: $._config.multi_zone_read_path_multi_az_enabled,

    multi_zone_store_gateway_zone_a_multi_az_enabled: $._config.multi_zone_store_gateway_multi_az_enabled,
    multi_zone_store_gateway_zone_b_multi_az_enabled: $._config.multi_zone_store_gateway_multi_az_enabled,
    multi_zone_store_gateway_zone_c_multi_az_enabled: $._config.multi_zone_store_gateway_multi_az_enabled,

    // Available for overriding as part of migration to multi_az, e.g: from zone's [a, b, c] to [a, a-backup, b, b-backup].
    multi_zone_store_gateway_zone_c_enabled: $._config.multi_zone_store_gateway_enabled,

    multi_zone_store_gateway_backup_zones_enabled: false,
    multi_zone_store_gateway_zone_a_backup_enabled: $._config.multi_zone_store_gateway_backup_zones_enabled && $._config.multi_zone_store_gateway_enabled,
    multi_zone_store_gateway_zone_b_backup_enabled: $._config.multi_zone_store_gateway_backup_zones_enabled && $._config.multi_zone_store_gateway_enabled,

    multi_zone_store_gateway_zone_a_backup_multi_az_enabled: $._config.multi_zone_store_gateway_multi_az_enabled,
    multi_zone_store_gateway_zone_b_backup_multi_az_enabled: $._config.multi_zone_store_gateway_multi_az_enabled,
  },

  local container = $.core.v1.container,
  local statefulSet = $.apps.v1.statefulSet,
  local podDisruptionBudget = $.policy.v1.podDisruptionBudget,
  local service = $.core.v1.service,
  local servicePort = $.core.v1.servicePort,
  local podAntiAffinity = $.apps.v1.deployment.mixin.spec.template.spec.affinity.podAntiAffinity,

  local isMultiAZEnabled = $._config.multi_zone_store_gateway_zone_a_multi_az_enabled || $._config.multi_zone_store_gateway_zone_b_multi_az_enabled || $._config.multi_zone_store_gateway_zone_c_multi_az_enabled,
  local isZoneAEnabled = $._config.multi_zone_store_gateway_zone_a_multi_az_enabled && std.length($._config.multi_zone_availability_zones) >= 1,
  local isZoneBEnabled = $._config.multi_zone_store_gateway_zone_b_multi_az_enabled && std.length($._config.multi_zone_availability_zones) >= 2,
  local isZoneCEnabled = $._config.multi_zone_store_gateway_zone_c_multi_az_enabled && std.length($._config.multi_zone_availability_zones) >= 3,
  local isMultiAZAEnabled = $._config.multi_zone_store_gateway_zone_a_backup_multi_az_enabled && std.length($._config.multi_zone_availability_zones) >= 1,
  local isMultiAZBEnabled = $._config.multi_zone_store_gateway_zone_b_backup_multi_az_enabled && std.length($._config.multi_zone_availability_zones) >= 2,

  assert !isMultiAZEnabled || $._config.multi_zone_store_gateway_enabled : 'store-gateway multi-AZ deployment requires store-gateway multi-zone to be enabled',
  assert !isMultiAZEnabled || $._config.multi_zone_memcached_enabled : 'store-gateway multi-AZ deployment requires memcached multi-zone to be enabled',
  assert !$._config.multi_zone_store_gateway_zpdb_enabled || $._config.rollout_operator_webhooks_enabled : 'zpdb configuration requires rollout_operator_webhooks_enabled=true',

  //
  // Zone-aware replication.
  //

  ruler_args+:: (
    // During the migration, if read path switch is enabled we need to apply changes directly to rulers instead of queryBlocksStorageConfig.
    if !($._config.multi_zone_store_gateway_enabled && $._config.multi_zone_store_gateway_read_path_enabled && $._config.multi_zone_store_gateway_migration_enabled) then {} else {
      'store-gateway.sharding-ring.zone-awareness-enabled': 'true',
      'store-gateway.sharding-ring.prefix': 'multi-zone/',
    }
  ),

  querier_args+:: (
    // During the migration, if read path switch is enabled we need to apply changes directly to queriers instead of queryBlocksStorageConfig.
    if !($._config.multi_zone_store_gateway_enabled && $._config.multi_zone_store_gateway_read_path_enabled && $._config.multi_zone_store_gateway_migration_enabled) then {} else {
      'store-gateway.sharding-ring.zone-awareness-enabled': 'true',
      'store-gateway.sharding-ring.prefix': 'multi-zone/',
    }
  ),

  //
  // Multi-zone store-gateways.
  //

  store_gateway_zone_a_args:: if !isZoneAEnabled then {} else $.blocks_chunks_zone_a_caching_config + $.blocks_metadata_zone_a_caching_config,
  store_gateway_zone_b_args:: if !isZoneBEnabled then {} else $.blocks_chunks_zone_b_caching_config + $.blocks_metadata_zone_b_caching_config,
  store_gateway_zone_c_args::
    if isZoneCEnabled then
      $.blocks_chunks_zone_c_caching_config + $.blocks_metadata_zone_c_caching_config
    else if isZoneAEnabled then
      // Edge case: when store-gateway is deployed multi-AZ but there are only 2 AZs, then store-gateway zone-c
      // runs in the same AZ as zone-a, so we should run it with that config.
      $.blocks_chunks_zone_a_caching_config + $.blocks_metadata_zone_a_caching_config
    else
      {},
  store_gateway_zone_a_backup_args:: if !isMultiAZAEnabled then {} else
    $.blocks_chunks_zone_a_caching_config +
    $.blocks_metadata_zone_a_caching_config,

  store_gateway_zone_b_backup_args:: if !isMultiAZBEnabled then {} else
    $.blocks_chunks_zone_b_caching_config +
    $.blocks_metadata_zone_b_caching_config,

  store_gateway_zone_a_env_map:: $.store_gateway_env_map,
  store_gateway_zone_b_env_map:: $.store_gateway_env_map,
  store_gateway_zone_c_env_map:: $.store_gateway_env_map,
  store_gateway_zone_a_backup_env_map:: $.store_gateway_env_map,
  store_gateway_zone_b_backup_env_map:: $.store_gateway_env_map,

  store_gateway_zone_a_node_affinity_matchers:: $.store_gateway_node_affinity_matchers + (if isZoneAEnabled then [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[0])] else []),
  store_gateway_zone_b_node_affinity_matchers:: $.store_gateway_node_affinity_matchers + (if isZoneBEnabled then [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[1])] else []),
  store_gateway_zone_c_node_affinity_matchers:: $.store_gateway_node_affinity_matchers + (if isZoneCEnabled then [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[2])] else []),
  store_gateway_zone_a_backup_node_affinity_matchers:: $.store_gateway_node_affinity_matchers + (if isMultiAZAEnabled then [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[0])] else []),
  store_gateway_zone_b_backup_node_affinity_matchers:: $.store_gateway_node_affinity_matchers + (if isMultiAZBEnabled then [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[1])] else []),

  newStoreGatewayZoneContainer(zone, zone_args, envmap={})::
    $.store_gateway_container +
    container.withArgs($.util.mapToFlags(
      // This first block contains flags that can be overridden.
      {
        // Do not unregister from ring at shutdown, so that no blocks re-shuffling occurs during rollouts.
        'store-gateway.sharding-ring.unregister-on-shutdown': false,
      } + $.store_gateway_args + zone_args + {
        'store-gateway.sharding-ring.instance-availability-zone': 'zone-%s' % zone,
        'store-gateway.sharding-ring.zone-awareness-enabled': true,

        // Use a different prefix so that both single-zone and multi-zone store-gateway rings can co-exists.
        'store-gateway.sharding-ring.prefix': 'multi-zone/',
      }
    )) +
    (if std.length(envmap) > 0 then container.withEnvMap(std.prune(envmap)) else {}),

  newStoreGatewayZoneStatefulSet(zone, container, nodeAffinityMatchers=[], rolloutGroup='store-gateway')::
    local name = 'store-gateway-zone-%s' % zone;

    $.newStoreGatewayStatefulSet(name, container, withAntiAffinity=false, nodeAffinityMatchers=nodeAffinityMatchers) +
    statefulSet.mixin.metadata.withLabels({ 'rollout-group': rolloutGroup }) +
    statefulSet.mixin.metadata.withAnnotations({ 'rollout-max-unavailable': std.toString($._config.multi_zone_store_gateway_max_unavailable) }) +
    statefulSet.mixin.spec.template.metadata.withLabels({ name: name, 'rollout-group': rolloutGroup }) +
    statefulSet.mixin.spec.selector.withMatchLabels({ name: name, 'rollout-group': rolloutGroup }) +
    statefulSet.mixin.spec.updateStrategy.withType('OnDelete') +
    statefulSet.mixin.spec.withReplicas(std.ceil($._config.multi_zone_store_gateway_replicas / 3)) +
    if $._config.store_gateway_allow_multiple_replicas_on_same_node then {} else {
      spec+:
        // Allow to schedule 2+ store-gateways in the same zone on the same node, but do not schedule 2+ store-gateways in
        // different zones on the same node. In case of 1 node failure in the Kubernetes cluster, only store-gateways
        // in 1 zone will be affected.
        podAntiAffinity.withRequiredDuringSchedulingIgnoredDuringExecution([
          podAntiAffinity.requiredDuringSchedulingIgnoredDuringExecutionType.new() +
          podAntiAffinity.requiredDuringSchedulingIgnoredDuringExecutionType.mixin.labelSelector.withMatchExpressions([
            { key: 'rollout-group', operator: 'In', values: [rolloutGroup] },
            { key: 'name', operator: 'NotIn', values: [name] },
          ]) +
          podAntiAffinity.requiredDuringSchedulingIgnoredDuringExecutionType.withTopologyKey('kubernetes.io/hostname'),
        ]).spec,
    },

  // Creates a headless service for the per-zone store-gateways StatefulSet. We don't use it
  // but we need to create it anyway because it's responsible for the network identity of
  // the StatefulSet pods. For more information, see:
  // https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.25/#statefulset-v1-apps
  newStoreGatewayZoneService(sts)::
    $.util.serviceFor(sts, $._config.service_ignored_labels) +
    service.mixin.spec.withClusterIp('None'),  // Headless.

  store_gateway_zone_a_container:: if !$._config.multi_zone_store_gateway_enabled then null else
    $.newStoreGatewayZoneContainer('a', $.store_gateway_zone_a_args, $.store_gateway_zone_a_env_map),

  store_gateway_zone_b_container:: if !$._config.multi_zone_store_gateway_enabled then null else
    $.newStoreGatewayZoneContainer('b', $.store_gateway_zone_b_args, $.store_gateway_zone_b_env_map),

  store_gateway_zone_c_container:: if !$._config.multi_zone_store_gateway_zone_c_enabled then null else
    $.newStoreGatewayZoneContainer('c', $.store_gateway_zone_c_args, $.store_gateway_zone_c_env_map),

  store_gateway_zone_a_backup_container:: if !$._config.multi_zone_store_gateway_zone_a_backup_enabled then null else
    $.newStoreGatewayZoneContainer('a-backup', $.store_gateway_zone_a_backup_args, $.store_gateway_zone_a_backup_env_map),

  store_gateway_zone_b_backup_container:: if !$._config.multi_zone_store_gateway_zone_b_backup_enabled then null else
    $.newStoreGatewayZoneContainer('b-backup', $.store_gateway_zone_b_backup_args, $.store_gateway_zone_b_backup_env_map),

  store_gateway_zone_b_statefulset: if !$._config.multi_zone_store_gateway_enabled then null else
    $.newStoreGatewayZoneStatefulSet('b', $.store_gateway_zone_b_container, $.store_gateway_zone_b_node_affinity_matchers) +
    (if isZoneBEnabled then statefulSet.spec.template.spec.withTolerationsMixin($.newMimirMultiZoneToleration()) else {}),

  store_gateway_zone_a_statefulset: if !$._config.multi_zone_store_gateway_enabled then null else
    $.newStoreGatewayZoneStatefulSet('a', $.store_gateway_zone_a_container, $.store_gateway_zone_a_node_affinity_matchers) +
    (if isZoneAEnabled then statefulSet.spec.template.spec.withTolerationsMixin($.newMimirMultiZoneToleration()) else {}),

  store_gateway_zone_c_statefulset: if !$._config.multi_zone_store_gateway_zone_c_enabled then null else
    $.newStoreGatewayZoneStatefulSet('c', $.store_gateway_zone_c_container, $.store_gateway_zone_c_node_affinity_matchers) +
    (if isZoneCEnabled then statefulSet.spec.template.spec.withTolerationsMixin($.newMimirMultiZoneToleration()) else {}),

  store_gateway_zone_a_backup_statefulset: if !$._config.multi_zone_store_gateway_zone_a_backup_enabled then null else
    $.newStoreGatewayZoneStatefulSet('a-backup', $.store_gateway_zone_a_backup_container, $.store_gateway_zone_a_backup_node_affinity_matchers) +
    (if isMultiAZAEnabled then statefulSet.spec.template.spec.withTolerationsMixin($.newMimirMultiZoneToleration()) else {}) +
    // Default to 0 replicas because we expect to use autoscaling and follow other zone replicas.
    statefulSet.mixin.spec.withReplicas(0),

  store_gateway_zone_b_backup_statefulset: if !$._config.multi_zone_store_gateway_zone_b_backup_enabled then null else
    $.newStoreGatewayZoneStatefulSet('b-backup', $.store_gateway_zone_b_backup_container, $.store_gateway_zone_b_backup_node_affinity_matchers) +
    (if isMultiAZBEnabled then statefulSet.spec.template.spec.withTolerationsMixin($.newMimirMultiZoneToleration()) else {}) +
    // Default to 0 replicas because we expect to use autoscaling and follow other zone replicas.
    statefulSet.mixin.spec.withReplicas(0),

  store_gateway_zone_a_service: if !$._config.multi_zone_store_gateway_enabled then null else
    $.newStoreGatewayZoneService($.store_gateway_zone_a_statefulset),

  store_gateway_zone_b_service: if !$._config.multi_zone_store_gateway_enabled then null else
    $.newStoreGatewayZoneService($.store_gateway_zone_b_statefulset),

  store_gateway_zone_c_service: if !$._config.multi_zone_store_gateway_zone_c_enabled then null else
    $.newStoreGatewayZoneService($.store_gateway_zone_c_statefulset),

  store_gateway_zone_a_backup_service: if !$._config.multi_zone_store_gateway_zone_a_backup_enabled then null else
    $.newStoreGatewayZoneService($.store_gateway_zone_a_backup_statefulset),

  store_gateway_zone_b_backup_service: if !$._config.multi_zone_store_gateway_zone_b_backup_enabled then null else
    $.newStoreGatewayZoneService($.store_gateway_zone_b_backup_statefulset),

  // Create a service backed by all store-gateway replicas (in all zone).
  // This service is used to access the store-gateway admin UI.
  store_gateway_multi_zone_service: if !$._config.multi_zone_store_gateway_enabled then null else
    local name = 'store-gateway-multi-zone';
    local labels = { 'rollout-group': 'store-gateway' };
    local ports = [
      servicePort.newNamed(name='store-gateway-http-metrics', port=80, targetPort=80) +
      servicePort.withProtocol('TCP'),
    ];

    service.new(name, labels, ports) +
    service.mixin.metadata.withLabels({ name: name }),

  store_gateway_rollout_pdb:
    if !$._config.multi_zone_store_gateway_enabled then null else
      (
        if $._config.multi_zone_store_gateway_zpdb_enabled then
          $.newZPDB('store-gateway-rollout', 'store-gateway', $._config.multi_zone_store_gateway_zpdb_max_unavailable)
        else
          podDisruptionBudget.new('store-gateway-rollout') +
          podDisruptionBudget.mixin.spec.withMaxUnavailable(1)
      )
      + podDisruptionBudget.mixin.metadata.withLabels({ name: 'store-gateway-rollout' })
      + podDisruptionBudget.mixin.spec.selector.withMatchLabels({ 'rollout-group': 'store-gateway' }),

  // Ensure all configured addresses are zonal ones.
  local storeGatewayZoneAConfigError = if isZoneAEnabled then $.validateMimirMultiZoneConfig(['store_gateway_zone_a_statefulset']) else null,
  assert storeGatewayZoneAConfigError == null : storeGatewayZoneAConfigError,

  local storeGatewayZoneBConfigError = if isZoneBEnabled then $.validateMimirMultiZoneConfig(['store_gateway_zone_b_statefulset']) else null,
  assert storeGatewayZoneBConfigError == null : storeGatewayZoneBConfigError,

  local storeGatewayZoneCConfigError = if isZoneCEnabled then $.validateMimirMultiZoneConfig(['store_gateway_zone_c_statefulset']) else null,
  assert storeGatewayZoneCConfigError == null : storeGatewayZoneCConfigError,

  local storeGatewayZoneABackupConfigError = if isMultiAZAEnabled then $.validateMimirMultiZoneConfig(['store_gateway_zone_a_backup_statefulset']) else null,
  assert storeGatewayZoneABackupConfigError == null : storeGatewayZoneABackupConfigError,

  local storeGatewayZoneBBackupConfigError = if isMultiAZBEnabled then $.validateMimirMultiZoneConfig(['store_gateway_zone_b_backup_statefulset']) else null,
  assert storeGatewayZoneBBackupConfigError == null : storeGatewayZoneBBackupConfigError,

  //
  // Single-zone store-gateways shouldn't be configured when multi-zone is enabled.
  //

  store_gateway_statefulset:
    // Remove the default store-gateway StatefulSet if multi-zone is enabled and no migration is in progress.
    if ($._config.multi_zone_store_gateway_enabled && !$._config.multi_zone_store_gateway_migration_enabled)
    then null
    else super.store_gateway_statefulset,

  store_gateway_service:
    // Remove the default store-gateway service if multi-zone is enabled and no migration is in progress.
    if ($._config.multi_zone_store_gateway_enabled && !$._config.multi_zone_store_gateway_migration_enabled)
    then null
    else super.store_gateway_service,

  store_gateway_pdb:
    // Remove the default store-gateway PodDisruptionBudget if multi-zone is enabled and no migration is in progress.
    if ($._config.multi_zone_store_gateway_enabled && !$._config.multi_zone_store_gateway_migration_enabled)
    then null
    else super.store_gateway_pdb,
}
