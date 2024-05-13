{
  local container = $.core.v1.container,
  local deployment = $.apps.v1.deployment,
  local statefulSet = $.apps.v1.statefulSet,
  local podDisruptionBudget = $.policy.v1.podDisruptionBudget,
  local volume = $.core.v1.volume,
  local service = $.core.v1.service,
  local servicePort = $.core.v1.servicePort,
  local podAntiAffinity = deployment.mixin.spec.template.spec.affinity.podAntiAffinity,

  _config+: {
    multi_zone_ingester_enabled: false,
    multi_zone_ingester_migration_enabled: false,
    multi_zone_ingester_replication_write_path_enabled: true,
    multi_zone_ingester_replication_read_path_enabled: true,
    multi_zone_ingester_replicas: 0,
    multi_zone_ingester_max_unavailable: 50,

    multi_zone_store_gateway_enabled: false,
    multi_zone_store_gateway_read_path_enabled: $._config.multi_zone_store_gateway_enabled,
    multi_zone_store_gateway_migration_enabled: false,
    multi_zone_store_gateway_replicas: 0,
    // When store-gateway lazy loading is disabled, store-gateway may take a long time to startup.
    // To speed up rollouts, we increase the max unavailable to rollout all store-gateways in a zone in a single batch.
    multi_zone_store_gateway_max_unavailable: if $._config.store_gateway_lazy_loading_enabled then 50 else 1000,

    // We can update the queryBlocksStorageConfig only once the migration is over. During the migration
    // we don't want to apply these changes to single-zone store-gateways too.
    queryBlocksStorageConfig+:: if !$._config.multi_zone_store_gateway_enabled || !$._config.multi_zone_store_gateway_read_path_enabled || $._config.multi_zone_store_gateway_migration_enabled then {} else {
      'store-gateway.sharding-ring.zone-awareness-enabled': 'true',
      'store-gateway.sharding-ring.prefix': 'multi-zone/',
    },
  },

  //
  // Zone-aware replication.
  //

  distributor_args+:: if !($._config.multi_zone_ingester_enabled && $._config.multi_zone_ingester_replication_write_path_enabled) then {} else {
    'ingester.ring.zone-awareness-enabled': 'true',
  },

  ruler_args+:: (
    if !($._config.multi_zone_ingester_enabled && $._config.multi_zone_ingester_replication_write_path_enabled) then {} else {
      'ingester.ring.zone-awareness-enabled': 'true',
    }
  ) + (
    // During the migration, if read path switch is enabled we need to apply changes directly to rulers instead of queryBlocksStorageConfig.
    if !($._config.multi_zone_store_gateway_enabled && $._config.multi_zone_store_gateway_read_path_enabled && $._config.multi_zone_store_gateway_migration_enabled) then {} else {
      'store-gateway.sharding-ring.zone-awareness-enabled': 'true',
      'store-gateway.sharding-ring.prefix': 'multi-zone/',
    }
  ),

  querier_args+:: (
    if !($._config.multi_zone_ingester_enabled && $._config.multi_zone_ingester_replication_read_path_enabled) then {} else {
      'ingester.ring.zone-awareness-enabled': 'true',
    }
  ) + (
    // During the migration, if read path switch is enabled we need to apply changes directly to queriers instead of queryBlocksStorageConfig.
    if !($._config.multi_zone_store_gateway_enabled && $._config.multi_zone_store_gateway_read_path_enabled && $._config.multi_zone_store_gateway_migration_enabled) then {} else {
      'store-gateway.sharding-ring.zone-awareness-enabled': 'true',
      'store-gateway.sharding-ring.prefix': 'multi-zone/',
    }
  ),

  //
  // Multi-zone ingesters.
  //

  local multi_zone_ingesters_deployed = $._config.is_microservices_deployment_mode && $._config.multi_zone_ingester_enabled,

  ingester_zone_a_args:: {},
  ingester_zone_b_args:: {},
  ingester_zone_c_args:: {},

  ingester_zone_a_env_map:: $.ingester_env_map,
  ingester_zone_b_env_map:: $.ingester_env_map,
  ingester_zone_c_env_map:: $.ingester_env_map,

  ingester_zone_a_node_affinity_matchers:: $.ingester_node_affinity_matchers,
  ingester_zone_b_node_affinity_matchers:: $.ingester_node_affinity_matchers,
  ingester_zone_c_node_affinity_matchers:: $.ingester_node_affinity_matchers,

  newIngesterZoneContainer(zone, zone_args, envmap={})::
    $.ingester_container +
    container.withArgs($.util.mapToFlags(
      $.ingester_args + zone_args + {
        'ingester.ring.zone-awareness-enabled': 'true',
        'ingester.ring.instance-availability-zone': 'zone-%s' % zone,
      },
    )) +
    (if std.length(envmap) > 0 then container.withEnvMap(std.prune(envmap)) else {}),

  newIngesterZoneStatefulSet(zone, container, nodeAffinityMatchers=[])::
    local name = 'ingester-zone-%s' % zone;

    self.newIngesterStatefulSet(name, container, withAntiAffinity=false, nodeAffinityMatchers=nodeAffinityMatchers) +
    statefulSet.mixin.metadata.withLabels({ 'rollout-group': 'ingester' }) +
    statefulSet.mixin.metadata.withAnnotations({ 'rollout-max-unavailable': std.toString($._config.multi_zone_ingester_max_unavailable) }) +
    statefulSet.mixin.spec.template.metadata.withLabels({ name: name, 'rollout-group': 'ingester' }) +
    statefulSet.mixin.spec.selector.withMatchLabels({ name: name, 'rollout-group': 'ingester' }) +
    statefulSet.mixin.spec.updateStrategy.withType('OnDelete') +
    statefulSet.mixin.spec.template.spec.withTerminationGracePeriodSeconds(1200) +
    statefulSet.mixin.spec.withReplicas(std.ceil($._config.multi_zone_ingester_replicas / 3)) +
    (if !std.isObject($._config.node_selector) then {} else statefulSet.mixin.spec.template.spec.withNodeSelectorMixin($._config.node_selector)) +
    if $._config.ingester_allow_multiple_replicas_on_same_node then {} else {
      spec+:
        // Allow to schedule 2+ ingesters in the same zone on the same node, but do not schedule 2+ ingesters in
        // different zones on the same node. In case of 1 node failure in the Kubernetes cluster, only ingesters
        // in 1 zone will be affected.
        podAntiAffinity.withRequiredDuringSchedulingIgnoredDuringExecution([
          podAntiAffinity.requiredDuringSchedulingIgnoredDuringExecutionType.new() +
          podAntiAffinity.requiredDuringSchedulingIgnoredDuringExecutionType.mixin.labelSelector.withMatchExpressions([
            { key: 'rollout-group', operator: 'In', values: ['ingester'] },
            { key: 'name', operator: 'NotIn', values: [name] },
          ]) +
          podAntiAffinity.requiredDuringSchedulingIgnoredDuringExecutionType.withTopologyKey('kubernetes.io/hostname'),
        ]).spec,
    },

  // Creates a headless service for the per-zone ingesters StatefulSet. We don't use it
  // but we need to create it anyway because it's responsible for the network identity of
  // the StatefulSet pods. For more information, see:
  // https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.25/#statefulset-v1-apps
  newIngesterZoneService(sts)::
    $.util.serviceFor(sts, $._config.service_ignored_labels) +
    service.mixin.spec.withClusterIp('None'),  // Headless.

  ingester_zone_a_container:: if !multi_zone_ingesters_deployed then null else
    self.newIngesterZoneContainer('a', $.ingester_zone_a_args, $.ingester_zone_a_env_map),

  ingester_zone_a_statefulset: if !multi_zone_ingesters_deployed then null else
    self.newIngesterZoneStatefulSet('a', $.ingester_zone_a_container, $.ingester_zone_a_node_affinity_matchers),

  ingester_zone_a_service: if !multi_zone_ingesters_deployed then null else
    $.newIngesterZoneService($.ingester_zone_a_statefulset),

  ingester_zone_b_container:: if !multi_zone_ingesters_deployed then null else
    self.newIngesterZoneContainer('b', $.ingester_zone_b_args, $.ingester_zone_b_env_map),

  ingester_zone_b_statefulset: if !multi_zone_ingesters_deployed then null else
    self.newIngesterZoneStatefulSet('b', $.ingester_zone_b_container, $.ingester_zone_b_node_affinity_matchers),

  ingester_zone_b_service: if !multi_zone_ingesters_deployed then null else
    $.newIngesterZoneService($.ingester_zone_b_statefulset),

  ingester_zone_c_container:: if !multi_zone_ingesters_deployed then null else
    self.newIngesterZoneContainer('c', $.ingester_zone_c_args, $.ingester_zone_c_env_map),

  ingester_zone_c_statefulset: if !multi_zone_ingesters_deployed then null else
    self.newIngesterZoneStatefulSet('c', $.ingester_zone_c_container, $.ingester_zone_c_node_affinity_matchers),

  ingester_zone_c_service: if !multi_zone_ingesters_deployed then null else
    $.newIngesterZoneService($.ingester_zone_c_statefulset),

  ingester_rollout_pdb: if !multi_zone_ingesters_deployed then null else
    podDisruptionBudget.new('ingester-rollout') +
    podDisruptionBudget.mixin.metadata.withLabels({ name: 'ingester-rollout' }) +
    podDisruptionBudget.mixin.spec.selector.withMatchLabels({ 'rollout-group': 'ingester' }) +
    podDisruptionBudget.mixin.spec.withMaxUnavailable(1),

  //
  // Single-zone ingesters shouldn't be configured when multi-zone is enabled.
  //

  ingester_statefulset:
    // Remove the default "ingester" StatefulSet if multi-zone is enabled and no migration is in progress.
    if !$._config.is_microservices_deployment_mode || ($._config.multi_zone_ingester_enabled && !$._config.multi_zone_ingester_migration_enabled)
    then null
    else super.ingester_statefulset,

  ingester_service:
    // Remove the default "ingester" service if multi-zone is enabled and no migration is in progress.
    if !$._config.is_microservices_deployment_mode || ($._config.multi_zone_ingester_enabled && !$._config.multi_zone_ingester_migration_enabled)
    then null
    else super.ingester_service,

  ingester_pdb:
    // There's no parent PDB if deployment mode is not microservices.
    if !$._config.is_microservices_deployment_mode
    then null
    // Keep it if multi-zone is disabled.
    else if !$._config.multi_zone_ingester_enabled
    then super.ingester_pdb
    // We don’t want Kubernetes to terminate any "ingester" StatefulSet's pod while migration is in progress.
    else if $._config.multi_zone_ingester_migration_enabled
    then super.ingester_pdb + podDisruptionBudget.mixin.spec.withMaxUnavailable(0)
    // Remove it if multi-zone is enabled and no migration is in progress.
    else null,

  //
  // Multi-zone store-gateways.
  //

  local multi_zone_store_gateways_deployed = $._config.is_microservices_deployment_mode && $._config.multi_zone_store_gateway_enabled,

  store_gateway_zone_a_args:: {},
  store_gateway_zone_b_args:: {},
  store_gateway_zone_c_args:: {},

  store_gateway_zone_a_env_map:: $.store_gateway_env_map,
  store_gateway_zone_b_env_map:: $.store_gateway_env_map,
  store_gateway_zone_c_env_map:: $.store_gateway_env_map,

  store_gateway_zone_a_node_affinity_matchers:: $.store_gateway_node_affinity_matchers,
  store_gateway_zone_b_node_affinity_matchers:: $.store_gateway_node_affinity_matchers,
  store_gateway_zone_c_node_affinity_matchers:: $.store_gateway_node_affinity_matchers,

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

  newStoreGatewayZoneStatefulSet(zone, container, nodeAffinityMatchers=[])::
    local name = 'store-gateway-zone-%s' % zone;

    self.newStoreGatewayStatefulSet(name, container, withAntiAffinity=false, nodeAffinityMatchers=nodeAffinityMatchers) +
    statefulSet.mixin.metadata.withLabels({ 'rollout-group': 'store-gateway' }) +
    statefulSet.mixin.metadata.withAnnotations({ 'rollout-max-unavailable': std.toString($._config.multi_zone_store_gateway_max_unavailable) }) +
    statefulSet.mixin.spec.template.metadata.withLabels({ name: name, 'rollout-group': 'store-gateway' }) +
    statefulSet.mixin.spec.selector.withMatchLabels({ name: name, 'rollout-group': 'store-gateway' }) +
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
            { key: 'rollout-group', operator: 'In', values: ['store-gateway'] },
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

  local nonRetainablePVCs = {
    _config+: {
      store_gateway_data_disk_class:
        if super.store_gateway_data_disk_class == 'fast' then 'fast-dont-retain'
        else super.store_gateway_data_disk_class,
    },
  },

  store_gateway_zone_a_container:: if !multi_zone_store_gateways_deployed then null else
    self.newStoreGatewayZoneContainer('a', $.store_gateway_zone_a_args, $.store_gateway_zone_a_env_map),

  store_gateway_zone_a_statefulset: if !multi_zone_store_gateways_deployed then null else
    (self + nonRetainablePVCs).newStoreGatewayZoneStatefulSet('a', $.store_gateway_zone_a_container, $.store_gateway_zone_a_node_affinity_matchers),

  store_gateway_zone_a_service: if !multi_zone_store_gateways_deployed then null else
    self.newStoreGatewayZoneService($.store_gateway_zone_a_statefulset),

  store_gateway_zone_b_container:: if !multi_zone_store_gateways_deployed then null else
    self.newStoreGatewayZoneContainer('b', $.store_gateway_zone_b_args, $.store_gateway_zone_b_env_map),

  store_gateway_zone_b_statefulset: if !multi_zone_store_gateways_deployed then null else
    (self + nonRetainablePVCs).newStoreGatewayZoneStatefulSet('b', $.store_gateway_zone_b_container, $.store_gateway_zone_b_node_affinity_matchers),

  store_gateway_zone_b_service: if !multi_zone_store_gateways_deployed then null else
    self.newStoreGatewayZoneService($.store_gateway_zone_b_statefulset),

  store_gateway_zone_c_container:: if !multi_zone_store_gateways_deployed then null else
    self.newStoreGatewayZoneContainer('c', $.store_gateway_zone_c_args, $.store_gateway_zone_c_env_map),

  store_gateway_zone_c_statefulset: if !multi_zone_store_gateways_deployed then null else
    (self + nonRetainablePVCs).newStoreGatewayZoneStatefulSet('c', $.store_gateway_zone_c_container, $.store_gateway_zone_c_node_affinity_matchers),

  store_gateway_zone_c_service: if !multi_zone_store_gateways_deployed then null else
    self.newStoreGatewayZoneService($.store_gateway_zone_c_statefulset),

  // Create a service backed by all store-gateway replicas (in all zone).
  // This service is used to access the store-gateway admin UI.
  store_gateway_multi_zone_service: if !multi_zone_store_gateways_deployed then null else
    local name = 'store-gateway-multi-zone';
    local labels = { 'rollout-group': 'store-gateway' };
    local ports = [
      servicePort.newNamed(name='store-gateway-http-metrics', port=80, targetPort=80) +
      servicePort.withProtocol('TCP'),
    ];

    service.new(name, labels, ports) +
    service.mixin.metadata.withLabels({ name: name }),

  store_gateway_rollout_pdb: if !multi_zone_store_gateways_deployed then null else
    podDisruptionBudget.new('store-gateway-rollout') +
    podDisruptionBudget.mixin.metadata.withLabels({ name: 'store-gateway-rollout' }) +
    podDisruptionBudget.mixin.spec.selector.withMatchLabels({ 'rollout-group': 'store-gateway' }) +
    podDisruptionBudget.mixin.spec.withMaxUnavailable(1),

  //
  // Single-zone store-gateways shouldn't be configured when multi-zone is enabled.
  //

  store_gateway_statefulset:
    // Remove the default store-gateway StatefulSet if multi-zone is enabled and no migration is in progress.
    if !$._config.is_microservices_deployment_mode || ($._config.multi_zone_store_gateway_enabled && !$._config.multi_zone_store_gateway_migration_enabled)
    then null
    else super.store_gateway_statefulset,

  store_gateway_service:
    // Remove the default store-gateway service if multi-zone is enabled and no migration is in progress.
    if !$._config.is_microservices_deployment_mode || ($._config.multi_zone_store_gateway_enabled && !$._config.multi_zone_store_gateway_migration_enabled)
    then null
    else super.store_gateway_service,

  store_gateway_pdb:
    // Remove the default store-gateway PodDisruptionBudget if multi-zone is enabled and no migration is in progress.
    if !$._config.is_microservices_deployment_mode || ($._config.multi_zone_store_gateway_enabled && !$._config.multi_zone_store_gateway_migration_enabled)
    then null
    else super.store_gateway_pdb,
}
