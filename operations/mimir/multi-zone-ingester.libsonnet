{
  _config+:: {
    multi_zone_ingester_enabled: false,
    multi_zone_ingester_migration_enabled: false,
    multi_zone_ingester_replication_write_path_enabled: true,
    multi_zone_ingester_replication_read_path_enabled: true,
    multi_zone_ingester_replicas: 0,
    multi_zone_ingester_max_unavailable: 50,

    // this can be either a number or a percenage. ie 1 or 50%
    multi_zone_ingester_zpdb_max_unavailable: if $._config.ingest_storage_enabled then 1 else std.toString($._config.multi_zone_ingester_max_unavailable),

    // the regex to extract the ingester partition identifier from a pod name
    multi_zone_ingester_zpdb_partition_regex: if $._config.ingest_storage_enabled then '[a-z\\-]+-zone-[a-z]-([0-9]+)' else '',

    // the regex subexpression group number - only required if the above regular expression has more then 1 grouping
    multi_zone_ingester_zpdb_partition_group: 1,

    // Controls whether the multi (virtual) zone ingester should also be deployed multi-AZ.
    multi_zone_ingester_multi_az_enabled: false,
  },

  local container = $.core.v1.container,
  local statefulSet = $.apps.v1.statefulSet,
  local podDisruptionBudget = $.policy.v1.podDisruptionBudget,
  local service = $.core.v1.service,
  local podAntiAffinity = $.apps.v1.deployment.mixin.spec.template.spec.affinity.podAntiAffinity,

  local isMultiAZEnabled = $._config.multi_zone_ingester_multi_az_enabled,
  local isZoneAEnabled = isMultiAZEnabled && std.length($._config.multi_zone_availability_zones) >= 1,
  local isZoneBEnabled = isMultiAZEnabled && std.length($._config.multi_zone_availability_zones) >= 2,
  local isZoneCEnabled = isMultiAZEnabled && std.length($._config.multi_zone_availability_zones) >= 3,

  assert !isMultiAZEnabled || $._config.multi_zone_ingester_enabled : 'ingester multi-AZ deployment requires ingester multi-zone to be enabled',

  //
  // Zone-aware replication.
  //

  distributor_args+:: if !($._config.multi_zone_ingester_enabled && $._config.multi_zone_ingester_replication_write_path_enabled) then {} else {
    'ingester.ring.zone-awareness-enabled': 'true',
  },

  ruler_args+:: if !($._config.multi_zone_ingester_enabled && $._config.multi_zone_ingester_replication_write_path_enabled) then {} else {
    'ingester.ring.zone-awareness-enabled': 'true',
  },

  querier_args+:: if !($._config.multi_zone_ingester_enabled && $._config.multi_zone_ingester_replication_read_path_enabled) then {} else {
    'ingester.ring.zone-awareness-enabled': 'true',
  },

  //
  // Multi-zone ingesters.
  //

  ingester_zone_a_args:: {},
  ingester_zone_b_args:: {},
  ingester_zone_c_args:: {},

  ingester_zone_a_env_map:: $.ingester_env_map,
  ingester_zone_b_env_map:: $.ingester_env_map,
  ingester_zone_c_env_map:: $.ingester_env_map,

  ingester_zone_a_node_affinity_matchers:: $.ingester_node_affinity_matchers + (if isZoneAEnabled then [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[0])] else []),
  ingester_zone_b_node_affinity_matchers:: $.ingester_node_affinity_matchers + (if isZoneBEnabled then [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[1])] else []),
  ingester_zone_c_node_affinity_matchers:: $.ingester_node_affinity_matchers + (if isZoneCEnabled then [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_availability_zones[2])] else []),

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

    $.newIngesterStatefulSet(name, container, withAntiAffinity=false, nodeAffinityMatchers=nodeAffinityMatchers) +
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

  ingester_zone_a_container:: if !$._config.multi_zone_ingester_enabled then null else
    $.newIngesterZoneContainer('a', $.ingester_zone_a_args, $.ingester_zone_a_env_map),

  ingester_zone_a_statefulset: if !$._config.multi_zone_ingester_enabled then null else
    $.newIngesterZoneStatefulSet('a', $.ingester_zone_a_container, $.ingester_zone_a_node_affinity_matchers) +
    (if isZoneAEnabled then statefulSet.spec.template.spec.withTolerationsMixin($.newMimirMultiZoneToleration()) else {}),

  ingester_zone_a_service: if !$._config.multi_zone_ingester_enabled then null else
    $.newIngesterZoneService($.ingester_zone_a_statefulset),

  ingester_zone_b_container:: if !$._config.multi_zone_ingester_enabled then null else
    $.newIngesterZoneContainer('b', $.ingester_zone_b_args, $.ingester_zone_b_env_map),

  ingester_zone_b_statefulset: if !$._config.multi_zone_ingester_enabled then null else
    $.newIngesterZoneStatefulSet('b', $.ingester_zone_b_container, $.ingester_zone_b_node_affinity_matchers) +
    (if isZoneBEnabled then statefulSet.spec.template.spec.withTolerationsMixin($.newMimirMultiZoneToleration()) else {}),

  ingester_zone_b_service: if !$._config.multi_zone_ingester_enabled then null else
    $.newIngesterZoneService($.ingester_zone_b_statefulset),

  ingester_zone_c_container:: if !$._config.multi_zone_ingester_enabled then null else
    $.newIngesterZoneContainer('c', $.ingester_zone_c_args, $.ingester_zone_c_env_map),

  ingester_zone_c_statefulset: if !$._config.multi_zone_ingester_enabled then null else
    $.newIngesterZoneStatefulSet('c', $.ingester_zone_c_container, $.ingester_zone_c_node_affinity_matchers) +
    (if isZoneCEnabled then statefulSet.spec.template.spec.withTolerationsMixin($.newMimirMultiZoneToleration()) else {}),

  ingester_zone_c_service: if !$._config.multi_zone_ingester_enabled then null else
    $.newIngesterZoneService($.ingester_zone_c_statefulset),

  ingester_rollout_pdb: if !$._config.multi_zone_ingester_enabled then null else
    (
      if $._config.multi_zone_zpdb_enabled then
        $.newZPDB('ingester-rollout', 'ingester', $._config.multi_zone_ingester_zpdb_max_unavailable, $._config.multi_zone_ingester_zpdb_partition_regex, $._config.multi_zone_ingester_zpdb_partition_group)
      else
        podDisruptionBudget.new('ingester-rollout') +
        podDisruptionBudget.mixin.spec.withMaxUnavailable(1)
    )
    + podDisruptionBudget.mixin.metadata.withLabels({ name: 'ingester-rollout' })
    + podDisruptionBudget.mixin.spec.selector.withMatchLabels({ 'rollout-group': 'ingester' }),

  // Ensure all configured addresses are zonal ones.
  local ingesterZoneAConfigError = if isZoneAEnabled then $.validateMimirMultiZoneConfig(['ingester_zone_a_statefulset']) else null,
  assert ingesterZoneAConfigError == null : ingesterZoneAConfigError,

  local ingesterZoneBConfigError = if isZoneBEnabled then $.validateMimirMultiZoneConfig(['ingester_zone_b_statefulset']) else null,
  assert ingesterZoneBConfigError == null : ingesterZoneBConfigError,

  local ingesterZoneCConfigError = if isZoneCEnabled then $.validateMimirMultiZoneConfig(['ingester_zone_c_statefulset']) else null,
  assert ingesterZoneCConfigError == null : ingesterZoneCConfigError,

  //
  // Single-zone ingesters shouldn't be configured when multi-zone is enabled.
  //

  ingester_statefulset:
    // Remove the default "ingester" StatefulSet if multi-zone is enabled and no migration is in progress.
    if ($._config.multi_zone_ingester_enabled && !$._config.multi_zone_ingester_migration_enabled)
    then null
    else super.ingester_statefulset,

  ingester_service:
    // Remove the default "ingester" service if multi-zone is enabled and no migration is in progress.
    if ($._config.multi_zone_ingester_enabled && !$._config.multi_zone_ingester_migration_enabled)
    then null
    else super.ingester_service,

  ingester_pdb:
    // Keep it if multi-zone is disabled.
    if !$._config.multi_zone_ingester_enabled
    then super.ingester_pdb
    // We don't want Kubernetes to terminate any "ingester" StatefulSet's pod while migration is in progress.
    else if $._config.multi_zone_ingester_migration_enabled
    then super.ingester_pdb + podDisruptionBudget.mixin.spec.withMaxUnavailable(0)
    // Remove it if multi-zone is enabled and no migration is in progress.
    else null,
}
