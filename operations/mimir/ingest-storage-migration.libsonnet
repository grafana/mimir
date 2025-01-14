{
  local statefulSet = $.apps.v1.statefulSet,

  _config+:: {
    // Controls whether ingester-zone-X-partition should be deployed even if ingest storage is not enabled globally.
    ingest_storage_migration_partition_ingester_zone_a_enabled: false,
    ingest_storage_migration_partition_ingester_zone_b_enabled: false,
    ingest_storage_migration_partition_ingester_zone_c_enabled: false,

    ingest_storage_migration_partition_ingester_zone_a_replicas: 1,
    ingest_storage_migration_partition_ingester_zone_b_replicas: 1,
    ingest_storage_migration_partition_ingester_zone_c_replicas: 1,

    // Controls whether write path components should tee writes both to classic ingesters and Kafka.
    ingest_storage_migration_write_to_partition_ingesters_enabled: false,
    ingest_storage_migration_write_to_classic_ingesters_enabled: false,

    // Controls whether reads path components should read from partition ingesters instead of classic ones.
    ingest_storage_migration_querier_enabled: false,

    // Controls the procedure to decommission classic ingesters.
    ingest_storage_migration_classic_ingesters_no_scale_down_delay: false,
    ingest_storage_migration_classic_ingesters_scale_down: false,
    ingest_storage_migration_classic_ingesters_zone_a_decommission: false,
    ingest_storage_migration_classic_ingesters_zone_b_decommission: false,
    ingest_storage_migration_classic_ingesters_zone_c_decommission: false,

    // Controls the procedure to decommission partition ingesters (to rename them back like classic ones).
    ingest_storage_migration_partition_ingester_zone_a_scale_down: false,
    ingest_storage_migration_partition_ingester_zone_b_scale_down: false,
    ingest_storage_migration_partition_ingester_zone_c_scale_down: false,
  },

  //
  // Partition ingesters (temporarily deployment used during the migration).
  //
  // Notes:
  // - To keep jsonnet changes easier, classic and partition ingesters share the same rollout-group. This means that,
  //   from the rollout-operator perspective, classic and partition ingesters are the same group and their StatefulSets
  //   get rolled out sequentially (even if classic and partition ingesters could be rolled out concurrently).
  // - No PodDisruptionBudget for partition ingesters because "ingester-rollout" PDB also covers partition ingesters,
  //   since the rollout-group is shared between the two.
  //

  local partitionIngesterArgs(zone) =
    // Explicitly include all the ingest storage config because during the migration the ingest storage
    // may not be enabled globally yet.
    $.ingest_storage_args +
    $.ingest_storage_kafka_consumer_args +
    $.ingest_storage_ingester_args +
    $.ingest_storage_ingester_ring_client_args + {
      // Run partition ingesters on a dedicated hash ring, so that they don't clash with classic ingesters.
      'ingester.ring.prefix': $._config.ingest_storage_ingester_instance_ring_dedicated_prefix,

      // Customize the consume group so that it will match the expected one when we'll migrate the temporarily
      // ingester-zone-[abc]-partition back to ingester-zone-[abc].
      'ingest-storage.kafka.consumer-group': 'ingester-zone-%s-<partition>' % zone,
    },

  local partitionIngesterStatefulSetLabelsAndAnnotations =
    // Let the rollout-operator know that it must call the prepare-shutdown endpoint before
    // scaling down partition ingesters. This is used to remove ingesters from the ingester and
    // partition rings when scaling down because migrated back to "ingester-zone-[abc]" StatefulSet
    // at the end of the migration.
    statefulSet.mixin.metadata.withLabelsMixin({
      'grafana.com/prepare-downscale': 'true',
    }) +
    statefulSet.mixin.metadata.withAnnotationsMixin({
      'grafana.com/prepare-downscale-http-path': 'ingester/prepare-shutdown',
      'grafana.com/prepare-downscale-http-port': '%(server_http_port)s' % $._config,
    }),

  local partitionIngesterStatefulSetPolicies =
    // We must guarantee that the PVCs are retained by the partition ingesters because during the migration
    // we have a step during which the partition ingesters StatefulSet is deleted (one zone at a time) and
    // PVCs need to be renamed, but their volumes preserved.
    statefulSet.spec.persistentVolumeClaimRetentionPolicy.withWhenScaled('Retain') +
    statefulSet.spec.persistentVolumeClaimRetentionPolicy.withWhenDeleted('Retain'),

  local gossipLabel = if !$._config.memberlist_ring_enabled then {} else
    $.apps.v1.statefulSet.spec.template.metadata.withLabelsMixin({ [$._config.gossip_member_label]: 'true' }),

  ingester_partition_zone_a_args:: $.ingester_zone_a_args + partitionIngesterArgs('a'),
  ingester_partition_zone_b_args:: $.ingester_zone_b_args + partitionIngesterArgs('b'),
  ingester_partition_zone_c_args:: $.ingester_zone_c_args + partitionIngesterArgs('c'),

  ingester_partition_zone_a_env_map:: $.ingester_zone_a_env_map,
  ingester_partition_zone_b_env_map:: $.ingester_zone_b_env_map,
  ingester_partition_zone_c_env_map:: $.ingester_zone_c_env_map,

  ingester_partition_zone_a_node_affinity_matchers:: $.ingester_zone_a_node_affinity_matchers,
  ingester_partition_zone_b_node_affinity_matchers:: $.ingester_zone_b_node_affinity_matchers,
  ingester_partition_zone_c_node_affinity_matchers:: $.ingester_zone_c_node_affinity_matchers,

  // Ingester-zone-a-partition.
  ingester_partition_zone_a_container:: if !$._config.ingest_storage_migration_partition_ingester_zone_a_enabled then null else
    self.newIngesterZoneContainer('a', $.ingester_partition_zone_a_args, $.ingester_partition_zone_a_env_map),

  ingester_partition_zone_a_statefulset: if !$._config.ingest_storage_migration_partition_ingester_zone_a_enabled then null else
    self.newIngesterZoneStatefulSet('a-partition', $.ingester_partition_zone_a_container, $.ingester_partition_zone_a_node_affinity_matchers) +
    statefulSet.mixin.spec.withReplicas($._config.ingest_storage_migration_partition_ingester_zone_a_replicas) +
    partitionIngesterStatefulSetLabelsAndAnnotations +
    partitionIngesterStatefulSetPolicies +
    (if !$._config.ingest_storage_migration_partition_ingester_zone_a_scale_down then {} else statefulSet.mixin.spec.withReplicas(0)),

  ingester_partition_zone_a_service: if !$._config.ingest_storage_migration_partition_ingester_zone_a_enabled then null else
    $.newIngesterZoneService($.ingester_partition_zone_a_statefulset),

  // Ingester-zone-b-partition.
  ingester_partition_zone_b_container:: if !$._config.ingest_storage_migration_partition_ingester_zone_b_enabled then null else
    self.newIngesterZoneContainer('b', $.ingester_partition_zone_b_args, $.ingester_partition_zone_b_env_map),

  ingester_partition_zone_b_statefulset: if !$._config.ingest_storage_migration_partition_ingester_zone_b_enabled then null else
    self.newIngesterZoneStatefulSet('b-partition', $.ingester_partition_zone_b_container, $.ingester_partition_zone_b_node_affinity_matchers) +
    statefulSet.mixin.spec.withReplicas($._config.ingest_storage_migration_partition_ingester_zone_b_replicas) +
    partitionIngesterStatefulSetLabelsAndAnnotations +
    partitionIngesterStatefulSetPolicies +
    (if !$._config.ingest_storage_migration_partition_ingester_zone_b_scale_down then {} else statefulSet.mixin.spec.withReplicas(0)),

  ingester_partition_zone_b_service: if !$._config.ingest_storage_migration_partition_ingester_zone_b_enabled then null else
    $.newIngesterZoneService($.ingester_partition_zone_b_statefulset),

  // Ingester-zone-c-partition.
  ingester_partition_zone_c_container:: if !$._config.ingest_storage_migration_partition_ingester_zone_c_enabled then null else
    self.newIngesterZoneContainer('c', $.ingester_partition_zone_c_args, $.ingester_partition_zone_c_env_map),

  ingester_partition_zone_c_statefulset: if !$._config.ingest_storage_migration_partition_ingester_zone_c_enabled then null else
    self.newIngesterZoneStatefulSet('c-partition', $.ingester_partition_zone_c_container, $.ingester_partition_zone_c_node_affinity_matchers) +
    statefulSet.mixin.spec.withReplicas($._config.ingest_storage_migration_partition_ingester_zone_c_replicas) +
    partitionIngesterStatefulSetLabelsAndAnnotations +
    partitionIngesterStatefulSetPolicies +
    (if !$._config.ingest_storage_migration_partition_ingester_zone_c_scale_down then {} else statefulSet.mixin.spec.withReplicas(0)),

  ingester_partition_zone_c_service: if !$._config.ingest_storage_migration_partition_ingester_zone_c_enabled then null else
    $.newIngesterZoneService($.ingester_partition_zone_c_statefulset),

  //
  // Write path (Kafka producers): distributor, ruler, aggregators.
  //

  local writeToPartitionIngestersArgs =
    // Explicitly include all the ingest storage config because during the migration the ingest storage
    // may not be enabled globally yet.
    $.ingest_storage_args +
    $.ingest_storage_kafka_producer_args +
    $.ingest_storage_ingester_ring_client_args,

  local writeToClassicIngestersArgs = {
    'ingest-storage.migration.distributor-send-to-ingesters-enabled': true,
  },

  distributor_args+::
    (if !$._config.ingest_storage_migration_write_to_partition_ingesters_enabled then {} else (writeToPartitionIngestersArgs + $.ingest_storage_distributor_args)) +
    (if !$._config.ingest_storage_migration_write_to_classic_ingesters_enabled then {} else writeToClassicIngestersArgs),

  ruler_args+::
    (if !$._config.ingest_storage_migration_write_to_partition_ingesters_enabled then {} else (writeToPartitionIngestersArgs + $.ingest_storage_ruler_args)) +
    (if !$._config.ingest_storage_migration_write_to_classic_ingesters_enabled then {} else writeToClassicIngestersArgs),

  //
  // Read path. Affected components:
  //
  // - Querier, ruler-querier: read from ingesters
  // - Query-frontend: fetch partition offsets to enforce strong read consistency
  //

  query_frontend_args+:: if !$._config.ingest_storage_migration_querier_enabled then {} else
    // Explicitly include all the ingest storage config because during the migration the ingest storage
    // may not be enabled globally yet.
    $.ingest_storage_args +
    $.ingest_storage_kafka_consumer_args +
    $.ingest_storage_query_frontend_args,

  querier_args+:: if !$._config.ingest_storage_migration_querier_enabled then {} else
    // Explicitly include all the ingest storage config because during the migration the ingest storage
    // may not be enabled globally yet.
    $.ingest_storage_args +
    $.ingest_storage_ingester_ring_client_args +
    // The following should only be configured on distributors and ingesters, but it's currently required to pass
    // config validation when ingest storage is enabled.
    // TODO remove once we've improved the config validation.
    $.ingest_storage_kafka_consumer_args + {
      // Run partition ingesters on a dedicated hash ring, so that they don't clash with classic ingesters.
      'ingester.ring.prefix': $._config.ingest_storage_ingester_instance_ring_dedicated_prefix,
    },

  //
  // Classic ingesters.
  //

  assert !$._config.ingest_storage_migration_classic_ingesters_scale_down || $._config.ingester_automated_downscale_enabled : 'ingest storage migration requires automated ingester downscaling to be enabled',

  ingester_zone_a_statefulset: overrideSuperIfExists(
    'ingester_zone_a_statefulset',
    if $._config.ingest_storage_migration_classic_ingesters_zone_a_decommission then
      null
    else (
      (
        if !$._config.ingest_storage_migration_classic_ingesters_no_scale_down_delay && !$._config.ingest_storage_migration_classic_ingesters_scale_down then {} else
          statefulSet.mixin.metadata.withLabelsMixin({
            'grafana.com/min-time-between-zones-downscale': '0',
          })
      ) + (
        if !$._config.ingest_storage_migration_classic_ingesters_scale_down then {} else
          statefulSet.mixin.spec.withReplicas(0)
      )
    ),
  ),

  ingester_zone_b_statefulset: overrideSuperIfExists(
    'ingester_zone_b_statefulset',
    if $._config.ingest_storage_migration_classic_ingesters_zone_b_decommission then
      null
    else if $._config.ingest_storage_migration_classic_ingesters_no_scale_down_delay || $._config.ingest_storage_migration_classic_ingesters_scale_down then
      statefulSet.mixin.metadata.withLabelsMixin({
        'grafana.com/min-time-between-zones-downscale': '0',
      })
    else
      {},
  ),

  ingester_zone_c_statefulset: overrideSuperIfExists(
    'ingester_zone_c_statefulset',
    if $._config.ingest_storage_migration_classic_ingesters_zone_c_decommission then
      null
    else if $._config.ingest_storage_migration_classic_ingesters_no_scale_down_delay || $._config.ingest_storage_migration_classic_ingesters_scale_down then
      statefulSet.mixin.metadata.withLabelsMixin({
        'grafana.com/min-time-between-zones-downscale': '0',
      })
    else
      {},
  ),

  ingester_zone_a_service: overrideSuperIfExists(
    'ingester_zone_a_service',
    if !$._config.ingest_storage_migration_classic_ingesters_zone_a_decommission then {} else null
  ),

  ingester_zone_b_service: overrideSuperIfExists(
    'ingester_zone_b_service',
    if !$._config.ingest_storage_migration_classic_ingesters_zone_b_decommission then {} else null
  ),

  ingester_zone_c_service: overrideSuperIfExists(
    'ingester_zone_c_service',
    if !$._config.ingest_storage_migration_classic_ingesters_zone_c_decommission then {} else null
  ),

  // Utility used to override a field only if exists in super.
  local overrideSuperIfExists(name, override) = if !( name in super) || super[name] == null || super[name] == {} then null else
    (if override == null then null else super[name] + override),
}

// Assert on required specs, to make sure they don't get overridden elsewhere. These assertions are
// executed at a later stage in the jsonnet evaluation, so they can detect overrides done in files
// imported after this one too.
{
  assert $.ingester_partition_zone_a_statefulset == null || $.ingester_partition_zone_a_statefulset.spec.persistentVolumeClaimRetentionPolicy.whenScaled == 'Retain' : 'persistentVolumeClaimRetentionPolicy.whenScaled must be set to Retain on ingester_partition_zone_a_statefulset',
  assert $.ingester_partition_zone_b_statefulset == null || $.ingester_partition_zone_b_statefulset.spec.persistentVolumeClaimRetentionPolicy.whenScaled == 'Retain' : 'persistentVolumeClaimRetentionPolicy.whenScaled must be set to Retain on ingester_partition_zone_b_statefulset',
  assert $.ingester_partition_zone_c_statefulset == null || $.ingester_partition_zone_c_statefulset.spec.persistentVolumeClaimRetentionPolicy.whenScaled == 'Retain' : 'persistentVolumeClaimRetentionPolicy.whenScaled must be set to Retain on ingester_partition_zone_c_statefulset',

  assert $.ingester_partition_zone_a_statefulset == null || $.ingester_partition_zone_a_statefulset.spec.persistentVolumeClaimRetentionPolicy.whenDeleted == 'Retain' : 'persistentVolumeClaimRetentionPolicy.whenDeleted must be set to Retain on ingester_partition_zone_a_statefulset',
  assert $.ingester_partition_zone_b_statefulset == null || $.ingester_partition_zone_b_statefulset.spec.persistentVolumeClaimRetentionPolicy.whenDeleted == 'Retain' : 'persistentVolumeClaimRetentionPolicy.whenDeleted must be set to Retain on ingester_partition_zone_b_statefulset',
  assert $.ingester_partition_zone_c_statefulset == null || $.ingester_partition_zone_c_statefulset.spec.persistentVolumeClaimRetentionPolicy.whenDeleted == 'Retain' : 'persistentVolumeClaimRetentionPolicy.whenDeleted must be set to Retain on ingester_partition_zone_c_statefulset',
}
