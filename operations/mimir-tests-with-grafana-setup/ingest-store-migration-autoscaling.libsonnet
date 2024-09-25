// This file exists to support migration of existing Mimir cell with "classic" architecture with ingester downscaling, HPA and limits-operator queries to ingest-storage.
{
  _config+:: {
    // Enable scaling annotations in ingester-zone-[abc]-partition.
    ingest_storage_ingester_migration_autoscaling_ingester_annotations_partition_zone_a_enabled: false,
    ingest_storage_ingester_migration_autoscaling_ingester_annotations_partition_zone_b_enabled: false,
    ingest_storage_ingester_migration_autoscaling_ingester_annotations_partition_zone_c_enabled: false,

    // Enable scaling annotations in ingester-zone-[abc].
    ingest_storage_ingester_migration_autoscaling_ingester_annotations_ingester_zone_a_enabled: false,
    ingest_storage_ingester_migration_autoscaling_ingester_annotations_ingester_zone_b_enabled: false,
    ingest_storage_ingester_migration_autoscaling_ingester_annotations_ingester_zone_c_enabled: false,

    // Primary zone has ingesters that will control the read-only state of partitions.
    ingest_storage_ingester_migration_autoscaling_ingester_primary_zone: 'ingester-zone-a-partition',

    // When set to true, ingester downscaling v2 annotations are removed from ingester-zone-[abc] statefulsets.
    ingest_storage_ingester_migration_classic_ingesters_remove_downscaling_annotations: false,

    // When set to true, ingester-zone-[abc] are scaled down to 0.
    ingest_storage_ingester_migration_classic_ingesters_scale_down: false,
  },

  local enabled = $._config.ingest_storage_ingester_migration_autoscaling_ingester_annotations_partition_zone_a_enabled ||
                  $._config.ingest_storage_ingester_migration_autoscaling_ingester_annotations_partition_zone_b_enabled ||
                  $._config.ingest_storage_ingester_migration_autoscaling_ingester_annotations_partition_zone_c_enabled ||
                  $._config.ingest_storage_ingester_migration_autoscaling_ingester_annotations_ingester_zone_a_enabled ||
                  $._config.ingest_storage_ingester_migration_autoscaling_ingester_annotations_ingester_zone_b_enabled ||
                  $._config.ingest_storage_ingester_migration_autoscaling_ingester_annotations_ingester_zone_c_enabled,

  // Validate the configuration.
  assert !enabled || !$._config.ingest_storage_ingester_autoscaling_enabled
         : 'ingest_storage_migration_autoscaling_ingester_annotations_*_enabled is only supported when ingest_storage_ingester_autoscaling_enabled is not enabled in namespace %s' % $._config.namespace,

  assert !enabled || $._config.multi_zone_ingester_enabled
         : 'ingest_storage_migration_autoscaling_ingester_annotations_*_enabled is only supported with multi-zone ingesters in namespace %s' % $._config.namespace,

  //  assert !enabled || $._config.ingester_automated_downscale_v2_enabled
  //         : 'ingest_storage_ingester_migration_autoscaling_ingester_annotations_enabled is only supported if ingester_automated_downscale_v2_enabled is enabled in namespace %s' % $._config.namespace,

  assert !enabled || $.rollout_operator_deployment != null
         : 'ingest_storage_ingester_migration_autoscaling_ingester_annotations_*_enabled requires rollout-operator in namespace %s' % $._config.namespace,
}
{
  // Utility used to override a field only if exists in super.
  local overrideSuperIfExists(name, override) = if !( name in super) || super[name] == null || super[name] == {} then null else
    super[name] + override,

  local removeIngesterDownscalingAnnotations(enabled, statefulsetName) = overrideSuperIfExists(
    statefulsetName,
    local statefulSet = $.apps.v1.statefulSet;

    if !enabled then {} else (
      // Remove scaling labels and annotations.
      statefulSet.mixin.metadata.withLabels({}) +
      statefulSet.mixin.metadata.withAnnotations({})
    )
  ),

  ingester_zone_a_statefulset: removeIngesterDownscalingAnnotations($._config.ingest_storage_ingester_migration_classic_ingesters_remove_downscaling_annotations, 'ingester_zone_a_statefulset'),
  ingester_zone_b_statefulset: removeIngesterDownscalingAnnotations($._config.ingest_storage_ingester_migration_classic_ingesters_remove_downscaling_annotations, 'ingester_zone_b_statefulset'),
  ingester_zone_c_statefulset: removeIngesterDownscalingAnnotations($._config.ingest_storage_ingester_migration_classic_ingesters_remove_downscaling_annotations, 'ingester_zone_c_statefulset'),
}
{
  // Utility used to override a field only if exists in super.
  local overrideSuperIfExists(name, override) = if !( name in super) || super[name] == null || super[name] == {} then null else
    super[name] + override,

  local scaleDownIngestersToZero(enabled, statefulsetName) = overrideSuperIfExists(
    statefulsetName,
    local statefulSet = $.apps.v1.statefulSet;
    if !enabled then {} else (
      {
        spec+: {
          replicas::: 0,  // This funky syntax makes hidden field visible again.
        },
      }
    )
  ),

  ingester_zone_a_statefulset: scaleDownIngestersToZero($._config.ingest_storage_ingester_migration_classic_ingesters_scale_down, 'ingester_zone_a_statefulset'),
  ingester_zone_b_statefulset: scaleDownIngestersToZero($._config.ingest_storage_ingester_migration_classic_ingesters_scale_down, 'ingester_zone_b_statefulset'),
  ingester_zone_c_statefulset: scaleDownIngestersToZero($._config.ingest_storage_ingester_migration_classic_ingesters_scale_down, 'ingester_zone_c_statefulset'),
}
{
  // Utility used to override a field only if exists in super.
  local overrideSuperIfExists(name, override) = if !( name in super) || super[name] == null || super[name] == {} then null else
    super[name] + override,

  local updateIngesterZone(check, statefulsetObject, zone, replicaTemplate) = overrideSuperIfExists(
    statefulsetObject,
    if !check then {} else (
      local statefulSet = $.apps.v1.statefulSet;

      $.removeReplicasFromSpec +
      statefulSet.mixin.metadata.withLabelsMixin({
        'grafana.com/prepare-downscale': 'true',
        'grafana.com/min-time-between-zones-downscale': '0',  // Follower zones should adjust their number of instances based on leader zone immediately.
        'rollout-group': 'ingester',
      }) +
      statefulSet.mixin.metadata.withAnnotationsMixin(
        {
          'grafana.com/prepare-downscale-http-path': 'ingester/prepare-shutdown',  // We want to tell ingesters that they are shutting down.
          'grafana.com/prepare-downscale-http-port': '80',
          'rollout-max-unavailable': std.toString($._config.multi_zone_ingester_max_unavailable),
        } + (
          if zone == $._config.ingest_storage_ingester_migration_autoscaling_ingester_primary_zone then {
            'grafana.com/rollout-mirror-replicas-from-resource-name': replicaTemplate.metadata.name,
            'grafana.com/rollout-mirror-replicas-from-resource-kind': replicaTemplate.kind,
            'grafana.com/rollout-mirror-replicas-from-resource-api-version': replicaTemplate.apiVersion,
            'grafana.com/rollout-delayed-downscale': $._config.ingester_automated_downscale_v2_delay,
            'grafana.com/rollout-prepare-delayed-downscale-url': 'http://pod/ingester/prepare-partition-downscale',
          } else {
            'grafana.com/rollout-downscale-leader': $._config.ingest_storage_ingester_migration_autoscaling_ingester_primary_zone,
          }
        )
      )
    )
  ),

  ingester_partition_zone_a_statefulset: updateIngesterZone($._config.ingest_storage_ingester_migration_autoscaling_ingester_annotations_partition_zone_a_enabled, 'ingester_partition_zone_a_statefulset', 'ingester-zone-a-partition', $.ingester_zone_a_replica_template),
  ingester_partition_zone_b_statefulset: updateIngesterZone($._config.ingest_storage_ingester_migration_autoscaling_ingester_annotations_partition_zone_b_enabled, 'ingester_partition_zone_b_statefulset', 'ingester-zone-b-partition', $.ingester_zone_a_replica_template),
  ingester_partition_zone_c_statefulset: updateIngesterZone($._config.ingest_storage_ingester_migration_autoscaling_ingester_annotations_partition_zone_c_enabled, 'ingester_partition_zone_c_statefulset', 'ingester-zone-c-partition', $.ingester_zone_a_replica_template),

  ingester_zone_a_statefulset: updateIngesterZone($._config.ingest_storage_ingester_migration_autoscaling_ingester_annotations_ingester_zone_a_enabled, 'ingester_zone_a_statefulset', 'ingester-zone-a', $.ingester_zone_a_replica_template),
  ingester_zone_b_statefulset: updateIngesterZone($._config.ingest_storage_ingester_migration_autoscaling_ingester_annotations_ingester_zone_b_enabled, 'ingester_zone_b_statefulset', 'ingester-zone-b', $.ingester_zone_a_replica_template),
  ingester_zone_c_statefulset: updateIngesterZone($._config.ingest_storage_ingester_migration_autoscaling_ingester_annotations_ingester_zone_c_enabled, 'ingester_zone_c_statefulset', 'ingester-zone-c', $.ingester_zone_a_replica_template),
}
