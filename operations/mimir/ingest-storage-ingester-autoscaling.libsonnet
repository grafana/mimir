{
  _config+:: {
    // Enables ingesters autoscaling when ingest storage is enabled.
    ingest_storage_ingester_autoscaling_enabled: false,

    // The name of the ingester StatefulSet running the leader zone. Other zones will follow its scaling.
    ingest_storage_ingester_autoscaling_primary_zone: 'ingester-zone-a',

    // Total number of min/max replicas across all zones.
    ingest_storage_ingester_autoscaling_min_replicas: error 'you must set ingest_storage_ingester_autoscaling_min_replicas in the _config in namespace %s' % $._config.namespace,
    ingest_storage_ingester_autoscaling_max_replicas: error 'you must set ingest_storage_ingester_autoscaling_max_replicas in the _config in namespace %s' % $._config.namespace,

    // The target number of active series per ingester.
    ingest_storage_ingester_autoscaling_active_series_threshold: 1500000,

    // How many zones ingesters have been deployed to.
    ingest_storage_ingester_zones: 3,

    // How long to wait before terminating an ingester after it has been notified about the scale down.
    ingest_storage_ingester_downscale_delay: if 'querier.query-ingesters-within' in $.querier_args then
      $.querier_args['querier.query-ingesters-within']
    else
      // The default -querier.query-ingesters-within in Mimir is 13 hours.
      '13h',

    // Allow to fine-tune which components gets deployed. This is useful when following the procedure
    // to rollout ingesters autoscaling with no downtime.
    ingest_storage_ingester_autoscaling_ingester_annotations_enabled: $._config.ingest_storage_ingester_autoscaling_enabled,
    ingest_storage_ingester_autoscaling_replica_template_custom_resource_definition_enabled: $._config.ingest_storage_ingester_autoscaling_enabled,
  },

  // Validate the configuration.
  assert !$._config.ingest_storage_ingester_autoscaling_enabled || $._config.multi_zone_ingester_enabled : 'partitions ingester autoscaling is only supported with multi-zone ingesters in namespace %s' % $._config.namespace,
  assert !$._config.ingest_storage_ingester_autoscaling_ingester_annotations_enabled || $._config.multi_zone_ingester_replicas <= 0 : 'partitions ingester autoscaling and multi_zone_ingester_replicas are mutually exclusive in namespace %s' % $._config.namespace,
  assert !$._config.ingest_storage_ingester_autoscaling_ingester_annotations_enabled || !$._config.ingester_automated_downscale_enabled : 'partitions ingester autoscaling and ingester automated downscale config are mutually exclusive in namespace %s' % $._config.namespace,
  assert !$._config.ingest_storage_ingester_autoscaling_enabled || $.rollout_operator_deployment != null : 'partitions ingester autoscaling requires rollout-operator in namespace %s' % $._config.namespace,

  //
  // ReplicaTemplate
  //

  replica_template:: std.parseYaml(importstr 'replica-templates.yaml'),
  replica_template_custom_resource: if !$._config.ingest_storage_ingester_autoscaling_replica_template_custom_resource_definition_enabled then null else $.replica_template,

  replicaTemplate(name):: {
    apiVersion: 'rollout-operator.grafana.com/v1',
    kind: 'ReplicaTemplate',
    metadata: {
      name: name,
      namespace: $._config.namespace,
    },
    spec: {
      replicas:: null,  // We don't want this field to be managed by Flux.
      labelSelector: 'name=unused',  // HPA requires that label selector exists and is valid, but it will not be used for target type of AverageValue.
    },
  },

  // Create resource that will be targetted by ScaledObject.
  ingester_primary_zone_replica_template: if !$._config.ingest_storage_ingester_autoscaling_enabled then null else $.replicaTemplate($._config.ingest_storage_ingester_autoscaling_primary_zone),

  //
  // Configure prepare-shutdown endpoint in all ingesters.
  //

  local updateIngesterZone(zone) = overrideSuperIfExists(
    '%s_statefulset' % std.strReplace(zone, '-', '_'),
    if !$._config.ingest_storage_ingester_autoscaling_ingester_annotations_enabled then {} else (
      local statefulSet = $.apps.v1.statefulSet;

      $.removeReplicasFromSpec +
      statefulSet.mixin.metadata.withLabelsMixin({
        'grafana.com/prepare-downscale': 'true',
        'grafana.com/min-time-between-zones-downscale': '0',  // Follower zones should adjust their number of instances based on leader zone immediately.
      }) +
      statefulSet.mixin.metadata.withAnnotationsMixin(
        {
          'grafana.com/prepare-downscale-http-path': 'ingester/prepare-shutdown',  // We want to tell ingesters that they are shutting down.
          'grafana.com/prepare-downscale-http-port': '80',
        } + (
          if zone == $._config.ingest_storage_ingester_autoscaling_primary_zone then {
            'grafana.com/rollout-mirror-replicas-from-resource-name': $._config.ingest_storage_ingester_autoscaling_primary_zone,
            'grafana.com/rollout-mirror-replicas-from-resource-kind': $.ingester_primary_zone_replica_template.kind,
            'grafana.com/rollout-mirror-replicas-from-resource-api-version': $.ingester_primary_zone_replica_template.apiVersion,
            'grafana.com/rollout-delayed-downscale': $._config.ingest_storage_ingester_downscale_delay,
            'grafana.com/rollout-prepare-delayed-downscale-url': 'http://pod/ingester/prepare-partition-downscale',
          } else {
            'grafana.com/rollout-downscale-leader': $._config.ingest_storage_ingester_autoscaling_primary_zone,
          }
        )
      )
    )
  ),

  ingester_zone_a_statefulset: updateIngesterZone('ingester-zone-a'),
  ingester_zone_b_statefulset: updateIngesterZone('ingester-zone-b'),
  ingester_zone_c_statefulset: updateIngesterZone('ingester-zone-c'),

  //
  // Define a ScaledObject for primary zone.
  //

  newPartitionsPrimaryIngesterZoneScaledObject(primaryZone, min_replicas, max_replicas, active_series_threshold, targetResource)::
    local scaledObjectConfig = {
      min_replica_count: min_replicas,
      max_replica_count: max_replicas,
      triggers: [
        {
          // Target ingesters in primary zone, ignoring read-write mode ingesters.
          local sum_series_from_ready_pods = |||
            sum(
                sum by (pod) (cortex_ingester_owned_series{cluster="%(cluster)s", job="%(namespace)s/%(primary_zone)s", container="ingester"})
                and
                sum by (pod) (kube_pod_status_ready{cluster="%(cluster)s",namespace="%(namespace)s", pod=~"%(primary_zone)s.*", condition="true"}) > 0
            )
          |||,

          local ratio_of_ready_pods_vs_all_replicas = |||
            (
                sum(kube_pod_status_ready{cluster="%(cluster)s", namespace="%(namespace)s", pod=~"%(primary_zone)s.*", condition="true"})
                /
                scalar(kube_statefulset_status_replicas{cluster="%(cluster)s", namespace="%(namespace)s", statefulset="%(primary_zone)s"})
            )
          |||,

          query: ('(' + sum_series_from_ready_pods + ' / ' + ratio_of_ready_pods_vs_all_replicas + ') '
                  + ' and (' + ratio_of_ready_pods_vs_all_replicas + ' > 0.7)') % {
            cluster: $._config.cluster,
            namespace: $._config.namespace,
            primary_zone: primaryZone,
          },
          metric_name: 'cortex_%s_replicas_hpa_%s' % [std.strReplace(primaryZone, '-', '_'), std.strReplace($._config.namespace, '-', '_')],
          threshold: std.toString(active_series_threshold),
          metric_type: 'AverageValue',  // HPA will compute desired replicas as "<query result> / <threshold>".
          ignore_null_values: false,  // Report error if query returns no value.
        },
      ],
    };

    self.newScaledObject(primaryZone, $._config.namespace, scaledObjectConfig, apiVersion=targetResource.apiVersion, kind=targetResource.kind) +
    {
      spec+: {
        advanced: {
          horizontalPodAutoscalerConfig: {
            behavior: {
              // Allow 100% upscaling of pods if scale up is indicated for 10 minutes to see effects of scaling faster.
              // Use a lookback period of 30 minutes, which will only upscale to the min(requestedReplicas) over that period
              scaleUp: {
                policies: [
                  {
                    type: 'Percent',
                    value: 100,
                    periodSeconds: $.util.parseDuration('10m'),
                  },
                ],
                selectPolicy: 'Min',
                stabilizationWindowSeconds: $.util.parseDuration('30m'),
              },
              // Allow 10% downscaling of pods if scale down is indicated for 30 minutes.
              // Use a lookback period of 60 minutes, which will only downscale to the max(requestedReplicas) over that period
              scaleDown: {
                policies: [{
                  type: 'Percent',
                  value: 10,
                  periodSeconds: $.util.parseDuration('30m'),
                }],
                selectPolicy: 'Max',
                stabilizationWindowSeconds: $.util.parseDuration('1h'),  // We can't go higher than 1h.,
              },
            },
          },
        },
      },
    },

  ingest_storage_ingester_primary_zone_scaling: if !$._config.ingest_storage_ingester_autoscaling_enabled then null else
    $.newPartitionsPrimaryIngesterZoneScaledObject(
      $._config.ingest_storage_ingester_autoscaling_primary_zone,
      std.ceil($._config.ingest_storage_ingester_autoscaling_min_replicas / $._config.ingest_storage_ingester_zones),
      std.ceil($._config.ingest_storage_ingester_autoscaling_max_replicas / $._config.ingest_storage_ingester_zones),
      $._config.ingest_storage_ingester_autoscaling_active_series_threshold,
      $.ingester_primary_zone_replica_template
    ),

  //
  // Grant extra privileges to rollout-operator.
  //

  local role = $.rbac.v1.role,
  local policyRule = $.rbac.v1.policyRule,
  rollout_operator_role: overrideSuperIfExists(
    'rollout_operator_role',
    if !$._config.ingest_storage_ingester_autoscaling_enabled then {} else
      role.withRulesMixin([
        policyRule.withApiGroups($.replica_template.spec.group) +
        policyRule.withResources(['%s/scale' % $.replica_template.spec.names.plural, '%s/status' % $.replica_template.spec.names.plural]) +
        policyRule.withVerbs(['get', 'patch']),
      ])
  ),

  // Utility used to override a field only if exists in super.
  local overrideSuperIfExists(name, override) = if !( name in super) || super[name] == null || super[name] == {} then null else
    super[name] + override,
}
