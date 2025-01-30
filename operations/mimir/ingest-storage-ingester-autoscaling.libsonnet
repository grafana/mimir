{
  _config+:: {
    // Enables ingesters autoscaling when ingest storage is enabled.
    ingest_storage_ingester_autoscaling_enabled: false,

    // The name of the ingester StatefulSet running the leader zone. Other zones will follow its scaling.
    ingest_storage_ingester_autoscaling_primary_zone: 'ingester-zone-a',

    // Total number of min/max replicas across all zones.
    ingest_storage_ingester_autoscaling_min_replicas_per_zone: error 'you must set ingest_storage_ingester_autoscaling_min_replicas_per_zone in the _config in namespace %s' % $._config.namespace,
    ingest_storage_ingester_autoscaling_max_replicas_per_zone: error 'you must set ingest_storage_ingester_autoscaling_max_replicas_per_zone in the _config in namespace %s' % $._config.namespace,

    // The target number of active series per ingester.
    ingest_storage_ingester_autoscaling_active_series_threshold: 1500000,

    // How long to wait before terminating an ingester after it has been notified about the scale down.
    ingest_storage_ingester_downscale_delay: if 'querier.query-ingesters-within' in $.querier_args then
      $.querier_args['querier.query-ingesters-within']
    else
      // The default -querier.query-ingesters-within in Mimir is 13 hours.
      '13h',

    // Allow to fine-tune which components gets deployed. This is useful when following the procedure
    // to rollout ingesters autoscaling with no downtime.
    ingest_storage_ingester_autoscaling_ingester_annotations_enabled: $._config.ingest_storage_ingester_autoscaling_enabled,

    // Make label selector in ReplicaTemplate configurable. This mostly doesn't matter, but from our experience if the selector
    // doesn't match correct pods, HPA in GKE will display wrong usage in "kubectl describe hpa".
    ingest_storage_replica_template_label_selector: 'name=ingester-zone-a',

    // Make triggers configurable so that we can add more. Each object needs to have: query, threshold, metric_type.
    ingest_storage_ingester_autoscaling_triggers: [
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
          primary_zone: $._config.ingest_storage_ingester_autoscaling_primary_zone,
        },
        threshold: std.toString($._config.ingest_storage_ingester_autoscaling_active_series_threshold),
        metric_type: 'AverageValue',  // HPA will compute desired replicas as "<query result> / <threshold>".
      },
    ],

    // When set to false, metrics used by scaling triggers are only indexed if there is more than 1 trigger.
    // When set to true, they are always indexed.
    ingest_storage_ingester_autoscaling_index_metrics: false,

    // Default stabilization windows for scaling up and down. This is the minimum time it takes for scaling event to start,
    // rate and impact of scaling events is further limited by policies.
    // Stabilization windows cannot be larger than 1 hour.
    // https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/#stabilization-window
    ingest_storage_ingester_autoscaling_scale_up_stabilization_window_seconds: $.util.parseDuration('30m'),
    ingest_storage_ingester_autoscaling_scale_down_stabilization_window_seconds: $.util.parseDuration('1h'),
  },

  // Validate the configuration.
  assert !$._config.ingest_storage_ingester_autoscaling_enabled || $._config.multi_zone_ingester_enabled : 'partitions ingester autoscaling is only supported with multi-zone ingesters in namespace %s' % $._config.namespace,
  assert !$._config.ingest_storage_ingester_autoscaling_ingester_annotations_enabled || $._config.multi_zone_ingester_replicas <= 0 : 'partitions ingester autoscaling and multi_zone_ingester_replicas are mutually exclusive in namespace %s' % $._config.namespace,
  assert !$._config.ingest_storage_ingester_autoscaling_ingester_annotations_enabled || !$._config.ingester_automated_downscale_enabled : 'partitions ingester autoscaling and ingester automated downscale config are mutually exclusive in namespace %s' % $._config.namespace,
  assert !$._config.ingest_storage_ingester_autoscaling_ingester_annotations_enabled || !$._config.ingester_automated_downscale_v2_enabled : 'partitions ingester autoscaling and ingester automated downscale v2 config are mutually exclusive in namespace %s' % $._config.namespace,
  assert !$._config.ingest_storage_ingester_autoscaling_enabled || $.rollout_operator_deployment != null : 'partitions ingester autoscaling requires rollout-operator in namespace %s' % $._config.namespace,

  // Create resource that will be targetted by ScaledObject.
  ingester_primary_zone_replica_template: if !$._config.ingest_storage_ingester_autoscaling_enabled then null else $.replicaTemplate($._config.ingest_storage_ingester_autoscaling_primary_zone, replicas=-1, label_selector=$._config.ingest_storage_replica_template_label_selector),

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
          'grafana.com/prepare-downscale-http-port': '%(server_http_port)s' % $._config,
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

  newPartitionsPrimaryIngesterZoneScaledObject(primaryZone, min_replicas, max_replicas, triggers, targetResource, indexMetrics)::
    local formatTrigger(i, trigger, addIndexSuffix=false) = {
      query: trigger.query,
      metric_name: if addIndexSuffix then
        'cortex_%s_replicas_hpa_%s_%d' % [std.strReplace(primaryZone, '-', '_'), std.strReplace($._config.namespace, '-', '_'), i]
      else
        'cortex_%s_replicas_hpa_%s' % [std.strReplace(primaryZone, '-', '_'), std.strReplace($._config.namespace, '-', '_')]
      ,
      threshold: trigger.threshold,
      metric_type: trigger.metric_type,
      ignore_null_values: false,  // Report error if query returns no value.
    };

    local scaledObjectConfig = {
      min_replica_count: min_replicas,
      max_replica_count: max_replicas,
      triggers: std.mapWithIndex(function(ix, tr) formatTrigger(ix, tr, indexMetrics || std.length(triggers) > 1), triggers),
    };

    self.newScaledObject(primaryZone, $._config.namespace, scaledObjectConfig, apiVersion=targetResource.apiVersion, kind=targetResource.kind) +
    {
      spec+: {
        advanced: {
          horizontalPodAutoscalerConfig: {
            behavior: {
              // Allow 100% upscaling of pods every 10 minutes to the min value of desired replicas over the stabilization window.
              scaleUp: {
                policies: [
                  {
                    type: 'Percent',
                    value: 100,
                    periodSeconds: $.util.parseDuration('10m'),
                  },
                ],
                selectPolicy: 'Min',  // This would only have effect if there were multiple policies.
                stabilizationWindowSeconds: $._config.ingest_storage_ingester_autoscaling_scale_up_stabilization_window_seconds,
              },
              // Allow 10% downscaling of pods every 30 minutes to the max value of desired replicas over the stabilization window.
              scaleDown: {
                policies: [{
                  type: 'Percent',
                  value: 10,
                  periodSeconds: $.util.parseDuration('30m'),
                }],
                selectPolicy: 'Max',  // This would only have effect if there were multiple policies.
                stabilizationWindowSeconds: $._config.ingest_storage_ingester_autoscaling_scale_down_stabilization_window_seconds,
              },
            },
          },
        },
      },
    },

  ingest_storage_ingester_primary_zone_scaling: if !$._config.ingest_storage_ingester_autoscaling_enabled then null else
    $.newPartitionsPrimaryIngesterZoneScaledObject(
      $._config.ingest_storage_ingester_autoscaling_primary_zone,
      $._config.ingest_storage_ingester_autoscaling_min_replicas_per_zone,
      $._config.ingest_storage_ingester_autoscaling_max_replicas_per_zone,
      $._config.ingest_storage_ingester_autoscaling_triggers,
      $.ingester_primary_zone_replica_template,
      $._config.ingest_storage_ingester_autoscaling_index_metrics,
    ),

  // Utility used to override a field only if exists in super.
  local overrideSuperIfExists(name, override) = if !( name in super) || super[name] == null || super[name] == {} then null else
    super[name] + override,
}
