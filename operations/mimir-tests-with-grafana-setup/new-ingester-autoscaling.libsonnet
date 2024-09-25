{
  // TODO -- describe what this does

  _config+:: {
    new_ingester_hpa_enabled: false,
    new_ingester_autoscale_enabled: false,

    // Target max number of owned series per ingester pod.
    new_ingester_autoscaling_owned_series_threshold: 2e6,

    // Total number of min/max replicas across all zones.
    // For the static trigger we use a min of multi_zone_ingester_replicas and a max of 2x multi_zone_ingester_replicas.
    // For autoscaling these must be set in the cell's _config.
    new_ingester_autoscaling_min_replicas: if !$._config.new_ingester_autoscale_enabled then ($._config.multi_zone_ingester_replicas) else error 'you must set new_ingester_autoscaling_min_replicas in the _config',
    new_ingester_autoscaling_max_replicas: if !$._config.new_ingester_autoscale_enabled then (2 * $._config.multi_zone_ingester_replicas) else error 'you must set new_ingester_autoscaling_max_replicas in the _config',

    new_ingester_hpa_static_trigger: {
      query: 'mimir_config_multi_zone_ingester_replicas_per_zone{namespace="%s"}' % [$._config.namespace],
      threshold: '1',
      metric_type: 'AverageValue',  // HPA will compute desired replicas as "<query result> / <threshold>".
    },

    new_ingester_hpa_autoscaling_trigger: {
      // To account for owned series skew, we consider the most heavily loaded pod.
      query: 'max(sum by(pod) (cortex_ingester_owned_series{cluster="%s", job="%s/ingester-zone-a", container="ingester"}))' % [$._config.cluster, $._config.namespace],
      threshold: std.toString($._config.new_ingester_autoscaling_owned_series_threshold),
      // as metric_type is 'Value', desired pods will be computed as (metric_reading / threshold) * curr_pods
      metric_type: 'Value',
    },

    // Scaling triggers to pass to the scaledObject
    new_ingester_hpa_triggers: [
      if $._config.new_ingester_autoscale_enabled then $._config.new_ingester_hpa_autoscaling_trigger else $._config.new_ingester_hpa_static_trigger,
    ],
  },

  // Validate the configuration.
  // TODO -- require ingester_automated_downscale_v2_enabled in all zones instead? does per-zone still make sense?
  assert !$._config.new_ingester_hpa_enabled || $._config.ingester_automated_downscale_v2_enabled : 'new ingester hpa requires ingester_automated_downscale_v2_enabled in namespace %s' % $._config.namespace,
  assert !$._config.new_ingester_autoscale_enabled || $._config.new_ingester_hpa_enabled : 'new ingester autoscaling requires new_ingester_hpa_enabled in namespace %s' % $._config.namespace,

  // Utility used to override a field only if exists in super.
  local overrideSuperIfExists(name, override) = if !( name in super) || super[name] == null || super[name] == {} then null else
    super[name] + override,

  // Remove the replicas field from the replicaTemplate so the HPA can manage it
  ingester_zone_a_replica_template: overrideSuperIfExists(
    'ingester_zone_a_replica_template',
    if $._config.new_ingester_hpa_enabled then $.removeReplicasFromSpec else {}
  ),

  newIngesterZoneScaledObject(zone, targetResource)::
    local formatTrigger(i, trigger) = {
      query: trigger.query,
      metric_name: 'cortex_%s_replicas_hpa_%s_%d' % [std.strReplace(zone, '-', '_'), std.strReplace($._config.namespace, '-', '_'), i],
      threshold: trigger.threshold,
      metric_type: trigger.metric_type,
      ignore_null_values: false,  // Report error if query returns no value.
    };

    local scaledObjectConfig = {
      min_replica_count: std.ceil($._config.new_ingester_autoscaling_min_replicas / 3),
      max_replica_count: std.ceil($._config.new_ingester_autoscaling_max_replicas / 3),
      triggers: std.mapWithIndex(formatTrigger, $._config.new_ingester_hpa_triggers),
    };

    self.newScaledObject(zone, $._config.namespace, scaledObjectConfig, apiVersion=targetResource.apiVersion, kind=targetResource.kind) +
    {
      spec+: {
        advanced: {
          horizontalPodAutoscalerConfig: {
            behavior: {
              // Allow 100% upscaling of pods every 10 minutes.
              // Use a lookback period of 10 minutes, which will only upscale to the min(requestedReplicas) over that period
              scaleUp: {
                policies: [
                  {
                    type: 'Percent',
                    value: 100,
                    periodSeconds: $.util.parseDuration('10m'),
                  },
                ],
                selectPolicy: 'Min',
                stabilizationWindowSeconds: $.util.parseDuration('10m'),
              },
              // Allow 10% downscaling of pods every 30 minutes
              // Use a lookback period of 60 minutes, which will only downscale to the max(requestedReplicas) over that period
              scaleDown: {
                policies: [{
                  type: 'Percent',
                  value: 10,
                  periodSeconds: $.util.parseDuration('30m'),
                }],
                selectPolicy: 'Max',
                stabilizationWindowSeconds: $.util.parseDuration('1h'),
              },
            },
          },
        },
      },
    },

  ingester_zone_a_scaled_object: if !$._config.new_ingester_hpa_enabled then null else
    $.newIngesterZoneScaledObject('ingester-zone-a', $.ingester_zone_a_replica_template),
}
