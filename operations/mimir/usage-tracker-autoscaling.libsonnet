{
  local keda = (import 'keda-libsonnet/main.libsonnet').keda.v1alpha1,
  local scaledObject = keda.scaledObject,
  local scaleDownBehavior = scaledObject.spec.advanced.horizontalPodAutoscalerConfig.behavior.scaleDown,
  local scaleUpBehavior = scaledObject.spec.advanced.horizontalPodAutoscalerConfig.behavior.scaleUp,
  local scalingModifiers = scaledObject.spec.advanced.scalingModifiers,

  _config+:: {
    // When enabled, a KEDA ScaledObject scales the zone-a ReplicaTemplate, and zone-b mirrors its replica count.
    usage_tracker_hpa_enabled: false,
    usage_tracker_hpa_min_replicas_per_zone: 1,
    usage_tracker_hpa_max_replicas_per_zone: 64,

    usage_tracker_hpa_scale_down_stabilization_seconds: 900,  // 15m
    usage_tracker_hpa_scale_up_stabilization_seconds: 300,  // 5m
    usage_tracker_hpa_scale_down_period_seconds: 600,  // 10m
    usage_tracker_hpa_scale_up_period_seconds: 600,  // 10m

    // Delay KEDA's first trigger evaluation after ScaledObject creation. Without it, the first
    // evaluation can scale the usage-tracker down to the min replica count. Set to 0 to disable.
    usage_tracker_hpa_initial_cooldown_period_seconds: 8 * 60,  // 8m

    // Resource utilization floors: the HPA keeps the power-of-2 replica count that keeps the CPU
    // and memory utilization below these targets.
    usage_tracker_hpa_cpu_floor_target_utilization: 0.9,
    usage_tracker_hpa_memory_floor_target_utilization: 0.5,
  },

  assert !$._config.usage_tracker_hpa_enabled ||
         std.pow(2, std.floor(std.log($._config.usage_tracker_hpa_min_replicas_per_zone) / std.log(2))) == $._config.usage_tracker_hpa_min_replicas_per_zone :
         'usage_tracker_hpa_min_replicas_per_zone must be a power of 2, got %d' % $._config.usage_tracker_hpa_min_replicas_per_zone,

  assert !$._config.usage_tracker_hpa_enabled ||
         std.pow(2, std.floor(std.log($._config.usage_tracker_hpa_max_replicas_per_zone) / std.log(2))) == $._config.usage_tracker_hpa_max_replicas_per_zone :
         'usage_tracker_hpa_max_replicas_per_zone must be a power of 2, got %d' % $._config.usage_tracker_hpa_max_replicas_per_zone,

  assert !$._config.usage_tracker_hpa_enabled ||
         $._config.usage_tracker_hpa_max_replicas_per_zone <= $._config.usage_tracker_partitions :
         'usage_tracker_hpa_max_replicas_per_zone (%d) must not exceed usage_tracker_partitions (%d)' %
         [$._config.usage_tracker_hpa_max_replicas_per_zone, $._config.usage_tracker_partitions],

  local isEnabled = $._config.usage_tracker_enabled && $._config.usage_tracker_hpa_enabled,

  local prometheusHeaders = if $._config.autoscaling_prometheus_tenant != '' then 'X-Scope-OrgID=%s' % $._config.autoscaling_prometheus_tenant else '',

  // desired power-of-2 replicas from CPU utilization. It uses the CPU of all zones, because when a
  // zone is down its requests fail over to the other zone.
  local cpuFloorQuery = |||
    2 ^ ceil(
      ln(
        clamp_min(
          sum(rate(container_cpu_usage_seconds_total{container="usage-tracker", namespace="%(namespace)s", pod=~"usage-tracker-zone-[a-z].*"}[5m]))
          / (
            min(kube_pod_container_resource_requests{resource="cpu", container="usage-tracker", namespace="%(namespace)s", pod=~"usage-tracker-zone-[a-z].*"})
            * %(cpu_floor_target_utilization)s
          ),
          1
        )
      ) / scalar(ln(vector(2)))
    )
  ||| % {
    namespace: $._config.namespace,
    cpu_floor_target_utilization: '%g' % $._config.usage_tracker_hpa_cpu_floor_target_utilization,
  },

  // desired power-of-2 replicas from memory utilization.
  local memoryFloorQuery = |||
    2 ^ ceil(
      ln(
        clamp_min(
          sum(container_memory_working_set_bytes{container="usage-tracker", namespace="%(namespace)s", pod=~"usage-tracker-zone-a.*"})
          / (
            min(kube_pod_container_resource_requests{resource="memory", container="usage-tracker", namespace="%(namespace)s", pod=~"usage-tracker-zone-a.*"})
            * %(memory_floor_target_utilization)s
          ),
          1
        )
      ) / scalar(ln(vector(2)))
    )
  ||| % {
    namespace: $._config.namespace,
    memory_floor_target_utilization: '%g' % $._config.usage_tracker_hpa_memory_floor_target_utilization,
  },

  // Scale-down by halving (50%) and scale-up by doubling (100%) to guarantee
  // intermediate replica counts remain powers of 2 during transitions.
  local scaleDownPolicy = {
    type: 'Percent',
    value: 50,
    periodSeconds: $._config.usage_tracker_hpa_scale_down_period_seconds,
  },

  local scaleUpPolicy = {
    type: 'Percent',
    value: 100,
    periodSeconds: $._config.usage_tracker_hpa_scale_up_period_seconds,
  },

  local prometheusTrigger(name, metricName, query) = {
    name: name,
    type: 'prometheus',
    metadata: {
      serverAddress: $._config.autoscaling_prometheus_url,
      [if prometheusHeaders != '' then 'customHeaders']: prometheusHeaders,
      query: query,
      metricName: metricName,
      threshold: '1',
      // Return an error when the query has no result, instead of returning 0.
      ignoreNullValues: 'false',
    },
    metricType: 'AverageValue',
  },

  local newUsageTrackerScaledObject(zone, targetResource) =
    local metricPrefix = 'usage_tracker_%s_%s' % [std.strReplace(zone, '-', '_'), std.strReplace($._config.namespace, '-', '_')];

    scaledObject.new(targetResource.metadata.name)
    + scaledObject.spec.scaleTargetRef.withApiVersion(targetResource.apiVersion)
    + scaledObject.spec.scaleTargetRef.withKind(targetResource.kind)
    + scaledObject.spec.scaleTargetRef.withName(targetResource.metadata.name)
    + (if $._config.usage_tracker_hpa_initial_cooldown_period_seconds > 0 then
         scaledObject.spec.withInitialCooldownPeriod($._config.usage_tracker_hpa_initial_cooldown_period_seconds)
       else {})
    + scaledObject.spec.withMinReplicaCount($._config.usage_tracker_hpa_min_replicas_per_zone)
    + scaledObject.spec.withMaxReplicaCount($._config.usage_tracker_hpa_max_replicas_per_zone)
    + scaledObject.spec.withTriggers([
      prometheusTrigger('cpu_floor', '%s_cpu_floor' % metricPrefix, cpuFloorQuery),
      prometheusTrigger('memory_floor', '%s_memory_floor' % metricPrefix, memoryFloorQuery),
    ])
    + scalingModifiers.withFormula('max(cpu_floor, memory_floor)')
    + scalingModifiers.withTarget('1')
    + scaleDownBehavior.withStabilizationWindowSeconds($._config.usage_tracker_hpa_scale_down_stabilization_seconds)
    + scaleDownBehavior.withPolicies(scaleDownPolicy)
    + scaleUpBehavior.withStabilizationWindowSeconds($._config.usage_tracker_hpa_scale_up_stabilization_seconds)
    + scaleUpBehavior.withPolicies(scaleUpPolicy),

  // Utility used to override a field only if exists in super.
  local overrideSuperIfExists(name, override) = if !( name in super) || super[name] == null || super[name] == {} then null else
    super[name] + override,

  local statefulSet = $.apps.v1.statefulSet,

  // Remove replicas from zone-A ReplicaTemplate so the HPA can manage it.
  usage_tracker_zone_a_replica_template: overrideSuperIfExists(
    'usage_tracker_zone_a_replica_template',
    if isEnabled then $.removeReplicasFromSpec else {},
  ),

  // Remove the unused zone-B ReplicaTemplate when HPA is enabled:
  // zone-B's StatefulSet mirrors zone-A's ReplicaTemplate, so this object has no consumer.
  usage_tracker_zone_b_replica_template: if isEnabled then null else super.usage_tracker_zone_b_replica_template,

  // Point zone-B's StatefulSet at zone-A's ReplicaTemplate so the
  // rollout-operator propagates the HPA-managed replica count to zone-B.
  usage_tracker_zone_b_statefulset: overrideSuperIfExists(
    'usage_tracker_zone_b_statefulset',
    if isEnabled then
      statefulSet.mixin.metadata.withAnnotationsMixin({
        'grafana.com/rollout-mirror-replicas-from-resource-name': 'usage-tracker-zone-a',
      })
    else {},
  ),

  usage_tracker_zone_a_scaled_object: if !isEnabled then null else
    newUsageTrackerScaledObject('usage-tracker-zone-a', $.usage_tracker_zone_a_replica_template),
}
