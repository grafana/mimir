// This file contains the experimental configuration to deploy distributors in multi-AZ.
{
  _config+:: {
    multi_zone_distributor_enabled: false,
    multi_zone_distributor_availability_zones: [],
    multi_zone_distributor_replicas: std.length($._config.multi_zone_distributor_availability_zones),
  },

  local container = $.core.v1.container,
  local deployment = $.apps.v1.deployment,
  local service = $.core.v1.service,

  local isMultiZoneEnabled = $._config.multi_zone_distributor_enabled,
  local isZoneAEnabled = isMultiZoneEnabled && std.length($._config.multi_zone_distributor_availability_zones) >= 1,
  local isZoneBEnabled = isMultiZoneEnabled && std.length($._config.multi_zone_distributor_availability_zones) >= 2,
  local isZoneCEnabled = isMultiZoneEnabled && std.length($._config.multi_zone_distributor_availability_zones) >= 3,

  local gossipLabel = if !$._config.memberlist_ring_enabled then {} else
    $.apps.v1.statefulSet.spec.template.metadata.withLabelsMixin({ [$._config.gossip_member_label]: 'true' }),

  distributor_zone_a_args:: $.distributor_args,
  distributor_zone_b_args:: $.distributor_args,
  distributor_zone_c_args:: $.distributor_args,

  distributor_zone_a_env_map:: {},
  distributor_zone_b_env_map:: {},
  distributor_zone_c_env_map:: {},

  distributor_zone_a_node_affinity_matchers:: $.distributor_node_affinity_matchers + [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_distributor_availability_zones[0])],
  distributor_zone_b_node_affinity_matchers:: $.distributor_node_affinity_matchers + [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_distributor_availability_zones[1])],
  distributor_zone_c_node_affinity_matchers:: $.distributor_node_affinity_matchers + [$.newMimirNodeAffinityMatcherAZ($._config.multi_zone_distributor_availability_zones[2])],

  distributor_zone_a_container:: if !isZoneAEnabled then null else
    $.newDistributorZoneContainer('a', $.distributor_zone_a_args, $.distributor_zone_a_env_map),

  distributor_zone_b_container:: if !isZoneBEnabled then null else
    $.newDistributorZoneContainer('b', $.distributor_zone_b_args, $.distributor_zone_b_env_map),

  distributor_zone_c_container:: if !isZoneCEnabled then null else
    $.newDistributorZoneContainer('c', $.distributor_zone_c_args, $.distributor_zone_c_env_map),

  distributor_zone_a_deployment: if !isZoneAEnabled then null else
    $.newDistributorZoneDeployment('a', $.distributor_zone_a_container, $.distributor_zone_a_node_affinity_matchers),

  distributor_zone_b_deployment: if !isZoneBEnabled then null else
    $.newDistributorZoneDeployment('b', $.distributor_zone_b_container, $.distributor_zone_b_node_affinity_matchers),

  distributor_zone_c_deployment: if !isZoneCEnabled then null else
    $.newDistributorZoneDeployment('c', $.distributor_zone_c_container, $.distributor_zone_c_node_affinity_matchers),

  distributor_zone_a_service: if !isZoneAEnabled then null else
    $.util.serviceFor($.distributor_zone_a_deployment, $._config.service_ignored_labels) +
    service.mixin.spec.withClusterIp('None'),

  distributor_zone_b_service: if !isZoneBEnabled then null else
    $.util.serviceFor($.distributor_zone_b_deployment, $._config.service_ignored_labels) +
    service.mixin.spec.withClusterIp('None'),

  distributor_zone_c_service: if !isZoneCEnabled then null else
    $.util.serviceFor($.distributor_zone_c_deployment, $._config.service_ignored_labels) +
    service.mixin.spec.withClusterIp('None'),

  distributor_zone_a_pdb: if !isZoneAEnabled then null else
    $.newMimirPdb('distributor-zone-a'),

  distributor_zone_b_pdb: if !isZoneBEnabled then null else
    $.newMimirPdb('distributor-zone-b'),

  distributor_zone_c_pdb: if !isZoneCEnabled then null else
    $.newMimirPdb('distributor-zone-c'),

  distributor_zone_a_scaled_object: if !isZoneAEnabled || !$._config.autoscaling_distributor_enabled then null else
    $.newDistributorScaledObject('distributor-zone-a', 'distributor-zone-a.*'),

  distributor_zone_b_scaled_object: if !isZoneBEnabled || !$._config.autoscaling_distributor_enabled then null else
    $.newDistributorScaledObject('distributor-zone-b', 'distributor-zone-b.*'),

  distributor_zone_c_scaled_object: if !isZoneCEnabled || !$._config.autoscaling_distributor_enabled then null else
    $.newDistributorScaledObject('distributor-zone-c', 'distributor-zone-c.*'),

  newDistributorZoneContainer(zone, args, extraEnvVarMap={})::
    $.distributor_container +
    container.withArgs($.util.mapToFlags(args)) +
    (if std.length(extraEnvVarMap) > 0 then container.withEnvMixin(std.prune(extraEnvVarMap)) else {}),

  newDistributorZoneDeployment(zone, container, nodeAffinityMatchers=[])::
    local name = 'distributor-zone-%s' % zone;

    $.newDistributorDeployment(name, container, nodeAffinityMatchers) +
    deployment.mixin.spec.withReplicas(std.ceil($._config.multi_zone_distributor_replicas / std.length($._config.multi_zone_distributor_availability_zones))) +
    deployment.spec.template.spec.withTolerationsMixin([
      $.core.v1.toleration.withKey('topology') +
      $.core.v1.toleration.withOperator('Equal') +
      $.core.v1.toleration.withValue('multi-az') +
      $.core.v1.toleration.withEffect('NoSchedule'),
    ]) +
    gossipLabel
    + (if !$._config.autoscaling_distributor_enabled then {} else $.removeReplicasFromSpec),

  newDistributorScaledObject(name, pod_regex)::
    $.newResourceScaledObject(
      name=name,
      container_name='distributor',
      cpu_requests=$.distributor_container.resources.requests.cpu,
      memory_requests=$.distributor_container.resources.requests.memory,
      min_replicas=$._config.autoscaling_distributor_min_replicas,
      max_replicas=$._config.autoscaling_distributor_max_replicas,
      cpu_target_utilization=$._config.autoscaling_distributor_cpu_target_utilization,
      memory_target_utilization=$._config.autoscaling_distributor_memory_target_utilization,
      with_cortex_prefix=true,
      with_ready_trigger=true,
      pod_regex=pod_regex,
    ) + (
      {
        spec+: {
          advanced: {
            horizontalPodAutoscalerConfig: {
              behavior: {
                scaleUp: {
                  // When multiple policies are specified the policy which allows the highest amount of change is the
                  // policy which is selected by default.
                  policies: [
                    {
                      // Allow to scale up at most 50% of pods every 2m. Every 2min is chosen as enough time for new
                      // pods to be handling load and counted in the 15min lookback window.
                      //
                      // This policy covers the case we already have a high number of pods running and adding +50%
                      // in the span of 2m means adding a significative number of pods.
                      type: 'Percent',
                      value: 50,
                      periodSeconds: 60 * 2,
                    },
                    {
                      // Allow to scale up at most 50% of pods every 2m. Every 2min is chosen as enough time for new
                      // pods to be handling load and counted in the 15min lookback window.
                      //
                      // This policy covers the case we currently have a small number of pods (e.g. < 10) and limiting
                      // the scaling by percentage may be too slow when scaling up.
                      type: 'Pods',
                      value: 15,
                      periodSeconds: 60 * 2,
                    },
                  ],
                  // After a scaleup we should wait at least 2 minutes to observe the effect.
                  stabilizationWindowSeconds: 60 * 2,
                },
                scaleDown: {
                  policies: [{
                    // Allow to scale down up to 10% of pods every 2m.
                    type: 'Percent',
                    value: 10,
                    periodSeconds: 120,
                  }],
                  // Reduce the likelihood of flapping replicas. When the metrics indicate that the target should be scaled
                  // down, HPA looks into previously computed desired states, and uses the highest value from the last 30m.
                  // This is particularly high for distributors due to their reasonably stable load and their long
                  // shutdown-delay + grace period.
                  stabilizationWindowSeconds: 60 * 30,
                },
              },
            },
          },
        },
      }
    ),

  // Remove single-zone deployment when multi-zone is enabled.
  distributor_deployment: if isMultiZoneEnabled then null else super.distributor_deployment,
  distributor_service: if isMultiZoneEnabled then null else super.distributor_service,
  distributor_pdb: if isMultiZoneEnabled then null else super.distributor_pdb,
  distributor_scaled_object: if isMultiZoneEnabled then null else super.distributor_scaled_object,
}
