{
  _config+:: {
    multi_zone_etcd_enabled: false,
  },

  // Enforcing the spread of etcd pods across multi-AZ is not easy because we're limited
  // by etcd-operator features. The etcd-operator doesn't support setting pod topology spread
  // constraints, so we can only leverage on pod affinity.
  //
  // Here we configure a preferred (but not required) anti-affinity rule to increase the likelihood
  // that different etcd pods will run in different AZs. The reason why we use "preferred"
  // instead of "required" is because if the number of etcd pods (e.g. 5) is greater than the
  // number of available AZs (e.g. 3) some pods will be never scheduled if we use "required".
  etcd: overrideSuperIfExists('etcd', if !$._config.multi_zone_etcd_enabled then {} else {
    local podAntiAffinity = $.apps.v1.deployment.mixin.spec.template.spec.affinity.podAntiAffinity,
    local weightedPodAffinityTerm = $.core.v1.weightedPodAffinityTerm,
    local deployment = $.apps.v1.deployment,

    spec+: {
      pod+: {
        affinity+: {
          podAntiAffinity+:
            podAntiAffinity.withPreferredDuringSchedulingIgnoredDuringExecution([
              weightedPodAffinityTerm.withWeight(100) +
              weightedPodAffinityTerm.podAffinityTerm.labelSelector.withMatchLabels({ etcd_cluster: 'etcd' }) +
              weightedPodAffinityTerm.podAffinityTerm.withTopologyKey('topology.kubernetes.io/zone'),
            ]).spec.template.spec.affinity.podAntiAffinity,
        },

        tolerations+:
          deployment.spec.template.spec.withTolerationsMixin([
            $.core.v1.toleration.withKey('topology') +
            $.core.v1.toleration.withOperator('Equal') +
            $.core.v1.toleration.withValue('multi-az') +
            $.core.v1.toleration.withEffect('NoSchedule'),
          ]).spec.template.spec.tolerations,
      },
    },
  }),


  // Utility used to override a field only if exists in super.
  local overrideSuperIfExists(name, override) = if !( name in super) || super[name] == null || super[name] == {} then null else
    super[name] + override,
}
