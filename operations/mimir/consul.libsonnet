local consul = import 'consul/consul.libsonnet';

{
  _config+:: {
    consul_replicas: 1,
    other_namespaces+: [],
  },

  consul: consul {
    _config+:: {
      consul_replicas: $._config.consul_replicas,
      namespace: $._config.namespace,
    },

    // Snapshot the raft.db very frequently, to stop it getting to big.
    consul_config+:: {
      raft_snapshot_threshold: 128,
      raft_trailing_logs: 10e3,
    },

    local container = $.core.v1.container,

    consul_container+::
      container.withArgsMixin([
        '-ui-content-path=/%s/consul/' % $._config.namespace,
      ]) +
      $.util.resourcesRequests('4', '4Gi'),

    local deployment = $.apps.v1.deployment,
    local podAntiAffinity = deployment.mixin.spec.template.spec.affinity.podAntiAffinity,
    local volume = $.core.v1.volume,

    // backwards compatibility with ksonnet
    local podAffinityTerm =
      if std.objectHasAll($.core.v1, 'podAffinityTerm')
      then $.core.v1.podAffinityTerm
      else podAntiAffinity.requiredDuringSchedulingIgnoredDuringExecutionType,

    consul_deployment+:

      // Keep the consul state on a ramdisk, as they are ephemeral to us.
      $.util.emptyVolumeMount(
        'data',
        '/consul/data/',
        volumeMixin=volume.mixin.emptyDir.withMedium('Memory'),
      ) +

      // Ensure Consul is not scheduled on the same host as an ingester
      // (in any namespace - hence other_namespaces).
      podAntiAffinity.withRequiredDuringSchedulingIgnoredDuringExecutionMixin([
        podAffinityTerm.mixin.labelSelector.withMatchLabels({ name: 'ingester' }) +
        podAffinityTerm.withNamespaces([$._config.namespace] + $._config.other_namespaces) +
        podAffinityTerm.withTopologyKey('kubernetes.io/hostname'),
      ]) +

      $.util.podPriority('high'),

    // Don't healthcheck services, adds load to consul.
    consul_exporter+::
      container.withArgsMixin([
        '--no-consul.health-summary',
        '--consul.allow_stale',
      ]),
  },
}
