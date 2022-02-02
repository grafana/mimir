{
  local pvc = $.core.v1.persistentVolumeClaim,
  local volumeMount = $.core.v1.volumeMount,
  local volume = $.core.v1.volume,
  local container = $.core.v1.container,
  local statefulSet = $.apps.v1.statefulSet,
  local service = $.core.v1.service,
  local configMap = $.core.v1.configMap,

  // The Alertmanager has three operational modes.
  local haType = if $._config.alertmanager.sharding_enabled then
    'sharding'
  else if $._config.alertmanager.replicas > 1 then
    'gossip_multi_replica'
  else
    'gossip_single_replica',
  // mode represents which operational mode the alertmanager runs in.
  // ports: array of container ports used for gossiping.
  // args: arguments that are eventually converted to flags on the container
  // flags: arguments directly added to the container. For legacy reasons, we need to use -- as a prefix for some flags.
  // service: the service definition
  local mode = {
    sharding: {
      ports: [],
      args: {
        'alertmanager.sharding-enabled': true,
        'alertmanager.sharding-ring.store': $._config.alertmanager.ring_store,
        'alertmanager.sharding-ring.consul.hostname': $._config.alertmanager.ring_hostname,
        'alertmanager.sharding-ring.replication-factor': $._config.alertmanager.ring_replication_factor,
      },
      flags: [],
      service:
        $.util.serviceFor($.alertmanager_statefulset, $._config.service_ignored_labels) +
        service.mixin.spec.withClusterIp('None'),
    },
    gossip_multi_replica: {
      ports: [
        $.core.v1.containerPort.newUDP('gossip-udp', $._config.alertmanager.gossip_port),
        $.core.v1.containerPort.new('gossip-tcp', $._config.alertmanager.gossip_port),
      ],
      args: {},
      flags: [
        '--alertmanager.cluster.listen-address=[$(POD_IP)]:%s' % $._config.alertmanager.gossip_port,
        '--alertmanager.cluster.peers=%s' % std.join(',', peers),
      ],
      service:
        $.util.serviceFor($.alertmanager_statefulset, $._config.service_ignored_labels) +
        service.mixin.spec.withClusterIp('None'),
    },
    gossip_single_replica: {
      ports: [],
      args: {},
      flags: ['--alertmanager.cluster.listen-address=""'],
      service: $.util.serviceFor($.alertmanager_statefulset, $._config.service_ignored_labels),
    },
  }[haType],
  local hasFallbackConfig = std.length($._config.alertmanager.fallback_config) > 0,
  local peers = [
    'alertmanager-%d.alertmanager.%s.svc.%s.local:%s' % [i, $._config.namespace, $._config.cluster, $._config.alertmanager.gossip_port]
    for i in std.range(0, $._config.alertmanager.replicas - 1)
  ],
  alertmanager_args::
    $._config.grpcConfig +
    $._config.alertmanagerStorageClientConfig +
    mode.args +
    {
      target: 'alertmanager',
      'runtime-config.file': '%s/overrides.yaml' % $._config.overrides_configmap_mountpoint,
      'alertmanager.enable-api': 'true',
      'alertmanager.storage.path': '/data',
      'alertmanager.web.external-url': '%s/alertmanager' % $._config.external_url,
      'server.http-listen-port': $._config.server_http_port,
    } +
    (if hasFallbackConfig then {
       'alertmanager.configs.fallback': '/configs/alertmanager_fallback_config.yaml',
     } else {}),

  alertmanager_fallback_config_map:
    if hasFallbackConfig then
      configMap.new('alertmanager-fallback-config') +
      configMap.withData({
        'alertmanager_fallback_config.yaml': $.util.manifestYaml($._config.alertmanager.fallback_config),
      })
    else {},


  alertmanager_pvc::
    if $._config.alertmanager_enabled then
      pvc.new() +
      pvc.mixin.metadata.withName('alertmanager-data') +
      pvc.mixin.spec.withAccessModes('ReadWriteOnce') +
      pvc.mixin.spec.resources.withRequests({ storage: '100Gi' })
    else {},

  alertmanager_container::
    if $._config.alertmanager_enabled then
      container.new('alertmanager', $._images.alertmanager) +
      container.withPorts($.util.defaultPorts + mode.ports) +
      container.withEnvMixin([container.envType.fromFieldPath('POD_IP', 'status.podIP')]) +
      container.withArgsMixin(
        $.util.mapToFlags($.alertmanager_args) +
        mode.flags
      ) +
      container.withVolumeMountsMixin(
        [volumeMount.new('alertmanager-data', '/data')] +
        if hasFallbackConfig then
          [volumeMount.new('alertmanager-fallback-config', '/configs')]
        else []
      ) +
      $.util.resourcesRequests('100m', '1Gi') +
      $.util.readinessProbe +
      $.jaeger_mixin
    else {},

  alertmanager_statefulset:
    if $._config.alertmanager_enabled then
      statefulSet.new('alertmanager', $._config.alertmanager.replicas, [$.alertmanager_container], $.alertmanager_pvc) +
      statefulSet.mixin.spec.withServiceName('alertmanager') +
      statefulSet.mixin.metadata.withNamespace($._config.namespace) +
      statefulSet.mixin.metadata.withLabels({ name: 'alertmanager' }) +
      statefulSet.mixin.spec.template.metadata.withLabels({ name: 'alertmanager' }) +
      statefulSet.mixin.spec.selector.withMatchLabels({ name: 'alertmanager' }) +
      statefulSet.mixin.spec.template.spec.securityContext.withRunAsUser(0) +
      statefulSet.mixin.spec.updateStrategy.withType('RollingUpdate') +
      statefulSet.mixin.spec.template.spec.withTerminationGracePeriodSeconds(900) +
      $.util.configVolumeMount($._config.overrides_configmap, $._config.overrides_configmap_mountpoint) +
      statefulSet.mixin.spec.template.spec.withVolumesMixin(
        if hasFallbackConfig then
          [volume.fromConfigMap('alertmanager-fallback-config', 'alertmanager-fallback-config')]
        else []
      )
    else {},

  alertmanager_service:
    if $._config.alertmanager_enabled then mode.service else {},
}
