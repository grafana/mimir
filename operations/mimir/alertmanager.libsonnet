{
  local pvc = $.core.v1.persistentVolumeClaim,
  local volumeMount = $.core.v1.volumeMount,
  local volume = $.core.v1.volume,
  local container = $.core.v1.container,
  local statefulSet = $.apps.v1.statefulSet,
  local service = $.core.v1.service,
  local configMap = $.core.v1.configMap,

  local hasFallbackConfig = std.length($._config.alertmanager.fallback_config) > 0,

  alertmanager_args::
    $._config.grpcConfig +
    $._config.alertmanagerStorageClientConfig +
    {
      target: 'alertmanager',
      'runtime-config.file': '%s/overrides.yaml' % $._config.overrides_configmap_mountpoint,
      'alertmanager.storage.path': '/data',
      'alertmanager.web.external-url': '%s/alertmanager' % $._config.external_url,
      'server.http-listen-port': $._config.server_http_port,
      'alertmanager.sharding-ring.store': $._config.alertmanager.ring_store,
      'alertmanager.sharding-ring.consul.hostname': $._config.alertmanager.ring_hostname,
      'alertmanager.sharding-ring.replication-factor': $._config.alertmanager.ring_replication_factor,
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
      container.withPorts($.util.defaultPorts) +
      container.withEnvMixin([container.envType.fromFieldPath('POD_IP', 'status.podIP')]) +
      container.withArgsMixin(
        $.util.mapToFlags($.alertmanager_args)
      ) +
      container.withVolumeMountsMixin(
        [volumeMount.new('alertmanager-data', '/data')] +
        if hasFallbackConfig then
          [volumeMount.new('alertmanager-fallback-config', '/configs')]
        else []
      ) +
      $.util.resourcesRequests('2', '10Gi') +
      $.util.resourcesLimits(null, '15Gi') +
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
    if $._config.alertmanager_enabled then
      $.util.serviceFor($.alertmanager_statefulset, $._config.service_ignored_labels) +
      service.mixin.spec.withClusterIp('None')
    else {},
}
