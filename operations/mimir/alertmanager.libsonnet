{
  local configMap = $.core.v1.configMap,
  local container = $.core.v1.container,
  local pvc = $.core.v1.persistentVolumeClaim,
  local service = $.core.v1.service,
  local statefulSet = $.apps.v1.statefulSet,
  local volume = $.core.v1.volume,
  local volumeMount = $.core.v1.volumeMount,

  local hasFallbackConfig = std.length($._config.alertmanager.fallback_config) > 0,

  alertmanager_args::
    $._config.usageStatsConfig +
    $._config.grpcConfig +
    $._config.storageConfig +
    $._config.alertmanagerStorageConfig +
    {
      target: 'alertmanager',
      'alertmanager.storage.path': '/data',
      'alertmanager.web.external-url': '%s/alertmanager' % $._config.external_url,
      'server.http-listen-port': $._config.server_http_port,
      'alertmanager.sharding-ring.store': $._config.alertmanager.ring_store,
      'alertmanager.sharding-ring.consul.hostname': $._config.alertmanager.ring_hostname,
      'alertmanager.sharding-ring.replication-factor': $._config.alertmanager.ring_replication_factor,
    } +
    $.mimirRuntimeConfigFile +
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

  alertmanager_ports:: $.util.defaultPorts,

  alertmanager_container::
    if $._config.alertmanager_enabled then
      container.new('alertmanager', $._images.alertmanager) +
      container.withPorts($.alertmanager_ports) +
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
    if $._config.is_microservices_deployment_mode && $._config.alertmanager_enabled then
      $.newMimirStatefulSet('alertmanager', $._config.alertmanager.replicas, $.alertmanager_container, $.alertmanager_pvc, podManagementPolicy=null) +
      statefulSet.mixin.spec.template.spec.withTerminationGracePeriodSeconds(900) +
      $.mimirVolumeMounts +
      statefulSet.mixin.spec.template.spec.withVolumesMixin(
        if hasFallbackConfig then
          [volume.fromConfigMap('alertmanager-fallback-config', 'alertmanager-fallback-config')]
        else []
      )
    else {},

  alertmanager_service:
    if $._config.is_microservices_deployment_mode && $._config.alertmanager_enabled then
      $.util.serviceFor($.alertmanager_statefulset, $._config.service_ignored_labels) +
      service.mixin.spec.withClusterIp('None')
    else {},

  alertmanager_pdb: if !$._config.is_microservices_deployment_mode || !$._config.alertmanager_enabled then null else
    $.newMimirPdb('alertmanager'),
}
