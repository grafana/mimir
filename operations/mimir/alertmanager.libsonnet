{
  local pvc = $.core.v1.persistentVolumeClaim,
  local volumeMount = $.core.v1.volumeMount,
  local container = $.core.v1.container,
  local statefulSet = $.apps.v1.statefulSet,
  local service = $.core.v1.service,
  local isGossiping = $._config.alertmanager.replicas > 1,
  local peers = if isGossiping then
    [
      'alertmanager-%d.alertmanager.%s.svc.%s.local:%s' % [i, $._config.namespace, $._config.cluster, $._config.alertmanager_gossip_port]
      for i in std.range(0, $._config.alertmanager.replicas - 1)
    ]
  else [],

  alertmanager_args::
    {
      target: 'alertmanager',
      'log.level': 'debug',

      'experimental.alertmanager.enable-api': 'true',
      'alertmanager.storage.type': 'gcs',
      'alertmanager.storage.path': '/data',
      'alertmanager.storage.gcs.bucketname': '%(cluster)s-cortex-%(namespace)s' % $._config,
      'alertmanager.web.external-url': '%s/alertmanager' % $._config.external_url,
    },

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
      container.withPorts(
        $.util.defaultPorts +
        if isGossiping then [
          $.core.v1.containerPort.newUDP('gossip-udp', $._config.alertmanager_gossip_port),
          $.core.v1.containerPort.new('gossip-tcp', $._config.alertmanager_gossip_port),
        ]
        else [],
      ) +
      container.withEnvMixin([container.envType.fromFieldPath('POD_IP', 'status.podIP')]) +
      container.withArgsMixin(
        $.util.mapToFlags($.alertmanager_args) +
        if isGossiping then
          ['--cluster.listen-address=[$(POD_IP)]:%s' % $._config.alertmanager_gossip_port] +
          ['--cluster.peer=%s' % peer for peer in peers]
        else [],
      ) +
      container.withVolumeMountsMixin([volumeMount.new('alertmanager-data', '/data')]) +
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
      statefulSet.mixin.spec.template.spec.withTerminationGracePeriodSeconds(900)
    else {},

  alertmanager_service:
    if $._config.alertmanager_enabled then
      if $._config.alertmanager.replicas > 1 then
        $.util.serviceFor($.alertmanager_statefulset) +
        service.mixin.spec.withClusterIp('None')
      else
        $.util.serviceFor($.alertmanager_statefulset)
    else {},
}
