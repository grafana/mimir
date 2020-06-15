{
  local pvc = $.core.v1.persistentVolumeClaim,
  local volumeMount = $.core.v1.volumeMount,
  local container = $.core.v1.container,
  local statefulSet = $.apps.v1beta1.statefulSet,
  local service = $.core.v1.service,


  alertmanager_args::
    {
      target: 'alertmanager',
      'log.level': 'debug',

      'alertmanager.storage.type': 'gcs',
      'alertmanager.storage.path': '/data',
      'alertmanager.gcs.bucketname': '%(cluster)s-cortex-configdb-%(namespace)s' % $._config,
      'alertmanager.web.external-url': '%s/alertmanager' % $._config.external_url,
    },

  alertmanager_pvc::
    pvc.new() +
    pvc.mixin.metadata.withName('alertmanager-data') +
    pvc.mixin.spec.withAccessModes('ReadWriteOnce') +
    pvc.mixin.spec.resources.withRequests({ storage: '100Gi' }),

  alertmanager_container::
    container.new('alertmanager', $._images.alertmanager) +
    container.withPorts($.util.defaultPorts) +
    container.withArgsMixin($.util.mapToFlags($.alertmanager_args)) +
    container.withVolumeMountsMixin([volumeMount.new('alertmanager-data', '/data')]) +
    $.util.resourcesRequests('100m', '1Gi') +
    $.util.readinessProbe +
    $.jaeger_mixin,


  alertmanager_statefulset:
    statefulSet.new('alertmanager', 1, [$.alertmanager_container], $.alertmanager_pvc) +
    statefulSet.mixin.spec.withServiceName('alertmanager') +
    statefulSet.mixin.metadata.withNamespace($._config.namespace) +
    statefulSet.mixin.metadata.withLabels({ name: 'alertmanager' }) +
    statefulSet.mixin.spec.template.metadata.withLabels({ name: 'alertmanager' }) +
    statefulSet.mixin.spec.selector.withMatchLabels({ name: 'alertmanager' }) +
    statefulSet.mixin.spec.template.spec.securityContext.withRunAsUser(0) +
    statefulSet.mixin.spec.updateStrategy.withType('RollingUpdate') +
    statefulSet.mixin.spec.template.spec.withTerminationGracePeriodSeconds(900),

  alertmanager_service:
    $.util.serviceFor($.alertmanager_statefulset),
}
