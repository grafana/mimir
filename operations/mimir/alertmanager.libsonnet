{
  local container = $.core.v1.container,

  alertmanager_args::
    {
      target: 'alertmanager',
      'log.level': 'debug',

      'alertmanager.storage.type': 'gcs',
      'alertmanager.gcs.bucketname': '%(cluster)s-cortex-configdb-%(namespace)s' % $._config,
      'alertmanager.web.external-url': 'http://alertmanager.%s.svc.cluster.local/alertmanager' % $._config.namespace,
    },

  alertmanager_container::
    container.new('alertmanager', $._images.alertmanager) +
    container.withPorts($.util.defaultPorts) +
    container.withArgsMixin($.util.mapToFlags($.alertmanager_args)) +
    $.util.resourcesRequests('100m', '1Gi') +
    $.jaeger_mixin,

  local deployment = $.apps.v1beta1.deployment,

  alertmanager_deployment:
    deployment.new('alertmanager', 1, [$.alertmanager_container]) +
    deployment.mixin.spec.template.spec.withRestartPolicy('Always') +
    $.util.antiAffinity,

  local service = $.core.v1.service,

  alertmanager_server:
    $.util.serviceFor($.alertmanager_deployment),
}
