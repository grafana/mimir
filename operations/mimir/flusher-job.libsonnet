{
  // Usage example:
  // local flusher_job = import 'cortex/flusher-job.libsonnet';
  // flusher_job + {
  //   flusher_job:
  //     $.flusher_job_func('pvc-af8947e6-182e-11ea-82e4-42010a9a0137', 'ingester-pvc-ingester-5'),
  // }

  local container = $.core.v1.container,
  local job = $.batch.v1.job,
  local volumeMount = $.core.v1.volumeMount,
  local volume = $.core.v1.volume,

  flusher_container::
    container.new('flusher', $._images.flusher) +
    container.withPorts($.util.defaultPorts) +
    container.withArgsMixin($.util.mapToFlags($.ingester_args {
      target: 'flusher',
      'flusher.wal-dir': $._config.wal_dir,
    })) +
    $.util.resourcesRequests('4', '15Gi') +
    $.util.resourcesLimits(null, '25Gi') +
    $.util.readinessProbe +
    $.jaeger_mixin,

  flusher_job_storage_config_mixin::
    job.mixin.metadata.withAnnotationsMixin({ schemaID: $._config.schemaID },) +
    $.util.configVolumeMount('schema-' + $._config.schemaID, '/etc/cortex/schema'),

  flusher_job_func(volumeName, pvcName)::
    job.new() +
    job.mixin.spec.template.spec.withContainers([
      $.flusher_container +
      container.withVolumeMountsMixin([
        volumeMount.new(volumeName, $._config.wal_dir),
      ]),
    ]) +
    job.mixin.spec.template.spec.withRestartPolicy('Never') +
    job.mixin.spec.template.spec.withVolumes([
      volume.fromPersistentVolumeClaim(volumeName, pvcName),
    ]) +
    $.flusher_job_storage_config_mixin +
    job.mixin.metadata.withName('flusher') +
    job.mixin.metadata.withNamespace($._config.namespace) +
    job.mixin.metadata.withLabels({ name: 'flusher' }) +
    job.mixin.spec.template.metadata.withLabels({ name: 'flusher' }) +
    job.mixin.spec.template.spec.securityContext.withRunAsUser(0) +
    job.mixin.spec.template.spec.withTerminationGracePeriodSeconds(300) +
    $.util.configVolumeMount($._config.overrides_configmap, '/etc/cortex') +
    $.util.podPriority('high'),
}
