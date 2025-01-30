{
  // Usage example:
  //
  // local flusher_job = import 'mimir/flusher-job-blocks.libsonnet';
  //
  // flusher_job {
  //   'flusher-25': $.flusher_job_func('flusher-25', 'ingester-data-ingester-25'),
  // }
  //
  // Where 'flusher-25' is a job name, and 'ingester-data-ingester-25' is PVC to flush.

  local container = $.core.v1.container,
  local job = $.batch.v1.job,
  local volumeMount = $.core.v1.volumeMount,
  local volume = $.core.v1.volume,

  flusher_env_map:: {},

  flusher_container::
    container.new('flusher', $._images.flusher) +
    container.withPorts($.util.defaultPorts) +
    container.withArgsMixin($.util.mapToFlags($.ingester_args {
      target: 'flusher',
      'blocks-storage.tsdb.retention-period': '10000h',  // don't delete old blocks too soon.
    })) +
    (if std.length($.flusher_env_map) > 0 then container.withEnvMap(std.prune($.flusher_env_map)) else {}) +
    $.util.resourcesRequests('4', '15Gi') +
    $.util.resourcesLimits(null, '25Gi') +
    $.util.readinessProbe +
    $.jaeger_mixin,

  flusher_job_func(jobName, pvcName)::
    job.new() +
    job.mixin.spec.template.spec.withContainers([
      $.flusher_container +
      container.withVolumeMountsMixin([
        volumeMount.new('flusher-data', '/data'),
      ]),
    ]) +
    job.mixin.spec.template.spec.withRestartPolicy('Never') +
    job.mixin.spec.template.spec.withVolumes([
      volume.fromPersistentVolumeClaim('flusher-data', pvcName),
    ]) +
    job.mixin.metadata.withName(jobName) +
    job.mixin.metadata.withNamespace($._config.namespace) +
    job.mixin.metadata.withLabels({ name: 'flusher' }) +
    job.mixin.spec.template.metadata.withLabels({ name: 'flusher' }) +
    job.mixin.spec.template.spec.withTerminationGracePeriodSeconds(300) +
    (if !std.isObject($._config.node_selector) then {} else job.mixin.spec.template.spec.withNodeSelectorMixin($._config.node_selector)) +
    $.mimirVolumeMounts +
    $.util.podPriority('high'),
}
