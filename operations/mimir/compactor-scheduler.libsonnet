{
  local container = $.core.v1.container,
  local statefulSet = $.apps.v1.statefulSet,
  local service = $.core.v1.service,
  local pvc = $.core.v1.persistentVolumeClaim,
  local volumeMount = $.core.v1.volumeMount,

  _config+:: {
    compactor_scheduler_data_disk_size: '10Gi',
    compactor_scheduler_data_disk_class: 'standard',
    compactor_scheduler_planning_interval: '30m',

    // When enabled, compactors cache block metadata read through the scheduler-client,
    // reusing the same memcached the store-gateway uses for block metadata.
    compactor_scheduler_metadata_caching_enabled: $._config.cache_metadata_enabled,
  },

  compactor_scheduler_args::
    $._config.commonConfig +
    $._config.grpcConfig +
    $._config.storageConfig +
    $._config.blocksStorageConfig +
    {
      target: 'compactor-scheduler',
      'server.http-listen-port': $._config.server_http_port,
      'compactor-scheduler.persistence-type': 'bbolt',
      'compactor-scheduler.bbolt.dir': '/data',
      'compactor-scheduler.planning-interval': $._config.compactor_scheduler_planning_interval,

      // Keep tenant scoping in sync with the compactor when it's set via overrides.
      [if 'compactor.enabled-tenants' in $.compactor_args then 'compactor.enabled-tenants']:
        $.compactor_args['compactor.enabled-tenants'],
      [if 'compactor.disabled-tenants' in $.compactor_args then 'compactor.disabled-tenants']:
        $.compactor_args['compactor.disabled-tenants'],
    },

  compactor_scheduler_ports:: $.util.defaultPorts,

  compactor_scheduler_env_map:: {},

  compactor_scheduler_node_affinity_matchers:: [],

  local compactor_scheduler_data_pvc =
    pvc.new() +
    pvc.mixin.spec.resources.withRequests({ storage: $._config.compactor_scheduler_data_disk_size }) +
    pvc.mixin.spec.withAccessModes(['ReadWriteOnce']) +
    pvc.mixin.spec.withStorageClassName($._config.compactor_scheduler_data_disk_class) +
    pvc.mixin.metadata.withName('compactor-scheduler-data'),

  // The scheduler's persisted state is rebuildable, so the volume is removed with the pod.
  local pvc_retention_policy =
    statefulSet.spec.persistentVolumeClaimRetentionPolicy.withWhenScaled('Delete') +
    statefulSet.spec.persistentVolumeClaimRetentionPolicy.withWhenDeleted('Delete'),

  newCompactorSchedulerContainer(name, args, envmap={})::
    container.new(name, $._images.compactor_scheduler) +
    container.withPorts($.compactor_scheduler_ports) +
    container.withArgsMixin($.util.mapToFlags(args)) +
    container.withVolumeMountsMixin([volumeMount.new('compactor-scheduler-data', '/data')]) +
    $.util.resourcesRequests('100m', '512Mi') +
    $.util.resourcesLimits('500m', '1Gi') +
    $.mimirEphemeralStorageRequest +
    $.util.readinessProbe +
    $.tracing_env_mixin +
    (if std.length(envmap) > 0 then container.withEnvMap(std.prune(envmap)) else {}),

  compactor_scheduler_container::
    self.newCompactorSchedulerContainer('compactor-scheduler', $.compactor_scheduler_args, $.compactor_scheduler_env_map),

  newCompactorSchedulerStatefulSet(name, container, nodeAffinityMatchers=[])::
    $.newMimirStatefulSet(name, 1, container, compactor_scheduler_data_pvc) +
    $.newMimirNodeAffinityMatchers(nodeAffinityMatchers) +
    $.mimirVolumeMounts +
    pvc_retention_policy,

  compactor_scheduler_statefulset: if !$._config.compactor_scheduler_enabled then null else
    self.newCompactorSchedulerStatefulSet('compactor-scheduler', $.compactor_scheduler_container, $.compactor_scheduler_node_affinity_matchers),

  newCompactorSchedulerService(name, sts)::
    $.util.serviceFor(sts, $._config.service_ignored_labels) +
    service.mixin.spec.withSelector({
      name: name,
      // Select only the first replica, in case somehow multiple are running.
      'statefulset.kubernetes.io/pod-name': '%s-0' % name,
    }),

  compactor_scheduler_service: if !$._config.compactor_scheduler_enabled then null else
    self.newCompactorSchedulerService('compactor-scheduler', $.compactor_scheduler_statefulset),

  compactor_scheduler_pdb: if !$._config.compactor_scheduler_enabled then null else
    $.newMimirPdb('compactor-scheduler'),

  // Arguments added to compactors so they pull jobs from the scheduler instead of
  // planning locally.
  compactor_scheduler_worker_args:: {
    'compactor.scheduler-client.enabled': 'true',
    'compactor.scheduler-client.scheduler-endpoint': 'dns:///compactor-scheduler.%(namespace)s.svc.%(cluster_domain)s:9095' % $._config,
  } + (
    if !$._config.compactor_scheduler_metadata_caching_enabled then {} else
      local merged = $.blocks_metadata_caching_config + $.blocks_metadata_zone_a_caching_config;
      {
        [std.strReplace(k, 'blocks-storage.bucket-store', 'compactor.scheduler-client')]: merged[k]
        for k in std.objectFields(merged)
      }
  ),

  compactor_args+:: if $._config.compactor_scheduler_enabled then $.compactor_scheduler_worker_args else {},

  // When the scheduler is enabled, compactors act as ephemeral workers (the scheduler re-leases
  // jobs from compactors that go away), so their data volumes are deleted on scale-down/deletion.
  compactor_statefulset+: if !$._config.compactor_scheduler_enabled then {} else
    statefulSet.spec.persistentVolumeClaimRetentionPolicy.withWhenScaled('Delete') +
    statefulSet.spec.persistentVolumeClaimRetentionPolicy.withWhenDeleted('Delete'),
}
