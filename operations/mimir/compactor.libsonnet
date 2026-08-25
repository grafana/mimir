{
  local pvc = $.core.v1.persistentVolumeClaim,
  local volumeMount = $.core.v1.volumeMount,
  local container = $.core.v1.container,
  local statefulSet = $.apps.v1.statefulSet,
  local deployment = $.apps.v1.deployment,

  compactor_args::
    $._config.commonConfig +
    $._config.usageStatsConfig +
    $._config.grpcConfig +
    $._config.storageConfig +
    $._config.blocksStorageConfig +
    $._config.compactorLimitsConfig +
    {
      target: 'compactor',

      'server.http-listen-port': $._config.server_http_port,

      // Compactor config.
      'compactor.block-ranges': '2h,12h,24h',
      'compactor.data-dir': '/data',
      'compactor.compaction-interval': '30m',
      'compactor.compaction-concurrency': $._config.compactor_max_concurrency,
      'compactor.cleanup-interval': $._config.compactor_cleanup_interval,

      // Will be set on per-tenant basis via overrides and user classes. No splitting by default.
      'compactor.split-and-merge-shards': 0,
      'compactor.compactor-tenant-shard-size': 1,

      // One group by default, except when overriden by user class.
      'compactor.split-groups': 1,

      // Enable open/close/flush concurrency.
      'compactor.max-opening-blocks-concurrency': '4',
      'compactor.max-closing-blocks-concurrency': '2',  // Closing of blocks means writing index, which uses extra memory, hence only 2.
      'compactor.symbols-flushers-concurrency': '4',

      // Configure sharding.
      'compactor.ring.store': 'consul',
      'compactor.ring.consul.hostname': 'consul.%(namespace)s.svc.%(cluster_domain)s:8500' % $._config,
      'compactor.ring.prefix': '',
      'compactor.ring.wait-stability-min-duration': '1m',  // Wait until ring is stable before switching to ACTIVE.

      // Relax pressure on KV store when running at scale.
      'compactor.ring.heartbeat-period': '1m',
      'compactor.ring.heartbeat-timeout': '4m',

      // The compactor wait period is the amount of time that compactors will wait before compacting
      // 1st level blocks (uploaded by ingesters) since the last block was uploaded. In the worst
      // case scenario, we have 1 ingester whose TSDB head compaction started at time 0 and another
      // ingester who started at time -blocks-storage.tsdb.head-compaction-interval. So the total
      // time we should wait is -blocks-storage.tsdb.head-compaction-interval + a small delta (10m) to
      // account for am ingester slower than others to compact and upload blocks.
      'compactor.first-level-compaction-wait-period': $.util.formatDuration(
        $.util.parseDuration($.ingester_args['blocks-storage.tsdb.head-compaction-interval']) +
        $.util.parseDuration('10m')
      ),

      // Delete blocks sooner in order to keep the number of live blocks lower in the storage.
      'compactor.deletion-delay': $.util.formatDuration(
        // Bucket index is updated every cleanup interval.
        $.util.parseDuration($._config.compactor_cleanup_interval) +
        // Wait until after the ignore deletion marks delay.
        $.util.parseDuration(
          if std.objectHas($.store_gateway_args, 'blocks-storage.bucket-store.ignore-deletion-marks-delay') then
            $.store_gateway_args['blocks-storage.bucket-store.ignore-deletion-marks-delay']
          else
            '1h'  // Default config.
        ) +
        // Wait until store-gateway have updated. Add 3x the sync interval (instead of 1x) to account for delays and temporarily failures.
        ($.util.parseDuration(
           if std.objectHas($.store_gateway_args, 'blocks-storage.bucket-store.sync-interval') then
             $.store_gateway_args['blocks-storage.bucket-store.sync-interval']
           else
             '15m'  // Default config.
         ) * 3)
      ),
    } + $.mimirRuntimeConfigFile,

  local compactor_data_pvc =
    pvc.new() +
    pvc.mixin.spec.resources.withRequests({ storage: $._config.compactor_data_disk_size }) +
    pvc.mixin.spec.withAccessModes(['ReadWriteOnce']) +
    pvc.mixin.spec.withStorageClassName($._config.compactor_data_disk_class) +
    pvc.mixin.metadata.withName('compactor-data'),

  compactor_ports:: $.util.defaultPorts,

  compactor_env_map:: {},

  compactor_node_affinity_matchers:: [],

  // Always named "compactor", whichever workload runs it, so dashboards and alerts selecting on the
  // container name cover every fleet.
  newCompactorContainer(args, memory='6Gi', envmap={})::
    container.new('compactor', $._images.compactor) +
    container.withPorts($.compactor_ports) +
    container.withArgsMixin($.util.mapToFlags(args)) +
    container.withVolumeMountsMixin([volumeMount.new('compactor-data', '/data')]) +
    (if std.length(envmap) > 0 then container.withEnvMap(std.prune(envmap)) else {}) +
    // Do not limit compactor CPU and request enough cores to honor configured max concurrency.
    $.util.resourcesRequests(args['compactor.compaction-concurrency'], memory) +
    $.util.resourcesLimits(null, memory) +
    $.mimirEphemeralStorageRequest +
    $.util.readinessProbe +
    $.tracing_env_mixin,

  compactor_container::
    $.newCompactorContainer($.compactor_args, envmap=$.compactor_env_map),

  // Switches a compactor StatefulSet to rollout-operator-coordinated concurrent rollouts: OnDelete updates
  // plus a rollout-group keyed on the StatefulSet name.
  newCompactorConcurrentRolloutMixin(name, max_unavailable)::
    statefulSet.mixin.spec.selector.withMatchLabels({ name: name, 'rollout-group': name }) +
    statefulSet.mixin.spec.updateStrategy.withType('OnDelete') +
    statefulSet.mixin.metadata.withLabelsMixin({ 'rollout-group': name }) +
    statefulSet.mixin.metadata.withAnnotationsMixin({ 'rollout-max-unavailable': std.toString(max_unavailable) }) +
    statefulSet.mixin.spec.template.metadata.withLabelsMixin({ 'rollout-group': name }),

  newCompactorStatefulSet(name, container, nodeAffinityMatchers=[], concurrent_rollout_enabled=false, max_unavailable=1)::
    $.newMimirStatefulSet(name, 1, container, compactor_data_pvc) +
    $.newMimirNodeAffinityMatchers(nodeAffinityMatchers) +
    statefulSet.mixin.spec.template.spec.withTerminationGracePeriodSeconds(900) +
    $.mimirVolumeMounts +
    (if !concurrent_rollout_enabled then {} else $.newCompactorConcurrentRolloutMixin(name, max_unavailable)),

  // Only valid for workers running with -compactor.scheduler-client.enable-ring-based-cleanup=false.
  newCompactorWorkerDeployment(name, replicas, container, nodeAffinityMatchers=[])::
    deployment.new(name, replicas, [container]) +
    $.newMimirNodeAffinityMatchers(nodeAffinityMatchers) +
    deployment.spec.template.spec.withVolumes([
      {
        name: 'compactor-data',
        ephemeral: {
          volumeClaimTemplate: {
            spec: {
              accessModes: ['ReadWriteOnce'],
              storageClassName: $._config.compactor_data_disk_class,
              resources: { requests: { storage: $._config.compactor_data_disk_size } },
            },
          },
        },
      },
    ]) +
    $.mimirVolumeMounts +
    (if !std.isObject($._config.node_selector) then {} else deployment.mixin.spec.template.spec.withNodeSelectorMixin($._config.node_selector)) +
    deployment.mixin.spec.template.spec.withTerminationGracePeriodSeconds(900) +
    deployment.mixin.spec.strategy.rollingUpdate.withMaxSurge('25%') +
    deployment.mixin.spec.strategy.rollingUpdate.withMaxUnavailable(0),

  compactor_statefulset:
    $.newCompactorStatefulSet(
      'compactor',
      $.compactor_container,
      $.compactor_node_affinity_matchers,
      $._config.cortex_compactor_concurrent_rollout_enabled,
      $._config.cortex_compactor_max_unavailable,
    ),

  compactor_service:
    local service = $.core.v1.service;

    $.util.serviceFor($.compactor_statefulset, $._config.service_ignored_labels) +
    service.mixin.spec.withClusterIp('None'),

  compactor_pdb:
    $.newMimirPdb('compactor'),
}
