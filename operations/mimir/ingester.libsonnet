{
  local container = $.core.v1.container,
  local pvc = $.core.v1.persistentVolumeClaim,
  local statefulSet = $.apps.v1.statefulSet,
  local volumeMount = $.core.v1.volumeMount,

  ingester_args::
    $._config.commonConfig +
    $._config.usageStatsConfig +
    $._config.grpcConfig +
    $._config.storageConfig +
    $._config.blocksStorageConfig +
    $._config.ingesterRingClientConfig +
    $._config.ingesterLimitsConfig +
    {
      target: 'ingester',

      'server.http-listen-port': $._config.server_http_port,
      'server.grpc-max-concurrent-streams': 500,

      // Ring config.
      'ingester.ring.num-tokens': 512,
      'ingester.ring.unregister-on-shutdown': $._config.unregister_ingesters_on_shutdown,

      // Blocks storage.
      'blocks-storage.tsdb.dir': '/data/tsdb',
      'blocks-storage.tsdb.block-ranges-period': '2h',
      'blocks-storage.tsdb.ship-interval': '1m',

      // Spread TSDB head compaction over a wider time range.
      'blocks-storage.tsdb.head-compaction-interval': '15m',

      // Persist ring tokens so that when the ingester will be restarted
      // it will pick the same tokens
      'ingester.ring.tokens-file-path': '/data/tokens',

      // When running vertical autoscaling (e.g. Grafana Labs "automated scaling"), which sets
      // the CPU request based on the actual utilization, we don't want an higher CPU to be
      // requested just because it spikes during the WAL replay. Therefore, the WAL replay
      // concurrency is chosen in such a way that it is always less than the current CPU request.
      'blocks-storage.tsdb.wal-replay-concurrency': std.max(1, std.floor($.util.parseCPU($.ingester_container.resources.requests.cpu) - 1)),

      // Relax pressure on KV store when running at scale.
      'ingester.ring.heartbeat-period': '2m',
    } + (
      // Optionally configure the TSDB head early compaction (only when enabled).
      if !$._config.ingester_tsdb_head_early_compaction_enabled then {} else {
        'blocks-storage.tsdb.early-head-compaction-min-in-memory-series': $._config.ingester_tsdb_head_early_compaction_min_in_memory_series,
        'blocks-storage.tsdb.early-head-compaction-min-estimated-series-reduction-percentage': $._config.ingester_tsdb_head_early_compaction_reduction_percentage,
      }
    ) + $.mimirRuntimeConfigFile,

  ingester_ports:: $.util.defaultPorts,

  local name = 'ingester',

  ingester_env_map:: {
    JAEGER_REPORTER_MAX_QUEUE_SIZE: '1000',

    local requests = $.util.parseCPU($.ingester_container.resources.requests.cpu),
    local requestsWithHeadroom = std.ceil(
      // double the requests, but with headroom of at least 3 and at most 6 CPU
      // too little headroom can lead to throttling
      // too much headroom can lead to inefficeint scheduling of goroutines
      // (e.g. when NumCPU is 128, but the ingester only needs 5 cores).
      requests + std.max(3, std.min(6, requests)),
    ) + 1,
    // If the ingester read path is limited,
    // we don't want to set GOMAXPROCS to something higher than that because then the read path limit will never be hit.
    local limitBasedGOMAXPROCS = std.parseInt($.ingester_args['ingester.read-path-cpu-utilization-limit']) + 1,
    local GOMAXPROCS = if 'ingester.read-path-cpu-utilization-limit' in $.ingester_args then limitBasedGOMAXPROCS else requestsWithHeadroom,

    GOMAXPROCS: std.toString(GOMAXPROCS),
  },

  ingester_node_affinity_matchers:: [],

  ingester_container::
    container.new(name, $._images.ingester) +
    container.withPorts($.ingester_ports) +
    container.withArgsMixin($.util.mapToFlags($.ingester_args)) +
    $.util.resourcesRequests('4', '15Gi') +
    $.util.resourcesLimits(null, '25Gi') +
    $.util.readinessProbe +
    $.jaeger_mixin,

  // The ingesters should persist TSDB blocks and WAL on a persistent
  // volume in order to be crash resilient.
  local ingester_data_pvc =
    pvc.new() +
    pvc.mixin.spec.resources.withRequests({ storage: $._config.ingester_data_disk_size }) +
    pvc.mixin.spec.withAccessModes(['ReadWriteOnce']) +
    pvc.mixin.spec.withStorageClassName($._config.ingester_data_disk_class) +
    pvc.mixin.metadata.withName('ingester-data'),

  newIngesterStatefulSet(name, container, withAntiAffinity=true, nodeAffinityMatchers=[])::
    local ingesterContainer = container + $.core.v1.container.withVolumeMountsMixin([
      volumeMount.new('ingester-data', '/data'),
    ]);

    $.newMimirStatefulSet(name, 3, ingesterContainer, ingester_data_pvc) +
    $.newMimirNodeAffinityMatchers(nodeAffinityMatchers) +
    // When the ingester needs to flush blocks to the storage, it may take quite a lot of time.
    // For this reason, we grant an high termination period (80 minutes).
    statefulSet.mixin.spec.template.spec.withTerminationGracePeriodSeconds(1200) +
    $.mimirVolumeMounts +
    $.util.podPriority('high') +
    (if withAntiAffinity then $.util.antiAffinity else {}),

  ingester_statefulset: if !$._config.is_microservices_deployment_mode then null else
    self.newIngesterStatefulSet(
      'ingester',
      $.ingester_container + (if std.length($.ingester_env_map) > 0 then container.withEnvMap(std.prune($.ingester_env_map)) else {}),
      !$._config.ingester_allow_multiple_replicas_on_same_node,
      $.ingester_node_affinity_matchers,
    ),

  ingester_service: if !$._config.is_microservices_deployment_mode then null else
    $.util.serviceFor($.ingester_statefulset, $._config.service_ignored_labels),

  newIngesterPdb(ingesterName)::
    $.newMimirPdb(ingesterName),

  ingester_pdb: if !$._config.is_microservices_deployment_mode then null else
    self.newIngesterPdb(name),
}
