{
  _config+:: {
    // Compaction jobs whose source blocks span at most this duration are served from the p1 lane,
    // longer ones from the p2 lane.
    compactor_scheduler_p1_max_span: '2h',

    // Number of jobs a p2 worker runs concurrently. It also sizes the CPU request, since a job
    // saturates roughly one core.
    compactor_p2_max_concurrency: $._config.compactor_max_concurrency,

    compactor_p2_memory: '6Gi',

    // Replicas of the p2 fleet. Only used when the fleet is not autoscaled.
    compactor_p2_replicas: 1,
  },

  assert !$._config.compactor_p2_fleet_enabled || $._config.compactor_scheduler_enabled
         : 'compactor_p2_fleet_enabled requires compactor_scheduler_enabled',

  // Both fleets are sized from the backlog of the lane they serve, which is only readable from the
  // per-lane last-empty gauge: cortex_compactor_scheduler_pending_jobs is not labelled by lane.
  local laneScopedAutoscaling = $._config.compactor_p2_fleet_enabled && $._config.autoscaling_compactor_scheduler_drain_enabled,
  assert !laneScopedAutoscaling ||
         !$._config.autoscaling_compactor_scheduler_lag_trigger_enabled ||
         $._config.autoscaling_compactor_scheduler_lag_trigger_use_last_empty_metric
         : 'compactor_p2_fleet_enabled requires autoscaling_compactor_scheduler_lag_trigger_use_last_empty_metric, because a per-lane lag factor cannot be derived from cortex_compactor_scheduler_pending_jobs',

  compactor_scheduler_args+:: if !$._config.compactor_p2_fleet_enabled then {} else {
    'compactor-scheduler.lane-policy.policy': 'compaction-urgency',
    'compactor-scheduler.lane-policy.compaction-urgency.p1-max-span': $._config.compactor_scheduler_p1_max_span,
  },

  // Keep the worker shape of the default lane configuration (compact+plan,plan), scoped to p1: the
  // p1 fleet also runs the planning that feeds both lanes.
  compactor_args+:: if !$._config.compactor_p2_fleet_enabled then {} else {
    'compactor.scheduler-client.lanes': 'compact-p1+plan,plan',
  },

  // One worker goroutine per concurrent job, each serving p2 only.
  //
  // The p2 workers do not clean, and so have no use for the ring: its only remaining job in
  // scheduler mode is electing a cleaner per tenant. Cleanup stays with the "compactor" fleet,
  // which keeps its ring. Dropping the ring also keeps the p2 fleet's scaling from reshuffling
  // cleanup ownership, and lets a new worker lease its first job without waiting for ring
  // stability, which is when a fleet sized for spikes can least afford to wait.
  compactor_p2_args:: $.compactor_args {
    'compactor.compaction-concurrency': $._config.compactor_p2_max_concurrency,
    'compactor.scheduler-client.lanes': std.join(',', std.repeat(['compact-p2'], $._config.compactor_p2_max_concurrency)),
    'compactor.scheduler-client.disable-ring-based-cleanup': true,
  },

  compactor_p2_env_map:: $.compactor_env_map,

  compactor_p2_node_affinity_matchers:: $.compactor_node_affinity_matchers,

  compactor_p2_container:: $.newCompactorContainer(
    $.compactor_p2_args,
    $._config.compactor_p2_max_concurrency,
    $._config.compactor_p2_memory,
    $.compactor_p2_env_map,
  ),

  compactor_p2_deployment: if !$._config.compactor_p2_fleet_enabled then null else
    $.newCompactorWorkerDeployment(
      'compactor-p2',
      $._config.compactor_p2_replicas,
      $.compactor_p2_container,
      $.compactor_p2_node_affinity_matchers,
    ),

  compactor_p2_service: if !$._config.compactor_p2_fleet_enabled then null else
    $.util.serviceFor($.compactor_p2_deployment, $._config.service_ignored_labels),

  compactor_p2_pdb: if !$._config.compactor_p2_fleet_enabled then null else
    $.newMimirPdb('compactor-p2'),
}
