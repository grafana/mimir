{
  _config+:: {
    // Compaction jobs whose source blocks span at most this duration are served from the p1 lane,
    // longer ones from the p2 lane.
    compactor_scheduler_p1_max_span: '2h',

    // One worker goroutine, and one core requested, per concurrent job.
    compactor_p2_max_concurrency: $._config.compactor_max_concurrency,

    compactor_p2_memory: '6Gi',

    // Only used when the fleet is not autoscaled.
    compactor_p2_replicas: 1,
  },

  assert !$._config.compactor_p2_fleet_enabled || $._config.compactor_scheduler_enabled
         : 'compactor_p2_fleet_enabled requires compactor_scheduler_enabled',

  assert !$._config.autoscaling_compactor_p2_enabled ||
         !$._config.autoscaling_compactor_scheduler_lag_trigger_enabled ||
         $._config.autoscaling_compactor_scheduler_lag_trigger_use_last_empty_metric
         : 'compactor_p2_fleet_enabled requires autoscaling_compactor_scheduler_lag_trigger_use_last_empty_metric, because a per-lane lag factor cannot be derived from cortex_compactor_scheduler_pending_jobs',

  compactor_scheduler_args+:: if !$._config.compactor_p2_fleet_enabled then {} else {
    'compactor-scheduler.lane-policy.policy': 'compaction-urgency',
    'compactor-scheduler.lane-policy.compaction-urgency.p1-max-span': $._config.compactor_scheduler_p1_max_span,
  },

  // The default lane shape (compact+plan,plan) scoped to p1: this fleet also runs the planning.
  compactor_args+:: if !$._config.compactor_p2_fleet_enabled then {} else {
    'compactor.scheduler-client.lanes': 'compact-p1+plan,plan',
  },

  // Electing a cleaner is the ring's only job in scheduler mode, so opting out of cleanup leaves
  // nothing to join it for. Cleanup stays with the "compactor" fleet, which keeps its ring.
  compactor_p2_args:: $.compactor_args {
    'compactor.compaction-concurrency': $._config.compactor_p2_max_concurrency,
    'compactor.scheduler-client.lanes': std.join(',', std.repeat(['compact-p2'], $._config.compactor_p2_max_concurrency)),
    'compactor.scheduler-client.enable-ring-based-cleanup': false,
  },

  compactor_p2_env_map:: $.compactor_env_map,

  compactor_p2_node_affinity_matchers:: $.compactor_node_affinity_matchers,

  compactor_p2_container:: $.newCompactorContainer(
    $.compactor_p2_args,
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
