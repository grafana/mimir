{
  _config+:: {
    // Compaction jobs whose source blocks span at most this duration are urgent, longer ones are
    // deferred.
    compactor_scheduler_urgency_max_span: '2h',

    // Keep out-of-order jobs urgent whatever they span: they are recompacted repeatedly, so they
    // span a wide range while staying small.
    compactor_scheduler_urgency_out_of_order: true,

    // One worker goroutine, and one core requested, per concurrent job.
    compactor_defer_max_concurrency: $._config.compactor_max_concurrency,

    compactor_defer_memory: '6Gi',

    // Only used when the fleet is not autoscaled.
    compactor_defer_replicas: 1,
  },

  assert !$._config.compactor_defer_fleet_enabled || $._config.compactor_scheduler_enabled
         : 'compactor_defer_fleet_enabled requires compactor_scheduler_enabled',

  assert !$._config.autoscaling_compactor_defer_enabled ||
         !$._config.autoscaling_compactor_scheduler_lag_trigger_enabled ||
         $._config.autoscaling_compactor_scheduler_lag_trigger_use_last_empty_metric
         : 'compactor_defer_fleet_enabled requires autoscaling_compactor_scheduler_lag_trigger_use_last_empty_metric, because a per-lane lag factor cannot be derived from cortex_compactor_scheduler_pending_jobs',

  compactor_scheduler_args+:: if !$._config.compactor_defer_fleet_enabled then {} else {
    'compactor-scheduler.lane-policy.policy': 'urgency',
    'compactor-scheduler.lane-policy.urgency.max-span': $._config.compactor_scheduler_urgency_max_span,
    'compactor-scheduler.lane-policy.urgency.out-of-order': $._config.compactor_scheduler_urgency_out_of_order,
  },

  // The default lane shape (compact+plan,plan) scoped to urgent: this fleet also runs the planning.
  compactor_args+:: if !$._config.compactor_defer_fleet_enabled then {} else {
    'compactor.scheduler-client.lanes': 'compact-urgent+plan,plan',
  },

  // Electing a cleaner is the ring's only job in scheduler mode, so opting out of cleanup leaves
  // nothing to join it for. Cleanup stays with the "compactor" fleet, which keeps its ring.
  compactor_defer_args:: $.compactor_args {
    'compactor.compaction-concurrency': $._config.compactor_defer_max_concurrency,
    'compactor.scheduler-client.lanes': std.join(',', std.repeat(['compact-defer'], $._config.compactor_defer_max_concurrency)),
    'compactor.scheduler-client.enable-ring-based-cleanup': false,
  },

  compactor_defer_env_map:: $.compactor_env_map,

  compactor_defer_node_affinity_matchers:: $.compactor_node_affinity_matchers,

  compactor_defer_container:: $.newCompactorContainer(
    $.compactor_defer_args,
    $._config.compactor_defer_memory,
    $.compactor_defer_env_map,
  ),

  compactor_defer_deployment: if !$._config.compactor_defer_fleet_enabled then null else
    $.newCompactorWorkerDeployment(
      'compactor-defer',
      $._config.compactor_defer_replicas,
      $.compactor_defer_container,
      $.compactor_defer_node_affinity_matchers,
    ),

  compactor_defer_service: if !$._config.compactor_defer_fleet_enabled then null else
    $.util.serviceFor($.compactor_defer_deployment, $._config.service_ignored_labels),

  compactor_defer_pdb: if !$._config.compactor_defer_fleet_enabled then null else
    $.newMimirPdb('compactor-defer'),
}
