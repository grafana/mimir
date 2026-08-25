// Based on test-compactor-scheduler-autoscaling.jsonnet.
(import 'test-compactor-scheduler-autoscaling.jsonnet') {
  _config+:: {
    compactor_defer_fleet_enabled: true,
    compactor_defer_max_concurrency: 2,
    autoscaling_compactor_defer_min_replicas: 1,
    autoscaling_compactor_defer_max_replicas: 10,
  },
}
