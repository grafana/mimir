// Based on test-compactor-scheduler-autoscaling.jsonnet.
(import 'test-compactor-scheduler-autoscaling.jsonnet') {
  _config+:: {
    compactor_p2_fleet_enabled: true,
    compactor_p2_max_concurrency: 2,
    autoscaling_compactor_p2_min_replicas: 1,
    autoscaling_compactor_p2_max_replicas: 10,
  },
}
