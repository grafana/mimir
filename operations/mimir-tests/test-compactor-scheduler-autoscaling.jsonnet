// Based on test-compactor-scheduler.jsonnet.
(import 'test-compactor-scheduler.jsonnet') {
  _config+:: {
    autoscaling_compactor_enabled: true,
    autoscaling_compactor_min_replicas: 2,
    autoscaling_compactor_max_replicas: 30,
  },
}
