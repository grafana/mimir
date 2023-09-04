// Based on test-read-write-deployment-mode-s3.jsonnet.
(import 'test-read-write-deployment-mode-s3.jsonnet') {
  _config+:: {
    autoscaling_mimir_read_enabled: true,
    autoscaling_mimir_read_min_replicas: 2,
    autoscaling_mimir_read_max_replicas: 20,
  },
}
