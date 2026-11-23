// Based on test-usage-tracker.jsonnet.
(import 'test-usage-tracker.jsonnet') {
  _config+:: {
    usage_tracker_hpa_enabled: true,
    usage_tracker_hpa_min_replicas_per_zone: 2,
    usage_tracker_hpa_max_replicas_per_zone: 16,
  },
}
