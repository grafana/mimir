// Based on test-multi-az-write-path.jsonnet, which already enables zone-aware distributor
// autoscaling, so this also covers the per-zone metric names and matchers.
(import 'test-multi-az-write-path.jsonnet') {
  _config+:: {
    autoscaling_distributor_inflight_push_requests_target_utilization: 0.5,
  },
}
