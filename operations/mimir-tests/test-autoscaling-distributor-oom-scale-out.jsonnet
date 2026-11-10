// Based on test-autoscaling.jsonnet. Renders the distributor ScaledObject with the OOM protection and OOM
// scale-out triggers. Both need the distributor memory trigger to be disabled.
(import 'test-autoscaling.jsonnet') {
  _config+:: {
    distributor_gomemlimit_enabled: true,
    autoscaling_oom_protection_enabled: true,
    autoscaling_distributor_oom_scale_out_enabled: true,
  },
}
