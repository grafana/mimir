// Based on test-compactor-scheduler-autoscaling.jsonnet.
(import 'test-compactor-scheduler-autoscaling.jsonnet') {
  _config+:: {
    autoscaling_compactor_scheduler_compaction_bytes_per_second_query:
      'max by (compaction_type) (namespace_compaction_type:cortex_compactor_compaction_bytes_per_second:rate24h{namespace="default"})',
    autoscaling_compactor_scheduler_plan_job_seconds_query:
      'max(namespace:cortex_compactor_plan_job_duration_seconds:avg24h{namespace="default"})',
  },
}
