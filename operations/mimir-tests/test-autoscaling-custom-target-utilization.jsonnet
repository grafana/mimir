// Based on test-autoscaling.jsonnet.
(import 'test-autoscaling.jsonnet') {
  _config+:: {
    local targetUtilization = 0.89,

    autoscaling_querier_target_utilization: targetUtilization,
    autoscaling_ruler_querier_cpu_target_utilization: targetUtilization,
    autoscaling_ruler_querier_memory_target_utilization: targetUtilization,
    autoscaling_ruler_querier_workers_target_utilization: targetUtilization,
    autoscaling_distributor_cpu_target_utilization: targetUtilization,
    autoscaling_distributor_memory_target_utilization: targetUtilization,
    autoscaling_ruler_cpu_target_utilization: targetUtilization,
    autoscaling_ruler_memory_target_utilization: targetUtilization,
    autoscaling_query_frontend_cpu_target_utilization: targetUtilization,
    autoscaling_query_frontend_memory_target_utilization: targetUtilization,
    autoscaling_ruler_query_frontend_cpu_target_utilization: targetUtilization,
    autoscaling_ruler_query_frontend_memory_target_utilization: targetUtilization,
    autoscaling_alertmanager_cpu_target_utilization: targetUtilization,
    autoscaling_alertmanager_memory_target_utilization: targetUtilization,
  },
}
