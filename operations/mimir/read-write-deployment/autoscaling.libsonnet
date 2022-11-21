{
  _config+:: {
    autoscaling_mimir_read_enabled: false,
    autoscaling_mimir_read_min_replicas: error 'you must set autoscaling_mimir_read_min_replicas in the _config',
    autoscaling_mimir_read_max_replicas: error 'you must set autoscaling_mimir_read_max_replicas in the _config',
  },

  //
  // Helper methods
  //

  local removeReplicasFromSpec = {
    spec+: {
      // Remove the "replicas" field so that Flux doesn't reconcile it.
      replicas+:: null,
    },
  },

  read_scaled_object: if !$._config.autoscaling_mimir_read_enabled then null else
    // NOTE(jhesketh): this reuses the newQuerierScaledObject from operations/mimir/autoscaling.libsonnet
    //                 as the scaling metric (cortex_query_scheduler_inflight_requests) is expected to
    //                 be a sufficient indication of load on the read path.
    self.newQuerierScaledObject(
      name='mimir-read',
      query_scheduler_container='mimir-backend',
      querier_max_concurrent=$.querier_args['querier.max-concurrent'],
      min_replicas=$._config.autoscaling_mimir_read_min_replicas,
      max_replicas=$._config.autoscaling_mimir_read_max_replicas,
    ),

  mimir_read_deployment: if !$._config.is_read_write_deployment_mode then null else (
    super.mimir_read_deployment + (
      if !$._config.autoscaling_mimir_read_enabled then {} else
        removeReplicasFromSpec
    )
  ),
}
