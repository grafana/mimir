{
  _config+:: {
    autoscaling_mimir_read_enabled: false,
    autoscaling_mimir_read_min_replicas: error 'you must set autoscaling_mimir_read_min_replicas in the _config',
    autoscaling_mimir_read_max_replicas: error 'you must set autoscaling_mimir_read_max_replicas in the _config',
  },

  newMimirReadScaledObject(name, query_scheduler_container, querier_max_concurrent, min_replicas, max_replicas):: self.newScaledObject(name, $._config.namespace, {
    min_replica_count: min_replicas,
    max_replica_count: max_replicas,

    triggers: [
      {
        metric_name: 'mimir_%s_hpa_%s' % [std.strReplace(name, '-', '_'), $._config.namespace],

        // Each query scheduler tracks *at regular intervals* the number of inflight requests
        // (both enqueued and processing queries) as a summary. With the following query we target
        // to have enough read workers to run the max observed inflight requests 75% of time.
        //
        // Instead of measuring it as instant query, we look at the max 75th percentile over the last
        // 5 minutes. This allows us to scale up quickly, but scale down slowly (and not too early
        // if within the next 5 minutes after a scale up we have further spikes).
        query: 'sum(max_over_time(cortex_query_scheduler_inflight_requests{container="%s",namespace="%s",quantile="0.75"}[5m]))' % [query_scheduler_container, $._config.namespace],

        // Target to utilize 75% querier workers on peak traffic (as measured by query above),
        // so we have 25% room for higher peaks.
        local targetUtilization = 0.75,
        threshold: '%d' % (querier_max_concurrent * targetUtilization),
      },
    ],
  }),

  //
  // Helper methods
  //

  local removeReplicasFromSpec = {
    spec+: {
      // Remove the "replicas" field so that Flux doesn't reconcile it.
      replicas+:: null,
    },
  },

  querier_scaled_object: if !$._config.autoscaling_mimir_read_enabled then null else
    self.newMimirReadScaledObject(
      name='mimir-read',
      query_scheduler_container='mimir-backend',
      querier_max_concurrent=$.querier_args['querier.max-concurrent'],
      min_replicas=$._config.autoscaling_mimir_read_min_replicas,
      max_replicas=$._config.autoscaling_mimir_read_max_replicas,
    ),

  mimir_read_deployment+: if !$._config.autoscaling_mimir_read_enabled then {} else
    removeReplicasFromSpec,

}
