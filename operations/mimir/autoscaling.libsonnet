{
  _config+:: {
    autoscaling_querier_enabled: false,
    autoscaling_querier_min_replicas: error 'you must set autoscaling_querier_min_replicas in the _config',
    autoscaling_querier_max_replicas: error 'you must set autoscaling_querier_max_replicas in the _config',
    autoscaling_prometheus_url: 'http://prometheus.default:9090/prometheus',
  },

  ensure_query_scheduler_is_enabled:: if $._config.autoscaling_querier_enabled && !$._config.query_scheduler_enabled then
    error 'you must enable query-scheduler in order to use querier autoscaling'
  else
    null,

  // The ScaledObject resource is watched by the KEDA operator. When this resource is created, KEDA
  // creates the related HPA resource in the namespace. Likewise, then ScaledObject is deleted, KEDA
  // deletes the related HPA.
  newScaledObject(name, namespace, config):: {
    apiVersion: 'keda.sh/v1alpha1',
    kind: 'ScaledObject',
    metadata: {
      name: name,
      namespace: namespace,
    },
    spec: {
      scaleTargetRef: {
        name: name,
      },

      // The min/max replica count settings are reflected to HPA too.
      maxReplicaCount: config.max_replica_count,
      minReplicaCount: config.min_replica_count,

      // The pollingInterval defines how frequently KEDA will run the queries defined in triggers.
      // This setting is only effective when scaling from 0->N because the scale up from 0 is managed
      // by KEDA, while scaling from 1+->N is managed by HPA (and this setting doesn't apply to HPA).
      pollingInterval: 10,

      triggers: [
        {
          type: 'prometheus',
          metadata: {
            serverAddress: $._config.autoscaling_prometheus_url,
            query: config.query,

            // The metric name uniquely identify a metric in the KEDA metrics server.
            metricName: config.metric_name,

            // The threshold value is set to the HPA's targetAverageValue. The number of desired replicas is computed
            // by HPA as:
            //   desiredReplicas = ceil[currentReplicas * ( currentMetricValue / desiredMetricValue )]
            //
            // Where:
            // - currentMetricValue = <query result value> / <number of running pods>
            // - desiredMetricValue = <threshold>
            //
            // Read more:
            // https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/#algorithm-details
            threshold: config.threshold,
          },
        },
      ],
    },
  },

  newQuerierScaledObject(name, query_scheduler_container, querier_max_concurrent, min_replicas, max_replicas):: self.newScaledObject(name, $._config.namespace, {
    min_replica_count: min_replicas,
    max_replica_count: max_replicas,
    metric_name: 'cortex_%s_hpa_%s' % [std.strReplace(name, '-', '_'), $._config.namespace],

    // Each query scheduler tracks *at regular intervals* the number of inflight requests
    // (both enqueued and processing queries) as a summary. With the following query we target
    // to have enough querier workers to run the max observed inflight requests 75% of time.
    //
    // Instead of measuring it as instant query, we look at the max 75th percentile over the last
    // 5 minutes. This allows us to scale up quickly, but scale down slowly (and not too early
    // if within the next 5 minutes after a scale up we have further spikes).
    query: 'sum(max_over_time(cortex_query_scheduler_inflight_requests{container="%s",namespace="%s",quantile="0.75"}[5m]))' % [query_scheduler_container, $._config.namespace],

    // Target to utilize 75% querier workers on peak traffic (as measured by query above),
    // so we have 25% room for higher peaks.
    local targetUtilization = 0.75,
    threshold: '%d' % (querier_max_concurrent * targetUtilization),
  }),

  // When querier autoscaling is enabled the querier's "replicas" is missing from the spec
  // so the query sharding jsonnet can't compute the min number of replicas. To fix it, we
  // override it and we compute it based on the max number of replicas (worst case scenario).
  // This pessimistic logic can be removed once we'll migrate query-frontend to autoscaling too.
  local queryFrontendReplicas(querier_max_replicas) = {
    spec+: {
      local min_replicas = std.max(std.floor(0.2 * querier_max_replicas), 2),

      replicas: std.max(super.replicas, min_replicas),
    },
  },

  local removeReplicasFromSpec = {
    spec+: {
      // Remove the "replicas" field so that Flux doesn't reconcile it.
      replicas+:: null,
    },
  },

  //
  // Queriers
  //

  querier_scaled_object: if !$._config.autoscaling_querier_enabled then null else
    self.newQuerierScaledObject(
      name='querier',
      query_scheduler_container='query-scheduler',
      querier_max_concurrent=$.querier_args['querier.max-concurrent'],
      min_replicas=$._config.autoscaling_querier_min_replicas,
      max_replicas=$._config.autoscaling_querier_max_replicas,
    ),

  querier_deployment+: if !$._config.autoscaling_querier_enabled then {} else
    removeReplicasFromSpec,

  query_frontend_deployment+: if !$._config.query_sharding_enabled || !$._config.autoscaling_querier_enabled then {} else
    queryFrontendReplicas($._config.autoscaling_querier_max_replicas),
}
