{
  _config+:: {
    autoscaling_querier_enabled: false,
    autoscaling_querier_min_replicas: error 'you must set autoscaling_querier_min_replicas in the _config',
    autoscaling_querier_max_replicas: error 'you must set autoscaling_querier_max_replicas in the _config',

    autoscaling_ruler_querier_enabled: false,
    autoscaling_ruler_querier_min_replicas: error 'you must set autoscaling_ruler_querier_min_replicas in the _config',
    autoscaling_ruler_querier_max_replicas: error 'you must set autoscaling_ruler_querier_max_replicas in the _config',

    autoscaling_distributor_enabled: false,
    autoscaling_distributor_min_replicas: error 'you must set autoscaling_distributor_min_replicas in the _config',
    autoscaling_distributor_max_replicas: error 'you must set autoscaling_distributor_max_replicas in the _config',

    autoscaling_ruler_enabled: false,
    autoscaling_ruler_min_replicas: error 'you must set autoscaling_ruler_min_replicas in the _config',
    autoscaling_ruler_max_replicas: error 'you must set autoscaling_ruler_max_replicas in the _config',

    autoscaling_prometheus_url: 'http://prometheus.default:9090/prometheus',
  },

  assert !$._config.autoscaling_querier_enabled || $._config.query_scheduler_enabled
         : 'you must enable query-scheduler in order to use querier autoscaling',

  // KEDA defaults to apiVersion: apps/v1 and kind: Deployment for scaleTargetRef, this function
  // avoids specifying apiVersion and kind if they are at their defaults.
  local scaleTargetRef(apiVersion, kind, name) = if apiVersion != 'apps/v1' || kind != 'Deployment' then {
    apiVersion: apiVersion,
    kind: kind,
    name: name,
  } else {
    name: name,
  },

  // metricWithWeight will multiply the metric by the weight provided if it's different from 1.
  local metricWithWeight(metric, weight) =
    if weight == 1 then metric
    else '%s * %.2f' % [metric, weight],


  // replicaswithWeight will return the portion of replicas that should be applied with the weight provided.
  local replicasWithWeight(replicas, weight) =
    if weight <= 0.5 then std.ceil(replicas * weight) else std.floor(replicas * weight),

  // The ScaledObject resource is watched by the KEDA operator. When this resource is created, KEDA
  // creates the related HPA resource in the namespace. Likewise, then ScaledObject is deleted, KEDA
  // deletes the related HPA.
  newScaledObject(name, namespace, config, apiVersion='apps/v1', kind='Deployment'):: {
    apiVersion: 'keda.sh/v1alpha1',
    kind: 'ScaledObject',
    metadata: {
      name: name,
      namespace: namespace,
    },
    spec: {
      scaleTargetRef: scaleTargetRef(apiVersion, kind, name),

      // The min/max replica count settings are reflected to HPA too.
      minReplicaCount: config.min_replica_count,
      maxReplicaCount: config.max_replica_count,

      // The pollingInterval defines how frequently KEDA will run the queries defined in triggers.
      // This setting is only effective when scaling from 0->N because the scale up from 0 is managed
      // by KEDA, while scaling from 1+->N is managed by HPA (and this setting doesn't apply to HPA).
      pollingInterval: 10,

      advanced: {
        horizontalPodAutoscalerConfig: {
          behavior: {
            scaleDown: {
              policies: [{
                // Allow to scale down up to 10% of pods every 1m. This prevents from suddenly scaling to minReplicas
                // when Prometheus comes back up after a long outage (longer than stabilizationWindowSeconds=300s)
                type: 'Percent',
                value: 10,
                periodSeconds: 60,
              }],
            },
          },
        },
      },

      triggers: [
        {
          type: 'prometheus',
          metadata: {
            serverAddress: $._config.autoscaling_prometheus_url,
            query: trigger.query,

            // The metric name uniquely identifies a metric in the KEDA metrics server.
            metricName: trigger.metric_name,

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
            //
            // We also have to ensure that the threshold is an integer (represented as a string)
            threshold: std.toString(std.parseInt(trigger.threshold)),
          },
        }
        for trigger in config.triggers
      ],
    },
  },

  // newQuerierScaledObject will create a scaled object for the querier component with the given name.
  // `weight` param can be used to control just a portion of the expected queriers with the generated scaled object.
  // For example, if you run multiple querier deployments on different node types, you can use the weight to control which portion of them runs on which nodes.
  // The weight is a number between 0 and 1, where 1 means 100% of the expected queriers.
  newQuerierScaledObject(name, query_scheduler_container, querier_max_concurrent, min_replicas, max_replicas, weight=1):: self.newScaledObject(name, $._config.namespace, {
    min_replica_count: replicasWithWeight(min_replicas, weight),
    max_replica_count: replicasWithWeight(max_replicas, weight),

    triggers: [
      {
        metric_name: 'cortex_%s_hpa_%s' % [std.strReplace(name, '-', '_'), $._config.namespace],

        // Each query scheduler tracks *at regular intervals* the number of inflight requests
        // (both enqueued and processing queries) as a summary. With the following query we target
        // to have enough querier workers to run the max observed inflight requests 75% of time.
        //
        // Instead of measuring it as instant query, we look at the max 75th percentile over the last
        // 5 minutes. This allows us to scale up quickly, but scale down slowly (and not too early
        // if within the next 5 minutes after a scale up we have further spikes).
        query: metricWithWeight('sum(max_over_time(cortex_query_scheduler_inflight_requests{container="%s",namespace="%s",quantile="0.75"}[5m]))' % [query_scheduler_container, $._config.namespace], weight),

        // Target to utilize 75% querier workers on peak traffic (as measured by query above),
        // so we have 25% room for higher peaks.
        local targetUtilization = 0.75,
        threshold: '%d' % (querier_max_concurrent * targetUtilization),
      },
    ],
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


  //
  // Helper methods
  //

  local removeReplicasFromSpec = {
    spec+: {
      // Remove the "replicas" field so that Flux doesn't reconcile it.
      replicas+:: null,
    },
  },

  local siToBytes(str) = (
    // Simple method to convert binary multiples.
    // Only works for limited set of SI prefixes (as below).

    // Utility converting the input to a (potentially decimal) number of bytes
    local siToBytesDecimal(str) = (
      if std.endsWith(str, 'Ki') then (
        std.parseJson(std.rstripChars(str, 'Ki')) * std.pow(2, 10)
      ) else if std.endsWith(str, 'Mi') then (
        std.parseJson(std.rstripChars(str, 'Mi')) * std.pow(2, 20)
      ) else if std.endsWith(str, 'Gi') then (
        std.parseJson(std.rstripChars(str, 'Gi')) * std.pow(2, 30)
      ) else if std.endsWith(str, 'Ti') then (
        std.parseJson(std.rstripChars(str, 'Ti')) * std.pow(2, 40)
      ) else (
        std.parseJson(str)
      )
    );

    // Round down to nearest integer
    std.floor(siToBytesDecimal(str))
  ),

  local cpuToMilliCPUInt(str) = (
    // Converts any CPU requests to millicores. (eg 0.5 = 500m)
    // This is due to KEDA requiring an integer.

    if (std.isString(str) && std.endsWith(str, 'm')) then (
      std.rstripChars(str, 'm')
    ) else (
      std.parseJson(str + '') * 1000
    )
  ),

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

  querier_deployment: overrideSuperIfExists(
    'querier_deployment',
    if !$._config.autoscaling_querier_enabled then {} else removeReplicasFromSpec
  ),

  query_frontend_deployment: overrideSuperIfExists(
    'query_frontend_deployment',
    if !$._config.query_sharding_enabled || !$._config.autoscaling_querier_enabled then {} else
      queryFrontendReplicas($._config.autoscaling_querier_max_replicas)
  ),

  //
  // Ruler-queriers
  //

  // newRulerQuerierScaledObject will create a scaled object for the ruler-querier component with the given name.
  // `weight` param works in the same way as in `newQuerierScaledObject`, see docs there.
  newRulerQuerierScaledObject(name, querier_cpu_requests, min_replicas, max_replicas, weight=1):: self.newScaledObject(name, $._config.namespace, {
    min_replica_count: replicasWithWeight(min_replicas, weight),
    max_replica_count: replicasWithWeight(max_replicas, weight),

    triggers: [
      {
        metric_name: 'cortex_%s_hpa_%s' % [std.strReplace(name, '-', '_'), $._config.namespace],

        // Due to the more predicatable nature of the ruler-querier workload we can scale on CPU usage.
        // To scale out relatively quickly, but scale in slower, we look at the average CPU utilization per ruler-querier over 5m (rolling window)
        // and then we pick the highest value over the last 15m.
        query: metricWithWeight('max_over_time(sum(rate(container_cpu_usage_seconds_total{container="%s",namespace="%s"}[5m]))[15m:]) * 1000' % [name, $._config.namespace], weight),

        // threshold is expected to be a string.
        threshold: std.toString(cpuToMilliCPUInt(querier_cpu_requests)),
      },
    ],
  }),

  ruler_querier_scaled_object: if !$._config.autoscaling_ruler_querier_enabled || !$._config.ruler_remote_evaluation_enabled then null else
    $.newRulerQuerierScaledObject(
      name='ruler-querier',
      querier_cpu_requests=$.ruler_querier_container.resources.requests.cpu,
      min_replicas=$._config.autoscaling_ruler_querier_min_replicas,
      max_replicas=$._config.autoscaling_ruler_querier_max_replicas,
    ),

  ruler_querier_deployment: overrideSuperIfExists(
    'ruler_querier_deployment',
    if !$._config.autoscaling_ruler_querier_enabled then {} else removeReplicasFromSpec
  ),

  ruler_query_frontend_deployment: overrideSuperIfExists(
    'ruler_query_frontend_deployment',
    if !$._config.query_sharding_enabled || !$._config.autoscaling_ruler_querier_enabled then {} else
      queryFrontendReplicas($._config.autoscaling_ruler_querier_max_replicas)
  ),

  //
  // Distributors
  //

  newDistributorScaledObject(name, distributor_cpu_requests, distributor_memory_requests, min_replicas, max_replicas):: self.newScaledObject(name, $._config.namespace, {
    min_replica_count: min_replicas,
    max_replica_count: max_replicas,

    triggers: [
      {
        metric_name: 'cortex_%s_cpu_hpa_%s' % [std.strReplace(name, '-', '_'), $._config.namespace],

        // To scale out relatively quickly, but scale in slower, we look at the average CPU utilization per distributor over 5m (rolling window)
        // and then we pick the highest value over the last 15m.
        // Multiply by 1000 to get the result in millicores. This is due to KEDA only working with Ints.
        query: 'max_over_time(sum(rate(container_cpu_usage_seconds_total{container="%s",namespace="%s"}[5m]))[15m:]) * 1000' % [name, $._config.namespace],

        // threshold is expected to be a string.
        threshold: std.toString(cpuToMilliCPUInt(distributor_cpu_requests)),
      },
      {
        metric_name: 'cortex_%s_memory_hpa_%s' % [std.strReplace(name, '-', '_'), $._config.namespace],

        // To scale out relatively quickly, but scale in slower, we look at the max memory utilization across all distributors over 15m.
        query: 'max_over_time(sum(container_memory_working_set_bytes{container="%s",namespace="%s"})[15m:])' % [name, $._config.namespace],

        // threshold is expected to be a string
        threshold: std.toString(siToBytes(distributor_memory_requests)),
      },
    ],
  }),

  distributor_scaled_object: if !$._config.autoscaling_distributor_enabled then null else
    $.newDistributorScaledObject(
      name='distributor',
      distributor_cpu_requests=$.distributor_container.resources.requests.cpu,
      distributor_memory_requests=$.distributor_container.resources.requests.memory,
      min_replicas=$._config.autoscaling_distributor_min_replicas,
      max_replicas=$._config.autoscaling_distributor_max_replicas,
    ),

  distributor_deployment: overrideSuperIfExists(
    'distributor_deployment',
    if !$._config.autoscaling_distributor_enabled then {} else removeReplicasFromSpec
  ),

  // Ruler

  local newRulerScaledObject(name) = self.newScaledObject(
    name, $._config.namespace, {
      min_replica_count: $._config.autoscaling_ruler_min_replicas,
      max_replica_count: $._config.autoscaling_ruler_max_replicas,
      // Be aware that the default value for the trigger "metricType" field is "AverageValue"
      // (see https://keda.sh/docs/2.9/concepts/scaling-deployments/#triggers), which means that KEDA will
      // determine the target number of replicas by dividing the metric value by the threshold. This means in practice
      // that we can sum together the values for the different replicas in our queries.
      triggers: [
        {
          metric_name: '%s_cpu_hpa_%s' % [std.strReplace(name, '-', '_'), $._config.namespace],

          // To scale out relatively quickly, but scale in slower, we look at the average CPU utilization per ruler over 5m (rolling window)
          // and then we pick the highest value over the last 15m.
          // Multiply by 1000 to get the result in millicores. This is due to KEDA only working with ints.
          query: 'max_over_time(sum(rate(container_cpu_usage_seconds_total{container="%s",namespace="%s"}[5m]))[15m:]) * 1000' % [
            name,
            $._config.namespace,
          ],
          // Threshold is expected to be a string
          threshold: std.toString(cpuToMilliCPUInt($.ruler_container.resources.requests.cpu)),
        },
        {
          metric_name: '%s_memory_hpa_%s' % [std.strReplace(name, '-', '_'), $._config.namespace],

          // To scale out relatively quickly, but scale in slower, we look at the max memory utilization across all rulers over 15m.
          query: 'max_over_time(sum(container_memory_working_set_bytes{container="%s",namespace="%s"})[15m:])' % [
            name,
            $._config.namespace,
          ],

          // Threshold is expected to be a string
          threshold: std.toString(siToBytes($.ruler_container.resources.requests.memory)),
        },
      ],
    },
  ),

  ruler_scaled_object: if !$._config.autoscaling_ruler_enabled then null else newRulerScaledObject(
    name='ruler',
  ),

  ruler_deployment: overrideSuperIfExists(
    'ruler_deployment',
    if !$._config.autoscaling_ruler_enabled then {} else removeReplicasFromSpec
  ),

  // Utility used to override a field only if exists in super.
  local overrideSuperIfExists(name, override) = if !( name in super) || super[name] == null || super[name] == {} then null else
    super[name] + override,
}
