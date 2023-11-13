{
  _config+:: {
    autoscaling_prometheus_url: 'http://prometheus.default:9090/prometheus',

    autoscaling_querier_enabled: false,
    autoscaling_querier_min_replicas: error 'you must set autoscaling_querier_min_replicas in the _config',
    autoscaling_querier_max_replicas: error 'you must set autoscaling_querier_max_replicas in the _config',
    autoscaling_querier_target_utilization: 0.75,  // Target to utilize 75% querier workers on peak traffic, so we have 25% room for higher peaks.

    autoscaling_ruler_querier_enabled: false,
    autoscaling_ruler_querier_min_replicas: error 'you must set autoscaling_ruler_querier_min_replicas in the _config',
    autoscaling_ruler_querier_max_replicas: error 'you must set autoscaling_ruler_querier_max_replicas in the _config',
    autoscaling_ruler_querier_cpu_target_utilization: 1,
    autoscaling_ruler_querier_memory_target_utilization: 1,

    autoscaling_distributor_enabled: false,
    autoscaling_distributor_min_replicas: error 'you must set autoscaling_distributor_min_replicas in the _config',
    autoscaling_distributor_max_replicas: error 'you must set autoscaling_distributor_max_replicas in the _config',
    autoscaling_distributor_cpu_target_utilization: 1,
    autoscaling_distributor_memory_target_utilization: 1,

    autoscaling_ruler_enabled: false,
    autoscaling_ruler_min_replicas: error 'you must set autoscaling_ruler_min_replicas in the _config',
    autoscaling_ruler_max_replicas: error 'you must set autoscaling_ruler_max_replicas in the _config',
    autoscaling_ruler_cpu_target_utilization: 1,
    autoscaling_ruler_memory_target_utilization: 1,

    autoscaling_query_frontend_enabled: false,
    autoscaling_query_frontend_min_replicas: error 'you must set autoscaling_query_frontend_min_replicas in the _config',
    autoscaling_query_frontend_max_replicas: error 'you must set autoscaling_query_frontend_max_replicas in the _config',
    autoscaling_query_frontend_cpu_target_utilization: 0.75,  // Query-frontend CPU utilization can be very spiky based on actual queries.
    autoscaling_query_frontend_memory_target_utilization: 1,

    autoscaling_ruler_query_frontend_enabled: false,
    autoscaling_ruler_query_frontend_min_replicas: error 'you must set autoscaling_ruler_query_frontend_min_replicas in the _config',
    autoscaling_ruler_query_frontend_max_replicas: error 'you must set autoscaling_ruler_query_frontend_max_replicas in the _config',
    autoscaling_ruler_query_frontend_cpu_target_utilization: 1,
    autoscaling_ruler_query_frontend_memory_target_utilization: 1,

    autoscaling_alertmanager_enabled: false,
    autoscaling_alertmanager_min_replicas: error 'you must set autoscaling_alertmanager_min_replicas in the _config',
    autoscaling_alertmanager_max_replicas: error 'you must set autoscaling_alertmanager_max_replicas in the _config',
    autoscaling_alertmanager_cpu_target_utilization: 1,
    autoscaling_alertmanager_memory_target_utilization: 1,
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

  // getScaleDownPeriod will return the scale down period expressed in seconds.
  // If the config doesn't have a scale_down_period set, the default value of 60 seconds will be returned.
  local getScaleDownPeriod(config) =
    if std.objectHas(config, 'scale_down_period') then config.scale_down_period else 60,

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
                periodSeconds: getScaleDownPeriod(config),
              }],
            },
          },
        },
      },

      triggers: [
        {
          name: trigger.metric_name,
          type: 'prometheus',
          metadata: {
            serverAddress: $._config.autoscaling_prometheus_url,
            query: trigger.query,

            // The metric name uniquely identifies a metric in the KEDA metrics server.
            // This is deprecatd in KEDA 2.10, and is instead set to a default based on the trigger type (e.g. `s0-prometheus`).
            // Instead we use the metric_name as the name for the trigger (above). This appears as the `scaler` label on `keda_scaler_metrics_value`.
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
        } + (
          // Be aware that the default value for the trigger "metricType" field is "AverageValue"
          // (see https://keda.sh/docs/2.9/concepts/scaling-deployments/#triggers), which means that KEDA will
          // determine the target number of replicas by dividing the metric value by the threshold. This means in practice
          // that we can sum together the values for the different replicas in our queries.
          if std.objectHas(trigger, 'metric_type') then { metricType: trigger.metric_type } else {}
        )
        for trigger in config.triggers
      ],
    },
  },

  // newQuerierScaledObject will create a scaled object for the querier component with the given name.
  // `weight` param can be used to control just a portion of the expected queriers with the generated scaled object.
  // For example, if you run multiple querier deployments on different node types, you can use the weight to control which portion of them runs on which nodes.
  // The weight is a number between 0 and 1, where 1 means 100% of the expected queriers.
  newQuerierScaledObject(name, query_scheduler_container, querier_max_concurrent, min_replicas, max_replicas, target_utilization, weight=1):: self.newScaledObject(name, $._config.namespace, {
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

        threshold: '%d' % std.floor(querier_max_concurrent * target_utilization),
      },
    ],
  }),

  // To scale out relatively quickly, but scale in slower, we look at the average CPU utilization
  // per replica over 5m (rolling window) and then we pick the highest value over the last 15m.
  // We multiply by 1000 to get the result in millicores. This is due to HPA only working with ints.
  // The "up" metrics correctly handles the stale marker when the pod is terminated, while it’s not the
  // case for the cAdvisor metrics. By intersecting these 2 metrics, we only look the CPU utilization
  // of containers there are running at any given time, without suffering the PromQL lookback period.

  local cpuHPAQuery = |||
    max_over_time(
      sum(
        sum by (pod) (rate(container_cpu_usage_seconds_total{container="%(container)s",namespace="%(namespace)s"}[5m]))
        and
        max by (pod) (up{container="%(container)s",namespace="%(namespace)s"}) > 0
      )[15m:]
    ) * 1000
  |||,

  // To scale out relatively quickly, but scale in slower, we look at the max memory utilization across
  // all replicas over 15m.
  // The "up" metrics correctly handles the stale marker when the pod is terminated, while it’s not the
  // case for the cAdvisor metrics. By intersecting these 2 metrics, we only look the memory utilization
  // of containers there are running at any given time, without suffering the PromQL lookback period.
  // If a pod is terminated because it OOMs, we still want to scale up -- add the memory resource request of OOMing
  //  pods to the memory metric calculation.
  local memoryHPAQuery = |||
    max_over_time(
      sum(
        (
          sum by (pod) (container_memory_working_set_bytes{container="%(container)s",namespace="%(namespace)s"})
          and
          max by (pod) (up{container="%(container)s",namespace="%(namespace)s"}) > 0
        ) or vector(0)
      )[15m:]
    )
    +
    sum(
      sum by (pod) (max_over_time(kube_pod_container_resource_requests{container="%(container)s", namespace="%(namespace)s", resource="memory"}[15m]))
      and
      max by (pod) (changes(kube_pod_container_status_restarts_total{container="%(container)s", namespace="%(namespace)s"}[15m]) > 0)
      and
      max by (pod) (kube_pod_container_status_last_terminated_reason{container="%(container)s", namespace="%(namespace)s", reason="OOMKilled"})
      or vector(0)
    )
  |||,

  newResourceScaledObject(
    name,
    cpu_requests,
    memory_requests,
    min_replicas,
    max_replicas,
    cpu_target_utilization,
    memory_target_utilization,
    with_cortex_prefix=false,
    weight=1,
    scale_down_period=null,
  ):: self.newScaledObject(
    name, $._config.namespace, {
      min_replica_count: replicasWithWeight(min_replicas, weight),
      max_replica_count: replicasWithWeight(max_replicas, weight),

      [if scale_down_period != null then 'scale_down_period']: scale_down_period,

      triggers: [
        {
          metric_name: '%s%s_cpu_hpa_%s' %
                       ([if with_cortex_prefix then 'cortex_' else ''] + [std.strReplace(name, '-', '_'), $._config.namespace]),

          query: metricWithWeight(cpuHPAQuery % {
            container: name,
            namespace: $._config.namespace,
          }, weight),

          // Threshold is expected to be a string
          threshold: std.toString(std.floor(cpuToMilliCPUInt(cpu_requests) * cpu_target_utilization)),
        },
        {
          metric_name: '%s%s_memory_hpa_%s' %
                       ([if with_cortex_prefix then 'cortex_' else ''] + [std.strReplace(name, '-', '_'), $._config.namespace]),

          query: memoryHPAQuery % {
            container: name,
            namespace: $._config.namespace,
          },

          // Threshold is expected to be a string
          threshold: std.toString(std.floor($.util.siToBytes(memory_requests) * memory_target_utilization)),
        },
      ],
    },
  ),

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

  removeReplicasFromSpec:: {
    spec+: {
      // Remove the "replicas" field so that Flux doesn't reconcile it.
      replicas+:: null,
    },
  },

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
      target_utilization=$._config.autoscaling_querier_target_utilization,
    ),

  querier_deployment: overrideSuperIfExists(
    'querier_deployment',
    if !$._config.autoscaling_querier_enabled then {} else $.removeReplicasFromSpec
  ),

  query_frontend_scaled_object: if !$._config.autoscaling_query_frontend_enabled then null else
    $.newResourceScaledObject(
      name='query-frontend',
      cpu_requests=$.query_frontend_container.resources.requests.cpu,
      memory_requests=$.query_frontend_container.resources.requests.memory,
      min_replicas=$._config.autoscaling_query_frontend_min_replicas,
      max_replicas=$._config.autoscaling_query_frontend_max_replicas,
      cpu_target_utilization=$._config.autoscaling_query_frontend_cpu_target_utilization,
      memory_target_utilization=$._config.autoscaling_query_frontend_memory_target_utilization,
    ),

  query_frontend_deployment: overrideSuperIfExists(
    'query_frontend_deployment',
    if $._config.autoscaling_query_frontend_enabled then $.removeReplicasFromSpec else
      if ($._config.query_sharding_enabled && $._config.autoscaling_querier_enabled) then
        queryFrontendReplicas($._config.autoscaling_querier_max_replicas) else
        {}
  ),

  //
  // Ruler-queriers
  //

  ruler_querier_scaled_object: if !$._config.autoscaling_ruler_querier_enabled || !$._config.ruler_remote_evaluation_enabled then null else
    $.newResourceScaledObject(
      name='ruler-querier',
      cpu_requests=$.ruler_querier_container.resources.requests.cpu,
      memory_requests=$.ruler_querier_container.resources.requests.memory,
      min_replicas=$._config.autoscaling_ruler_querier_min_replicas,
      max_replicas=$._config.autoscaling_ruler_querier_max_replicas,
      cpu_target_utilization=$._config.autoscaling_ruler_querier_cpu_target_utilization,
      memory_target_utilization=$._config.autoscaling_ruler_querier_memory_target_utilization,
    ),

  ruler_querier_deployment: overrideSuperIfExists(
    'ruler_querier_deployment',
    if !$._config.autoscaling_ruler_querier_enabled then {} else $.removeReplicasFromSpec
  ),

  ruler_query_frontend_scaled_object: if !$._config.autoscaling_ruler_query_frontend_enabled || !$._config.ruler_remote_evaluation_enabled then null else
    $.newResourceScaledObject(
      name='ruler-query-frontend',
      cpu_requests=$.ruler_query_frontend_container.resources.requests.cpu,
      memory_requests=$.ruler_query_frontend_container.resources.requests.memory,
      min_replicas=$._config.autoscaling_ruler_query_frontend_min_replicas,
      max_replicas=$._config.autoscaling_ruler_query_frontend_max_replicas,
      cpu_target_utilization=$._config.autoscaling_ruler_query_frontend_cpu_target_utilization,
      memory_target_utilization=$._config.autoscaling_ruler_query_frontend_memory_target_utilization,
    ),

  ruler_query_frontend_deployment: overrideSuperIfExists(
    'ruler_query_frontend_deployment',
    if $._config.autoscaling_ruler_query_frontend_enabled then $.removeReplicasFromSpec else
      if ($._config.query_sharding_enabled && $._config.autoscaling_ruler_querier_enabled) then
        queryFrontendReplicas($._config.autoscaling_ruler_querier_max_replicas) else
        {}
  ),

  distributor_scaled_object: if !$._config.autoscaling_distributor_enabled then null else
    $.newResourceScaledObject(
      name='distributor',
      cpu_requests=$.distributor_container.resources.requests.cpu,
      memory_requests=$.distributor_container.resources.requests.memory,
      min_replicas=$._config.autoscaling_distributor_min_replicas,
      max_replicas=$._config.autoscaling_distributor_max_replicas,
      cpu_target_utilization=$._config.autoscaling_distributor_cpu_target_utilization,
      memory_target_utilization=$._config.autoscaling_distributor_memory_target_utilization,
      with_cortex_prefix=true,
    ),

  distributor_deployment: overrideSuperIfExists(
    'distributor_deployment',
    if !$._config.autoscaling_distributor_enabled then {} else $.removeReplicasFromSpec
  ),

  ruler_scaled_object: if !$._config.autoscaling_ruler_enabled then null else $.newResourceScaledObject(
    name='ruler',
    cpu_requests=$.ruler_container.resources.requests.cpu,
    memory_requests=$.ruler_container.resources.requests.memory,
    min_replicas=$._config.autoscaling_ruler_min_replicas,
    max_replicas=$._config.autoscaling_ruler_max_replicas,
    cpu_target_utilization=$._config.autoscaling_ruler_cpu_target_utilization,
    memory_target_utilization=$._config.autoscaling_ruler_memory_target_utilization,
    // To guarantee rule evaluation without any omissions, it is imperative to avoid the frequent scaling up and
    // down of the ruler. As a result, we have made the decision to set the scale down period to 600 seconds.
    scale_down_period=600,
  ),

  ruler_deployment: overrideSuperIfExists(
    'ruler_deployment',
    if !$._config.autoscaling_ruler_enabled then {} else $.removeReplicasFromSpec
  ),

  // Utility used to override a field only if exists in super.
  local overrideSuperIfExists(name, override) = if !( name in super) || super[name] == null || super[name] == {} then null else
    super[name] + override,

  alertmanager_scaled_object: if !$._config.autoscaling_alertmanager_enabled then null else
    $.newResourceScaledObject(
      name='alertmanager',
      cpu_requests=$.alertmanager_container.resources.requests.cpu,
      memory_requests=$.alertmanager_container.resources.requests.memory,
      min_replicas=$._config.autoscaling_alertmanager_min_replicas,
      max_replicas=$._config.autoscaling_alertmanager_max_replicas,
      cpu_target_utilization=$._config.autoscaling_alertmanager_cpu_target_utilization,
      memory_target_utilization=$._config.autoscaling_alertmanager_memory_target_utilization,
      with_cortex_prefix=true,
    ) + {
      spec+: {
        scaleTargetRef+: {
          kind: 'StatefulSet',
        },
      },
    },

  alertmanager_statefulset: overrideSuperIfExists(
    'alertmanager_statefulset',
    if !$._config.autoscaling_alertmanager_enabled then {} else $.removeReplicasFromSpec
  ),
}
