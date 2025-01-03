{
  namespace:
    $.core.v1.namespace.new($._config.namespace),

  util+:: {
    local containerPort = $.core.v1.containerPort,
    local container = $.core.v1.container,

    defaultPorts::
      [
        containerPort.newNamed(name='http-metrics', containerPort=$._config.server_http_port),
        containerPort.newNamed(name='grpc', containerPort=9095),
      ],

    readinessProbe::
      container.mixin.readinessProbe.httpGet.withPath('/ready') +
      container.mixin.readinessProbe.httpGet.withPort($._config.server_http_port) +
      container.mixin.readinessProbe.withInitialDelaySeconds(15) +
      container.mixin.readinessProbe.withTimeoutSeconds(1),

    // parseCPU is used for conversion of Kubernetes CPU units to the corresponding float value of CPU cores.
    // Moreover, the function assumes the input is in a correct Kubernetes format, i.e., an integer, a float,
    // a string representation of an integer or a float, or a string containing a number ending with 'm'
    // representing a number of millicores.
    // Examples:
    // parseCPU(10) = parseCPU("10") = 10
    // parseCPU(4.5) = parse("4.5") = 4.5
    // parseCPU("3000m") = 3000 / 1000
    // parseCPU("3580m") = 3580 / 1000
    // parseCPU("3980.7m") = 3980.7 / 1000
    // parseCPU(0.5) = parse("0.5") = parse("500m") = 0.5
    parseCPU(v)::
      if std.isString(v) && std.endsWith(v, 'm') then std.parseJson(std.rstripChars(v, 'm')) / 1000
      else if std.isString(v) then std.parseJson(v)
      else if std.isNumber(v) then v
      else 0,

    // siToBytes is used to convert Kubernetes byte units to bytes.
    // Only works for limited set of SI prefixes: Ki, Mi, Gi, Ti.
    siToBytes(str):: (
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

    parseDuration(duration)::
      if std.endsWith(duration, 's') then
        std.parseInt(std.substr(duration, 0, std.length(duration) - 1))
      else if std.endsWith(duration, 'm') then
        std.parseInt(std.substr(duration, 0, std.length(duration) - 1)) * 60
      else if std.endsWith(duration, 'h') then
        std.parseInt(std.substr(duration, 0, std.length(duration) - 1)) * 3600
      else
        error 'unable to parse duration %s' % duration,

    formatDuration(seconds)::
      if seconds <= 60 then
        '%ds' % seconds
      else if seconds <= 3600 && seconds % 60 == 0 then
        '%dm' % (seconds / 60)
      else if seconds % 3600 == 0 then
        '%dh' % (seconds / 3600)
      else
        '%dm%ds' % [seconds / 60, seconds % 60],

    // Similar to std.prune() but only remove fields whose value is explicitly set to "null".
    removeNulls(obj)::
      if std.type(obj) == 'object' then {
        [key]: $.util.removeNulls(obj[key])
        for key in std.objectFields(obj)
        if obj[key] != null
      }
      else obj,
  },

  // Utility to create an headless service used to discover replicas of a Mimir deployment.
  newMimirDiscoveryService(name, deployment)::
    local service = $.core.v1.service;

    $.util.serviceFor(deployment, $._config.service_ignored_labels) +
    service.mixin.spec.withPublishNotReadyAddresses(true) +
    service.mixin.spec.withClusterIp('None') +
    service.mixin.metadata.withName(name),

  // Utility to create a PodDisruptionBudget for a Mimir deployment.
  newMimirPdb(deploymentName, maxUnavailable=1)::
    local podDisruptionBudget = $.policy.v1.podDisruptionBudget;
    local pdbName = deploymentName;

    podDisruptionBudget.new(pdbName) +
    podDisruptionBudget.mixin.metadata.withLabels({ name: pdbName }) +
    podDisruptionBudget.mixin.spec.selector.withMatchLabels({ name: deploymentName }) +
    podDisruptionBudget.mixin.spec.withMaxUnavailable(maxUnavailable),

  // Utility to create a StatefulSet for a Mimir component.
  //
  // By default, it uses the Parallel pod management policy which parallelly
  // scale up/down instances instead of starting them one by one. This does NOT
  // affect rolling updates: they will continue to be rolled out one by one
  // (the next pod will be rolled out once the previous is ready).
  newMimirStatefulSet(name, replicas, container, pvc, podManagementPolicy='Parallel')::
    local statefulSet = $.apps.v1.statefulSet;

    statefulSet.new(name, replicas, [container], pvc) +
    statefulSet.mixin.spec.withServiceName(name) +
    statefulSet.mixin.metadata.withNamespace($._config.namespace) +
    statefulSet.mixin.metadata.withLabels({ name: name }) +
    statefulSet.mixin.spec.template.metadata.withLabels({ name: name }) +
    statefulSet.mixin.spec.selector.withMatchLabels({ name: name }) +
    statefulSet.mixin.spec.updateStrategy.withType('RollingUpdate') +
    (if podManagementPolicy != null then statefulSet.mixin.spec.withPodManagementPolicy(podManagementPolicy) else {}) +
    (if !std.isObject($._config.node_selector) then {} else statefulSet.mixin.spec.template.spec.withNodeSelectorMixin($._config.node_selector)),

  // Utility to create the default pod topology spread constraint used by Mimir.
  newMimirSpreadTopology(name, maxSkew)::
    local deployment = $.apps.v1.deployment;
    local topologySpreadConstraints = $.core.v1.topologySpreadConstraint;

    deployment.spec.template.spec.withTopologySpreadConstraints(
      // Evenly spread replicas among available nodes.
      topologySpreadConstraints.labelSelector.withMatchLabels({ name: name }) +
      topologySpreadConstraints.withTopologyKey('kubernetes.io/hostname') +
      topologySpreadConstraints.withWhenUnsatisfiable('ScheduleAnyway') +
      topologySpreadConstraints.withMaxSkew(maxSkew),
    ),

  newMimirNodeAffinityMatchers(nodeAffinityMatchers)::
    local deployment = $.apps.v1.deployment;

    if std.length(nodeAffinityMatchers) == 0 then {} else (
      deployment.spec.template.spec.affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution.withNodeSelectorTerms(
        local sorted = std.sort(nodeAffinityMatchers, function(x) x.key);
        $.core.v1.nodeSelectorTerm.withMatchExpressions(sorted)
      )
    ),

  newMimirNodeAffinityMatcherAZ(az):: {
    key: 'topology.kubernetes.io/zone',
    operator: 'In',
    values: [az],
  },

  mimirVolumeMounts::
    $.util.volumeMounts(
      [$.util.volumeMountItem(name, $._config.configmaps[name]) for name in std.objectFieldsAll($._config.configmaps)]
    ),

  mimirRuntimeConfigFile:: {
    'runtime-config.file': std.join(',', $._config.runtime_config_files),
  },
}
