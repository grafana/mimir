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
    local pdbName = '%s-pdb' % deploymentName;

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
    statefulSet.mixin.spec.template.spec.securityContext.withRunAsUser(0) +
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

  mimirVolumeMounts::
    $.util.volumeMounts(
      [$.util.volumeMountItem(name, $._config.configmaps[name]) for name in std.objectFieldsAll($._config.configmaps)]
    ),

  mimirRuntimeConfigFile:: {
    'runtime-config.file': std.join(',', $._config.runtime_config_files),
  },
}
