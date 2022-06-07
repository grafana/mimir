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
    local podDisruptionBudget = $.policy.v1beta1.podDisruptionBudget;
    local pdbName = '%s-pdb' % deploymentName;

    podDisruptionBudget.new() +
    podDisruptionBudget.mixin.metadata.withName(pdbName) +
    podDisruptionBudget.mixin.metadata.withLabels({ name: pdbName }) +
    podDisruptionBudget.mixin.spec.selector.withMatchLabels({ name: deploymentName }) +
    podDisruptionBudget.mixin.spec.withMaxUnavailable(maxUnavailable),
}
