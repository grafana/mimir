{
  local service = $.core.v1.service,
  local podDisruptionBudget = $.policy.v1.podDisruptionBudget,

  newMimirRolloutGroupPDB(rolloutGroup, maxUnavailable)::
    podDisruptionBudget.new('%s-rollout-pdb' % rolloutGroup) +
    podDisruptionBudget.mixin.metadata.withLabels({ name: '%s-rollout-pdb' % rolloutGroup }) +
    podDisruptionBudget.mixin.spec.selector.withMatchLabels({ 'rollout-group': rolloutGroup }) +
    podDisruptionBudget.mixin.spec.withMaxUnavailable(maxUnavailable),

  // Creates a service resolving to all replicas in a rollout group.
  newMimirRolloutGroupService(rolloutGroup, deployments, ignored_labels=[])::
    local name = rolloutGroup;
    local selector = { 'rollout-group': rolloutGroup };
    local container = $.core.v1.container;
    local service = $.core.v1.service;
    local servicePort = $.core.v1.servicePort;

    // Find all (unique) ports exposed by all containers in the input deployments.
    // The std.set() function filters out all duplicates.
    local exposedPorts = std.set([
      {
        containerName: c.name,
        containerPort: p.containerPort,
        portName: p.name,
        portProtocol: if std.objectHas(p, 'protocol') then p.protocol else null,
      }
      for d in deployments
      for c in d.spec.template.spec.containers
      for p in (c + container.withPortsMixin([])).ports
    ], function(entry) entry.containerPort);

    // Generate the definition for the ports to expose from the service.
    local ports = [
      servicePort.newNamed(
        name=('%(container)s-%(port)s' % { container: port.containerName, port: port.portName }),
        port=port.containerPort,
        targetPort=port.containerPort
      ) +
      (if port.portProtocol != null then servicePort.withProtocol(port.portProtocol) else {})
      for port in exposedPorts
    ];

    service.new(name, selector, ports) +
    service.mixin.metadata.withLabels({ name: name }) +
    // Override the selector because the service.new() automatically inject the name label matcher too.
    { spec+: { selector: selector } },
}
