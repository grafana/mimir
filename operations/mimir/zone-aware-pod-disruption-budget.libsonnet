{
  // newZPDB creates new ZoneAwarePodDisruptionBudget resource.
  // name is the rollout-group
  newZPDB(name, rolloutGroup, maxUnavailable, ingestStorageEnabled):: {
    apiVersion: 'rollout-operator.grafana.com/v1',
    kind: 'ZoneAwarePodDisruptionBudget',
    metadata: {
      name: name,
      namespace: $._config.namespace,
      labels: {
        name: name,
      },
    },
    spec: {
      maxUnavailable: maxUnavailable,
      selector: {
        matchLabels: {
          'rollout-group': rolloutGroup,
        },
      },
      podNamePartitionRegex: if (ingestStorageEnabled) then '[a-z\\-]+-zone-[a-z]-([0-9]+)' else '',
    },
  },
}
