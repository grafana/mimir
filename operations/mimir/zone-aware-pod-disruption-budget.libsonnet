{
  // newZPDB creates new ZoneAwarePodDisruptionBudget resource.
  // name - a unique name within the namespace for this resource. eg ingester-rollout
  // rolloutGroup - the rollout-group label to match pods who are within this zpdb. eg ingester
  // maxUnavailable - the max unavailable pod count. ie 1
  // podNamePartitionRegex - use to enable partition awareness. A regex which can extract a partition name from a pod name. ie [a-z\\-]+-zone-[a-z]-([0-9]+)
  // podNameRegexGroup - use to define the regex grouping which holds the partition name. default is 1
  newZPDB(name, rolloutGroup, maxUnavailable, podNamePartitionRegex="", podNameRegexGroup=1):: {
    apiVersion: 'rollout-operator.grafana.com/v1',
    kind: 'ZoneAwarePodDisruptionBudget',
    metadata: {
      name: name,
      namespace: $._config.namespace,
      labels: {
        name: name,
      },
    },
    spec: ({
        maxUnavailable: maxUnavailable,
        selector: {
            matchLabels: {
              'rollout-group': rolloutGroup,
            },
        },
     } + if podNamePartitionRegex == "" then {} else {
        podNamePartitionRegex: podNamePartitionRegex,
        podNameRegexGroup: podNameRegexGroup,
     }
     ),
  },
}
