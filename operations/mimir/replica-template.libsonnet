{
  _config+: {
    replica_template_custom_resource_definition_enabled: $._config.ingest_storage_ingester_autoscaling_enabled,
  },

  replica_template:: std.parseYaml(importstr 'replica-templates.yaml'),
  replica_template_custom_resource: if !$._config.replica_template_custom_resource_definition_enabled then null else $.replica_template,

  replicaTemplate(name, replicas=0, label_selector):: {
    apiVersion: 'rollout-operator.grafana.com/v1',
    kind: 'ReplicaTemplate',
    metadata: {
      name: name,
      namespace: $._config.namespace,
    },
    spec: {
      // HPA requires that label selector exists and is valid, but it will not be used for target type of AverageValue.
      labelSelector: label_selector,
    } + (
      if replicas <= 0 then {
        replicas:: null,  // Hide replicas field.
      } else {
        replicas: replicas,
      }
    ),
  },
}
