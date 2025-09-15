local rt = import 'rollout-operator/rollout-operator.libsonnet';

rt {
  _config+:: {
    namespace: 'test',
    rollout_operator_webhooks_enabled: true,
    replica_template_custom_resource_definition_enabled: true,
  },
}
{
  ['rt_%d_%s' % [replicas, label_selector]]:
    $.replicaTemplate(
      'rt_%d_%s' % [replicas, label_selector],
      replicas,
      label_selector,
    )
  for replicas in [-10, 0, 1, 10]
  for label_selector in ['a=b', 'name=unused']
}
