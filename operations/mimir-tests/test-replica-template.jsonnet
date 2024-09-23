local rt = import 'mimir/replica-template.libsonnet';

rt {
  _config+:: {
    namespace: 'test',
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
