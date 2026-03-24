local rt =
  (import 'ksonnet-util/kausal.libsonnet') +
  (import 'mimir/common.libsonnet') +
  (import 'rollout-operator/rollout-operator.libsonnet') +
  (import 'mimir/rollout-operator.libsonnet');

rt {
  _config+:: {
    namespace: 'test',
    rollout_operator_enabled: true,
    rollout_operator_webhooks_enabled: true,
    replica_template_custom_resource_definition_enabled: true,

    // these are not relevant to this test. They are set because we by-pass the usual mimir/mimir.libsonnet imports and these fields are expected by mimir/rollout-operator.libsonnet
    ingest_storage_ingester_autoscaling_enabled: false,
    ingester_automated_downscale_v2_enabled: false,
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
