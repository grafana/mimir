local mimir = import 'mimir/mimir.libsonnet';

mimir {
  _config+:: {
    namespace: 'default',
    external_url: 'http://test',

    storage_backend: 'gcs',
    blocks_storage_bucket_name: 'blocks-bucket',
  },
}
{
  ['test_%s_%s_%g_%s_%s' % [with_cortex_prefix, with_ready_trigger, weight, container, pod_regex]]:
    $.newResourceScaledObject(
      'test_%s_%s_%g_%s_%s' % [with_cortex_prefix, with_ready_trigger, weight, container, pod_regex],
      cpu_requests='10',
      memory_requests='1Gi',
      min_replicas=5,
      max_replicas=10,
      cpu_target_utilization=0.8,
      memory_target_utilization=0.8,
      with_cortex_prefix=with_cortex_prefix,
      with_ready_trigger=with_ready_trigger,
      weight=weight,
      container_name=container,
      extra_matchers=(if pod_regex != '' then 'pod=~"%s"' % pod_regex else '')
    )
  for with_cortex_prefix in [false, true]
  for with_ready_trigger in [false, true]
  for weight in [0.7, 1, 1.5]
  for container in ['', 'test_container']
  for pod_regex in ['', 'pod-zone-a.*']
}
