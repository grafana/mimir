local mimir = import 'mimir/mimir.libsonnet';

mimir {
  _config+:: {
    namespace: 'default',
    external_url: 'http://test',

    storage_backend: 'gcs',
    blocks_storage_bucket_name: 'blocks-bucket',

    node_selector: {
      workload: 'mimir',
    },
  },

  local nodeAffinityMatchers = [
    { key: 'topology.kubernetes.io/region', operator: 'In', values: ['us-east-1'] },
    { key: 'topology.kubernetes.io/zone', operator: 'In', values: ['us-east-1a', 'us-east-1b'] },
  ],

  alertmanager_node_affinity_matchers:: nodeAffinityMatchers,
  compactor_node_affinity_matchers:: nodeAffinityMatchers,
  distributor_node_affinity_matchers:: nodeAffinityMatchers,
  ingester_node_affinity_matchers:: nodeAffinityMatchers,
  overrides_exporter_node_affinity_matchers:: nodeAffinityMatchers,
  querier_node_affinity_matchers:: nodeAffinityMatchers,
  query_frontend_node_affinity_matchers:: nodeAffinityMatchers,
  query_scheduler_node_affinity_matchers:: nodeAffinityMatchers,
  ruler_node_affinity_matchers:: nodeAffinityMatchers,
  store_gateway_node_affinity_matchers:: nodeAffinityMatchers,
}
