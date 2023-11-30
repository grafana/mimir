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

  distributor_node_affinity_matchers:: [
    { key: 'topology.kubernetes.io/region', operator: 'In', values: ['us-east-1'] },
    { key: 'topology.kubernetes.io/zone', operator: 'In', values: ['us-east-1a', 'us-east-1b'] },
  ],
}
