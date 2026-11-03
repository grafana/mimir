local mimir = import 'mimir/mimir.libsonnet';

mimir {
  _config+:: {
    namespace: 'default',
    external_url: 'http://test',

    storage_backend: 'gcs',
    blocks_storage_bucket_name: 'blocks-bucket',

    ruler_enabled: true,
    ruler_storage_bucket_name: 'rules-bucket',
    ruler_remote_evaluation_enabled: true,

    alertmanager_enabled: true,
    alertmanager_storage_bucket_name: 'alerts-bucket',

    // Ingest storage is required by block-builder.
    ingest_storage_enabled: true,

    block_builder+: {
      enabled: true,
    },

    overrides_exporter_enabled: true,
  },

  mimirComponentAnnotations+:: {
    'profile-memory': true,
  },

  alertmanager_annotations+:: {
    'component-id': 1,
  },

  block_builder_annotations+:: {
    'component-id': 2,
  },

  block_builder_scheduler_annotations+:: {
    'component-id': 3,
  },

  compactor_annotations+:: {
    'component-id': 4,
  },

  compactor_scheduler_annotations+:: {
    'component-id': 5,
  },

  continuous_test_annotations+:: {
    'component-id': 6,
  },

  distributor_annotations+:: {
    'component-id': 7,
  },

  ingester_annotations+:: {
    'component-id': 8,
  },

  overrides_exporter_annotations+:: {
    'component-id': 9,
  },

  querier_annotations+:: {
    'component-id': 10,
  },

  query_frontend_annotations+:: {
    'component-id': 11,
  },

  query_scheduler_annotations+:: {
    'component-id': 12,
  },

  ruler_annotations+:: {
    'component-id': 13,
  },

  ruler_querier_annotations+:: {
    'component-id': 14,
  },

  ruler_query_frontend_annotations+:: {
    'component-id': 15,
  },

  ruler_query_scheduler_annotations+:: {
    'component-id': 16,
  },

  store_gateway_annotations+:: {
    'component-id': 17,
  },
}
