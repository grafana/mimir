local mimir = import 'mimir/mimir.libsonnet';
local overridesExporter = import 'mimir/overrides-exporter.libsonnet';

mimir + overridesExporter {
  _config+:: {
    namespace: 'default',
    external_url: 'mimir.default.svc.cluster.local',
    blocks_storage_backend: 'gcs',
    blocks_storage_bucket_name: 'example-blocks-bucket',
    alertmanager_client_type: 'gcs',
    alertmanager_gcs_bucket_name: 'example-alertmanager-bucket',
    ruler_client_type: 'gcs',
    ruler_storage_bucket_name: 'example-ruler-bucket',
    alertmanager_enabled: true,
    ruler_enabled: true,
    memberlist_ring_enabled: true,
    unregister_ingesters_on_shutdown: false,
    query_scheduler_enabled: true,
    query_sharding_enabled: true,
  },

  # These are properties that are set differently on different components in jsonnet.
  # We unset them all here so the default values are used like in Helm.
  # TODO: Once the read-write deployment is stable, we can revisit these settings.
  # At that point there will likely be less deviation between components.
  # See the tracking issue: https://github.com/grafana/mimir/issues/2749
  querier_args+:: {
    'store.max-query-length': null,
    'server.http-write-timeout': null,
  },

  query_frontend_args+:: {
    'server.grpc-max-recv-msg-size-bytes': null,
    'store.max-query-length': null,
    'server.http-write-timeout': null,
    'query-frontend.query-sharding-total-shards': null,
  },

  ruler_args+:: {
    'store.max-query-length': null,
  },
}
