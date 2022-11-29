local mimir = import 'mimir/mimir.libsonnet';

mimir {
  _config+:: {
    namespace: 'default',
    external_url: 'mimir.default.svc.cluster.local',
    storage_backend: 'gcs',

    blocks_storage_bucket_name: 'example-blocks-bucket',
    alertmanager_storage_bucket_name: 'example-alertmanager-bucket',
    ruler_storage_bucket_name: 'example-ruler-bucket',
    alertmanager_enabled: true,
    ruler_enabled: true,
    unregister_ingesters_on_shutdown: false,
    query_sharding_enabled: true,
    overrides_exporter_enabled: true,

    alertmanager+: {
      fallback_config: {
        route: { receiver: 'default-receiver' },
        receivers: [{ name: 'default-receiver' }],
      },
    },
  },

  // These are properties that are set differently on different components in jsonnet.
  // We unset them all here so the default values are used like in Helm.
  // TODO: Once the read-write deployment is stable, we can revisit these settings.
  // At that point there will likely be less deviation between components.
  // See the tracking issue: https://github.com/grafana/mimir/issues/2749
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
