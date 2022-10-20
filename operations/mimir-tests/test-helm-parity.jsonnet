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

  # These are properties that are set differently on different components by default.
  # In order to match up with Helm, which sets all properties the same for every component,
  # we must explicitly set them here.

  # TODO: revisit these settings once the simplified deployment model is stable.
  # At that point we should have fewer possible options for these properties 
  # and might be able to find better defaults for Helm
  helm_common_args:: {
    'server.http-write-timeout': '1m',
    'server.grpc-max-recv-msg-size-bytes': '104857600',
    'store.max-query-length': '0',
  },

  alertmanager_args+:: self.helm_common_args,
  compactor_args+:: self.helm_common_args,
  distributor_args+:: self.helm_common_args,
  ingester_args+:: self.helm_common_args,
  overrides_exporter_args+:: self.helm_common_args,
  querier_args+:: self.helm_common_args,
  query_frontend_args+:: self.helm_common_args,
  query_scheduler_args+:: self.helm_common_args,
  ruler_args+:: self.helm_common_args,
  store_gateway_args+:: self.helm_common_args,
}
