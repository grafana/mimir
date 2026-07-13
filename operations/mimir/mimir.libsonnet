(import 'ksonnet-util/kausal.libsonnet') +
(import 'keda-libsonnet/main.libsonnet') +
(import 'images.libsonnet') +
(import 'common.libsonnet') +
(import 'tracing.libsonnet') +
(import 'config.libsonnet') +
(import 'consul.libsonnet') +
(import 'rollout-operator/rollout-operator.libsonnet') +
(import 'rollout-operator.libsonnet') +

// Tools to operate on blocks in object storage (run as one-off Jobs).
(import 'block-tools.libsonnet') +

// Mimir services
(import 'distributor.libsonnet') +
(import 'ingester.libsonnet') +
(import 'querier.libsonnet') +
(import 'query-frontend.libsonnet') +
(import 'ruler.libsonnet') +
(import 'alertmanager.libsonnet') +
(import 'query-scheduler.libsonnet') +
(import 'compactor.libsonnet') +
(import 'compactor-scheduler.libsonnet') +
(import 'store-gateway.libsonnet') +
(import 'overrides-exporter.libsonnet') +

// Supporting services
(import 'memcached.libsonnet') +

// Mimir features
(import 'shuffle-sharding.libsonnet') +
(import 'query-sharding.libsonnet') +
(import 'ruler-remote-evaluation.libsonnet') +
(import 'continuous-test.libsonnet') +

// Import autoscaling after other features because it overrides deployments,
// but before multi-zone deployments because they build on this.
(import 'autoscaling.libsonnet') +

// Multi-zone support.
(import 'multi-zone-common.libsonnet') +
(import 'multi-zone-distributor.libsonnet') +
(import 'multi-zone-ingester.libsonnet') +
(import 'multi-zone-store-gateway.libsonnet') +
(import 'multi-zone-memcached.libsonnet') +
(import 'multi-zone-querier.libsonnet') +
(import 'multi-zone-query-frontend.libsonnet') +
(import 'multi-zone-query-scheduler.libsonnet') +
(import 'multi-zone-ruler.libsonnet') +
(import 'multi-zone-ruler-remote-evaluation.libsonnet') +

// Automated downscale of ingesters and store-gateways
(import 'ingester-automated-downscale.libsonnet') +
(import 'ingester-automated-downscale-v2.libsonnet') +

// Deletion protection for specific Mimir components.
(import 'deletion-protection.libsonnet') +

// Experimental ingest storage. Keep this at the end, because we need to override components on top of other changes.
(import 'ingest-storage.libsonnet') +
(import 'ingest-storage-ingester-autoscaling.libsonnet') +
(import 'ingest-storage-migration.libsonnet') +

// Experimental compartments support. Must come after autoscaling, multi-zone and ingest-storage
// because it overrides their components and builds on their helpers.
(import 'compartments-common.libsonnet') +
(import 'compartments-distributor.libsonnet') +
(import 'compartments-ingester.libsonnet') +
(import 'compartments-querier.libsonnet') +
(import 'compartments-query-frontend.libsonnet') +
(import 'compartments-compactor.libsonnet') +
(import 'compartments-compactor-scheduler.libsonnet') +
(import 'compartments-store-gateway.libsonnet') +
(import 'compartments-memcached.libsonnet') +

// Store-gateway autoscaling. Keep it after multi-zone-store-gateway (it patches those StatefulSets) and after
// compartments-store-gateway so it can layer the same autoscaling onto the per-compartment StatefulSet maps.
(import 'store-gateway-autoscaling.libsonnet') +

// Automatic cleanup of unused PVCs after scaling down. Keep it after compartments so it can patch the
// per-compartment StatefulSet maps.
(import 'pvc-auto-deletion.libsonnet') +

// Add memberlist support. Keep it at the end because it overrides all Mimir components.
(import 'memberlist.libsonnet') +
(import 'multi-zone-memberlist-bridge.libsonnet')
