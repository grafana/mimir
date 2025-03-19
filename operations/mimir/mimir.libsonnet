(import 'ksonnet-util/kausal.libsonnet') +
(import 'jaeger-agent-mixin/jaeger.libsonnet') +
(import 'images.libsonnet') +
(import 'common.libsonnet') +
(import 'config.libsonnet') +
(import 'consul.libsonnet') +

// Mimir services
(import 'distributor.libsonnet') +
(import 'ingester.libsonnet') +
(import 'querier.libsonnet') +
(import 'query-frontend.libsonnet') +
(import 'ruler.libsonnet') +
(import 'alertmanager.libsonnet') +
(import 'query-scheduler.libsonnet') +
(import 'compactor.libsonnet') +
(import 'store-gateway.libsonnet') +
(import 'overrides-exporter.libsonnet') +

// Supporting services
(import 'etcd.libsonnet') +
(import 'memcached.libsonnet') +

// Mimir features
(import 'shuffle-sharding.libsonnet') +
(import 'query-sharding.libsonnet') +
(import 'rollout-operator.libsonnet') +
(import 'ruler-remote-evaluation.libsonnet') +
(import 'continuous-test.libsonnet') +

// Multi-zone support.
(import 'multi-zone.libsonnet') +
(import 'multi-zone-distributor.libsonnet') +
(import 'multi-zone-etcd.libsonnet') +

// Import autoscaling after other features because it overrides deployments.
(import 'autoscaling.libsonnet') +

// Read-write deployment mode.
(import 'read-write-deployment/main.libsonnet') +

// mTLS client configuration for Memcached
(import 'memcached-client-mtls.libsonnet') +

// Automated downscale of ingesters and store-gateways
(import 'ingester-automated-downscale.libsonnet') +
(import 'ingester-automated-downscale-v2.libsonnet') +
(import 'store-gateway-automated-downscale.libsonnet') +

// Automatic cleanup of unused PVCs after scaling down
(import 'pvc-auto-deletion.libsonnet') +

// Support for ReplicaTemplate objects.
(import 'replica-template.libsonnet') +

// Experimental ingest storage. Keep this at the end, because we need to override components on top of other changes.
(import 'ingest-storage.libsonnet') +
(import 'ingest-storage-ingester-autoscaling.libsonnet') +
(import 'ingest-storage-migration.libsonnet') +

// Add memberlist support. Keep it at the end because it overrides all Mimir components.
(import 'memberlist.libsonnet')
