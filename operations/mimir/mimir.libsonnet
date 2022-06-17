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

// Supporting services
(import 'etcd.libsonnet') +
(import 'memcached.libsonnet') +

// Mimir features
(import 'shuffle-sharding.libsonnet') +
(import 'query-sharding.libsonnet') +
(import 'multi-zone.libsonnet') +
(import 'memberlist.libsonnet') +
(import 'continuous-test.libsonnet') +
(import 'ruler-remote-evaluation.libsonnet') +

// Import autoscaling at the end because it overrides deployments.
(import 'autoscaling.libsonnet')
