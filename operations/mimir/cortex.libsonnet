(import 'ksonnet-util/kausal.libsonnet') +
(import 'ksonnet-util/jaeger.libsonnet') +
(import 'images.libsonnet') +
(import 'common.libsonnet') +
(import 'config.libsonnet') +
(import 'consul.libsonnet') +

// Cortex services
(import 'distributor.libsonnet') +
(import 'ingester.libsonnet') +
(import 'querier.libsonnet') +
(import 'query-frontend.libsonnet') +
(import 'table-manager.libsonnet') +
(import 'ruler.libsonnet') +
(import 'alertmanager.libsonnet') +

// Supporting services
(import 'etcd.libsonnet') +
(import 'memcached.libsonnet') +
(import 'test-exporter.libsonnet')
