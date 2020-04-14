(import 'dashboards.libsonnet') +
(import 'alerts.libsonnet') +
(import 'recording_rules.libsonnet') {
  grafanaDashboardFolder: 'Cortex',
  grafanaDashboardShards: 4,
}
