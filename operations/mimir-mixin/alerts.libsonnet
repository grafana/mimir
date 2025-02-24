{
  prometheusAlerts+::
    { _config:: $._config + $._group_config } +
    (import 'alerts/alerts.libsonnet') +
    (import 'alerts/alertmanager.libsonnet') +
    (import 'alerts/blocks.libsonnet') +
    (import 'alerts/compactor.libsonnet') +
    (import 'alerts/distributor.libsonnet') +
    (import 'alerts/autoscaling.libsonnet') +
    (if $._config.ingest_storage_enabled then import 'alerts/ingest-storage.libsonnet' else {}) +
    (import 'alerts/continuous-test.libsonnet'),
}
