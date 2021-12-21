{
  prometheusAlerts+::
    (import 'alerts/alerts.libsonnet') +
    (import 'alerts/alertmanager.libsonnet') +
    (import 'alerts/blocks.libsonnet') +
    (import 'alerts/compactor.libsonnet') +
    { _config:: $._config + $._group_config },
}
