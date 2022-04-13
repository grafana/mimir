{
  prometheusAlerts+::
    { _config:: $._config + $._group_config } +
    (import 'alerts/alerts.libsonnet') +
    (import 'alerts/alertmanager.libsonnet') +
    (import 'alerts/blocks.libsonnet') +
    (import 'alerts/compactor.libsonnet') +
    (import 'alerts/autoscaling.libsonnet') +
    (import 'alerts/continuous-test.libsonnet'),
}
