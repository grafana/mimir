{
  prometheusAlerts+::
    (import 'alerts/alerts.libsonnet') +

    { _config:: $._config },
}
