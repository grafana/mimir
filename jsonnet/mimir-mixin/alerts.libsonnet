{
  prometheusAlerts+::
    (import 'alerts/alerts.libsonnet') +

    (if std.setMember('tsdb', $._config.storage_engine)
     then
       (import 'alerts/blocks.libsonnet') +
       (import 'alerts/compactor.libsonnet')
     else {}) +

    { _config:: $._config },
}
