{
  grafanaDashboards+:
    (import 'dashboards/queries.libsonnet') +
    (import 'dashboards/reads.libsonnet') +
    (import 'dashboards/reads-resources.libsonnet') +
    (import 'dashboards/ruler.libsonnet') +
    (import 'dashboards/scaling.libsonnet') +
    (import 'dashboards/writes.libsonnet') +
    (import 'dashboards/writes-resources.libsonnet') +

    (if std.setMember('tsdb', $._config.storage_engine)
     then import 'dashboards/compactor.libsonnet'
     else {}) +

    (if std.setMember('chunks', $._config.storage_engine)
     then import 'dashboards/chunks.libsonnet'
     else {}) +

    (if std.setMember('tsdb', $._config.storage_engine)
        && std.setMember('chunks', $._config.storage_engine)
     then import 'dashboards/comparison.libsonnet'
     else {}) +

    { _config:: $._config },
}
