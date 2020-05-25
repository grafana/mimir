{
  grafanaDashboards+:
    (import 'dashboards/queries.libsonnet') +
    (import 'dashboards/reads.libsonnet') +
    (import 'dashboards/ruler.libsonnet') +
    (import 'dashboards/scaling.libsonnet') +
    (import 'dashboards/writes.libsonnet') +

    (if std.setMember('tsdb', $._config.storage_engine)
     then
       (import 'dashboards/compactor.libsonnet') +
       (import 'dashboards/compactor-resources.libsonnet') +
       (import 'dashboards/object-store.libsonnet')
     else {}) +

    (if std.setMember('chunks', $._config.storage_engine)
     then import 'dashboards/chunks.libsonnet'
     else {}) +

    (if std.setMember('tsdb', $._config.storage_engine)
        && std.setMember('chunks', $._config.storage_engine)
     then import 'dashboards/comparison.libsonnet'
     else {}) +

    (if !$._config.resources_dashboards_enabled then {} else
       (import 'dashboards/reads-resources.libsonnet') +
       (import 'dashboards/writes-resources.libsonnet')) +

    { _config:: $._config },
}
