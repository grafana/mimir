{
  grafanaDashboards+:
    (import 'dashboards/config.libsonnet') +
    (import 'dashboards/queries.libsonnet') +
    (import 'dashboards/reads.libsonnet') +
    (import 'dashboards/ruler.libsonnet') +
    (import 'dashboards/alertmanager.libsonnet') +
    (import 'dashboards/scaling.libsonnet') +
    (import 'dashboards/writes.libsonnet') +
    (import 'dashboards/slow-queries.libsonnet') +
    (import 'dashboards/rollout-progress.libsonnet') +

    (if std.member($._config.storage_engine, 'blocks')
     then
       (import 'dashboards/compactor.libsonnet') +
       (import 'dashboards/compactor-resources.libsonnet') +
       (import 'dashboards/object-store.libsonnet')
     else {}) +

    (if std.member($._config.storage_engine, 'chunks')
     then import 'dashboards/chunks.libsonnet'
     else {}) +

    (if std.member($._config.storage_engine, 'blocks')
        && std.member($._config.storage_engine, 'chunks')
     then import 'dashboards/comparison.libsonnet'
     else {}) +

    (if !$._config.resources_dashboards_enabled then {} else
       (import 'dashboards/reads-resources.libsonnet') +
       (import 'dashboards/reads-networking.libsonnet') +
       (import 'dashboards/writes-resources.libsonnet') +
       (import 'dashboards/writes-networking.libsonnet') +
       (import 'dashboards/alertmanager-resources.libsonnet')) +

    { _config:: $._config + $._group_config },
}
