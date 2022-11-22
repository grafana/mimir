{
  grafanaDashboards+:
    (import 'dashboards/config.libsonnet') +
    (import 'dashboards/queries.libsonnet') +
    (import 'dashboards/reads.libsonnet') +
    (import 'dashboards/ruler.libsonnet') +
    (import 'dashboards/remote-ruler-reads.libsonnet') +
    (import 'dashboards/alertmanager.libsonnet') +
    (import 'dashboards/scaling.libsonnet') +
    (import 'dashboards/writes.libsonnet') +
    (import 'dashboards/slow-queries.libsonnet') +
    (import 'dashboards/rollout-progress.libsonnet') +
    (import 'dashboards/compactor.libsonnet') +
    (import 'dashboards/compactor-resources.libsonnet') +
    (import 'dashboards/object-store.libsonnet') +
    (import 'dashboards/overrides.libsonnet') +
    (import 'dashboards/tenants.libsonnet') +
    (import 'dashboards/top-tenants.libsonnet') +
    (import 'dashboards/overview.libsonnet') +

    (if !$._config.resources_dashboards_enabled then {} else
       (import 'dashboards/overview-resources.libsonnet') +
       (import 'dashboards/overview-networking.libsonnet') +
       (import 'dashboards/reads-resources.libsonnet') +
       (import 'dashboards/remote-ruler-reads-resources.libsonnet') +
       (import 'dashboards/reads-networking.libsonnet') +
       (import 'dashboards/writes-resources.libsonnet') +
       (import 'dashboards/writes-networking.libsonnet') +
       (import 'dashboards/alertmanager-resources.libsonnet')) +

    { _config:: $._config + $._group_config },
}
