# Contributing to the Grafana Mimir mixin dashboards

The dashboards of the mixin live in [operations/mimir-mixin/dashboards](https://github.com/grafana/mimir/tree/main/operations/mimir-mixin/dashboards).
Run `make format-mixin build-mixin` after changing them, and commit the compiled dashboards.

## Dashboard files

Newer dashboard files are imported at the top level of [dashboards.libsonnet](https://github.com/grafana/mimir/blob/main/operations/mimir-mixin/dashboards.libsonnet), and define their own `grafanaDashboards+:` field.
They build the dashboard helpers locally from the mixin configuration:

```jsonnet
{
  local lib =
    (import 'dashboard-utils.libsonnet') +
    (import 'dashboard-queries.libsonnet') +
    { _config:: $._config + $._group_config },

  grafanaDashboards+: {
    'mimir-example.json': lib.dashboard('Example') + { uid: std.md5('mimir-example.json') },
  },
}
```

Older dashboard files are merged into the `grafanaDashboards+:` field of `dashboards.libsonnet`, and use the grafana-builder rows layout.
Lay out new dashboards with grafonnet rows and the `gridRow()` and `gridLayout()` helpers of `dashboard-utils.libsonnet` instead.

## Sharing content between dashboards

When the same rows and panels are shown in more than one dashboard, define them once in the hidden top-level `grafanaPanels+::` field, named after the dashboard they come from, and build each dashboard from it.
The content is a list of `gridRow()` rows, so each dashboard only lays it out:

```jsonnet
{
  grafanaPanels+:: {
    // Rows of the Reads networking dashboard, also shown in the Networking tab of the Reads dashboard.
    readsNetworkingContent: [
      lib.gridRow('Querier', [lib.containerNetworkReceiveBytesPanelByComponent('querier')]),
    ],
  },

  grafanaDashboards+: {
    'mimir-reads-networking.json':
      lib.dashboard('Reads networking')
      .addGridPanels(lib.gridLayout($.grafanaPanels.readsNetworkingContent)),
  },
}
```

For example, the `Mimir / Reads networking` and `Mimir / Reads resources` dashboards and the tabs of the `Mimir / Reads` dashboard are built from the same `grafanaPanels` content, so they can't drift apart.
Downstream jsonnet can override a content field to change every dashboard that shows it.

## Tabbed dashboards

Tabs are only available in Grafana's v2 dashboard schema.
`tabbedDashboard()` in [dashboard-v2.libsonnet](https://github.com/grafana/mimir/blob/main/operations/mimir-mixin/dashboards/dashboard-v2.libsonnet) converts a dashboard built with `lib.dashboard()`, for its variables, links and settings, and `grafanaPanels` content for each tab to a v2 dashboard.
The variables are shared by all tabs, so the queries of every tab must work with them.

mixtool lint only supports v1 dashboards, so `make check-mixin-mixtool` lints [mixin-lint.libsonnet](https://github.com/grafana/mimir/blob/main/operations/mimir-mixin/mixin-lint.libsonnet), which replaces each v2 dashboard with a v1 dashboard with the same panels and variables.
