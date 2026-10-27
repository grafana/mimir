local filename = 'mimir-reads-networking.json';

{
  local lib =
    (import 'dashboard-utils.libsonnet') +
    (import 'dashboard-queries.libsonnet') +
    { _config:: $._config + $._group_config },

  // Rows of the Reads networking dashboard, also shown in the Networking tab of the Reads dashboard.
  grafanaPanels+:: {
    readsNetworkingContent:
      [lib.fromLegacyRow(lib.containerNetworkingRowByComponent('Summary', 'read'))]
      + (if lib._config.gateway_enabled then [lib.fromLegacyRow(lib.containerNetworkingRowByComponent('Gateway', 'gateway'))] else [])
      + [
        lib.fromLegacyRow(lib.containerNetworkingRowByComponent('Query-frontend', 'query_frontend')),
        lib.fromLegacyRow(lib.containerNetworkingRowByComponent('Query-scheduler', 'query_scheduler')),
        lib.fromLegacyRow(lib.containerNetworkingRowByComponent('Querier', 'querier')),
        lib.fromLegacyRow(lib.containerNetworkingRowByComponent('Store-gateway', 'store_gateway')),
        lib.fromLegacyRow(lib.containerNetworkingRowByComponent('Ruler', 'ruler')),
      ],
  },

  grafanaDashboards+: if !lib._config.resources_dashboards_enabled then {} else {
    [filename]:
      assert std.md5(filename) == '54b2a0a4748b3bd1aefa92ce5559a1c2' : 'UID of the dashboard has changed, please update references to dashboard.';
      (lib.dashboard('Reads networking') + { uid: std.md5(filename) })
      .addClusterSelectorTemplates(false)
      .addGridPanels(lib.gridLayout($.grafanaPanels.readsNetworkingContent))
      + {
        templating+: {
          list: [
            // Do not allow to include all namespaces.
            l + (if (l.name == 'namespace') then { includeAll: false } else {})
            for l in super.list
          ],
        },
      },
  },
}
