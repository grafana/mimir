local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-reads-networking.json';

(import 'dashboard-utils.libsonnet') {
  [filename]:
    assert std.md5(filename) == '54b2a0a4748b3bd1aefa92ce5559a1c2' : 'UID of the dashboard has changed, please update references to dashboard.';
    ($.dashboard('Reads networking') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates(false)
    .addRow($.containerNetworkingRowByComponent('Summary', 'read'))
    .addRowIf($._config.gateway_enabled, $.containerNetworkingRowByComponent('Gateway', 'gateway'))
    .addRow($.containerNetworkingRowByComponent('Query-frontend', 'query_frontend'))
    .addRow($.containerNetworkingRowByComponent('Query-scheduler', 'query_scheduler'))
    .addRow($.containerNetworkingRowByComponent('Querier', 'querier'))
    .addRow($.containerNetworkingRowByComponent('Store-gateway', 'store_gateway'))
    .addRow($.containerNetworkingRowByComponent('Ruler', 'ruler'))
    + {
      templating+: {
        list: [
          // Do not allow to include all namespaces.
          l + (if (l.name == 'namespace') then { includeAll: false } else {})
          for l in super.list
        ],
      },
    },
}
