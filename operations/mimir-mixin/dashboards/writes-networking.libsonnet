local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-writes-networking.json';

(import 'dashboard-utils.libsonnet') {
  [filename]:
    assert std.md5(filename) == '978c1cb452585c96697a238eaac7fe2d' : 'UID of the dashboard has changed, please update references to dashboard.';
    ($.dashboard('Writes networking') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates(false)
    .addRow($.containerNetworkingRowByComponent('Summary', 'write'))
    .addRowIf($._config.gateway_enabled, $.containerNetworkingRowByComponent('Gateway', 'gateway'))
    .addRow($.containerNetworkingRowByComponent('Distributor', 'distributor'))
    .addRow($.containerNetworkingRowByComponent('Ingester', 'ingester'))
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
