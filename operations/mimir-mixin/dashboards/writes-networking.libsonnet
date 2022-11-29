local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-writes-networking.json';

(import 'dashboard-utils.libsonnet') {
  [filename]:
    ($.dashboard('Writes networking') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates(false)
    .addRow($.containerNetworkingRow('Summary', 'write'))
    .addRowIf($._config.gateway_enabled, $.containerNetworkingRow('Gateway', 'gateway'))
    .addRow($.containerNetworkingRow('Distributor', 'distributor'))
    .addRow($.containerNetworkingRow('Ingester', 'ingester'))
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
