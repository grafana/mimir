local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-overview-networking.json';

(import 'dashboard-utils.libsonnet') {
  [filename]:
    ($.dashboard('Overview networking') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates(false)
    .addRowIf($._config.gateway_enabled, $.containerNetworkingRow('Gateway', 'gateway'))
    .addRow($.containerNetworkingRow('Writes', 'write'))
    .addRow($.containerNetworkingRow('Reads', 'read'))
    .addRow($.containerNetworkingRow('Backend', 'backend'))
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
