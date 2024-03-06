local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-overview-networking.json';

(import 'dashboard-utils.libsonnet') {
  [filename]:
    assert std.md5(filename) == 'e15c71d372cc541367a088f10d9fcd92' : 'UID of the dashboard has changed, please update references to dashboard.';
    ($.dashboard('Overview networking') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates(false)
    .addRowIf($._config.gateway_enabled, $.containerNetworkingRowByComponent('Gateway', 'gateway'))
    .addRow($.containerNetworkingRowByComponent('Writes', 'write'))
    .addRow($.containerNetworkingRowByComponent('Reads', 'read'))
    .addRow($.containerNetworkingRowByComponent('Backend', 'backend'))
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
