local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-reads-networking.json';

(import 'dashboard-utils.libsonnet') {
  [filename]:
    ($.dashboard('Reads networking') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates(false)
    .addRow($.containerNetworkingRow('Summary', 'read'))
    .addRowIf($._config.gateway_enabled, $.containerNetworkingRow('Gateway', 'gateway'))
    .addRow($.containerNetworkingRow('Query-frontend', 'query_frontend'))
    .addRow($.containerNetworkingRow('Query-scheduler', 'query_scheduler'))
    .addRow($.containerNetworkingRow('Querier', 'querier'))
    .addRow($.containerNetworkingRow('Store-gateway', 'store_gateway'))
    .addRow($.containerNetworkingRow('Ruler', 'ruler'))
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
