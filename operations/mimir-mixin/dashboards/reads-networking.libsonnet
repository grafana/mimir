local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-reads-networking.json';

(import 'dashboard-utils.libsonnet') {
  [filename]:
    ($.dashboard('Reads networking') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates(false)
    .addRowIf($._config.gateway_enabled, $.jobNetworkingRow('Gateway', 'gateway'))
    .addRow($.jobNetworkingRow('Query-frontend', 'query_frontend'))
    .addRow($.jobNetworkingRow('Query-scheduler', 'query_scheduler'))
    .addRow($.jobNetworkingRow('Querier', 'querier'))
    .addRow($.jobNetworkingRow('Store-gateway', 'store_gateway'))
    .addRow($.jobNetworkingRow('Ruler', 'ruler'))
    + {
      templating+: {
        list: [
          // Do not allow to include all clusters/namespaces.
          l + (if (l.name == 'cluster' || l.name == 'namespace') then { includeAll: false } else {})
          for l in super.list
        ],
      },
    },
}
