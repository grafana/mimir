local utils = import 'mixin-utils/utils.libsonnet';

(import 'dashboard-utils.libsonnet') {
  'mimir-reads-networking.json':
    ($.dashboard('Reads networking') + { uid: 'c0464f0d8bd026f776c9006b05910000' })
    .addClusterSelectorTemplates(false)
    .addRow($.jobNetworkingRow('Gateway', 'gateway'))
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
