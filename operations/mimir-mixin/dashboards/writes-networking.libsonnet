local utils = import 'mixin-utils/utils.libsonnet';

(import 'dashboard-utils.libsonnet') {
  'mimir-writes-networking.json':
    ($.dashboard('Writes networking') + { uid: '681cd62b680b7154811fe73af55dcfd4' })
    .addClusterSelectorTemplates(false)
    .addRow($.jobNetworkingRow('Gateway', 'gateway'))
    .addRow($.jobNetworkingRow('Distributor', 'distributor'))
    .addRow($.jobNetworkingRow('Ingester', 'ingester'))
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
