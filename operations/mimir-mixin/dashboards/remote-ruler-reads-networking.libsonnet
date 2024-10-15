local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-remote-ruler-reads-networking.json';

(import 'dashboard-utils.libsonnet') {
  [filename]:
    assert std.md5(filename) == '9e8cfff65f91632f8a25981c6fe44bc9' : 'UID of the dashboard has changed, please update references to dashboard.';
    ($.dashboard('Remote ruler reads networking') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates(false)
    .addRow($.containerNetworkingRowByComponent('Summary', 'remote_ruler_read'))
    .addRow($.containerNetworkingRowByComponent('Ruler-query-frontend', 'ruler_query_frontend'))
    .addRow($.containerNetworkingRowByComponent('Ruler-query-scheduler', 'ruler_query_scheduler'))
    .addRow($.containerNetworkingRowByComponent('Ruler-querier', 'ruler_querier'))
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
