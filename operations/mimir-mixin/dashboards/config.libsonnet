local filename = 'mimir-config.json';
local uid = std.md5(filename);
assert uid == '5d9d0b4724c0f80d68467088ec61e003' : 'UID of the dashboard has changed, please update references to dashboard.';

{
  local g = (import 'grafonnet.libsonnet')($._config),
  local dashboard = g.dashboard,
  local panel = g.panel,
  local promQuery = g.query.prometheus,
  local timeSeries = panel.timeSeries,

  [filename]:
    dashboard.new('Config')
    + dashboard.withUid(std.md5(filename))
    + g.mimir.addClusterSelectorTemplates()
    + dashboard.withPanels([
      panel.row.new('Startup config file'),
      timeSeries.new('Startup config file hashes')
      + timeSeries.queryOptions.withDatasource('prometheus', '$datasource')
      + g.mimir.stack
      + timeSeries.queryOptions.withTargets(
        [
          promQuery.new('$datasource', 'count(cortex_config_hash{%s}) by (sha256)' % g.mimir.namespaceMatcher())
          + promQuery.withLegendFormat('sha256:{{sha256}}'),
        ]
      ),

      panel.row.new('Runtime config file'),
      timeSeries.new('Runtime config file hashes')
      + timeSeries.queryOptions.withDatasource('prometheus', '$datasource')
      + g.mimir.stack
      + timeSeries.queryOptions.withTargets([
        promQuery.new('$datasource', 'count(cortex_runtime_config_hash{%s}) by (sha256)' % g.mimir.namespaceMatcher())
        + promQuery.withLegendFormat('sha256:{{sha256}}')
        + timeSeries.standardOptions.withUnit('instances'),
      ]),
    ]),
}
