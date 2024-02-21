local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-config.json';

(import 'dashboard-utils.libsonnet') {
  [filename]:
    ($.dashboard('Config') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates()
    .addRow(
      $.row('Startup config file')
      .addPanel(
        $.timeseriesPanel('Startup config file hashes') +
        $.queryPanel('count(cortex_config_hash{%s}) by (sha256)' % $.namespaceMatcher(), 'sha256:{{sha256}}') +
        $.stack +
        { fieldConfig+: { defaults+: { unit: 'instances' } } },
      )
    )
    .addRow(
      $.row('Runtime config file')
      .addPanel(
        $.timeseriesPanel('Runtime config file hashes') +
        $.queryPanel('count(cortex_runtime_config_hash{%s}) by (sha256)' % $.namespaceMatcher(), 'sha256:{{sha256}}') +
        $.stack +
        { fieldConfig+: { defaults+: { unit: 'instances' } } },
      )
    ),
}
