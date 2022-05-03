local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-config.json';

(import 'dashboard-utils.libsonnet') {
  [filename]:
    ($.dashboard('Config') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates()
    .addRow(
      $.row('Startup config file')
      .addPanel(
        $.panel('Startup config file hashes') +
        $.queryPanel('count(cortex_config_hash{%s}) by (sha256)' % $.namespaceMatcher(), 'sha256:{{sha256}}') +
        $.stack +
        { yaxes: $.yaxes('instances') },
      )
    )
    .addRow(
      $.row('Runtime config file')
      .addPanel(
        $.panel('Runtime config file hashes') +
        $.queryPanel('count(cortex_runtime_config_hash{%s}) by (sha256)' % $.namespaceMatcher(), 'sha256:{{sha256}}') +
        $.stack +
        { yaxes: $.yaxes('instances') },
      )
    ),
}
