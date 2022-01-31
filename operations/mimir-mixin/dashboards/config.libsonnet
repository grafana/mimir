local utils = import 'mixin-utils/utils.libsonnet';

(import 'dashboard-utils.libsonnet') {
  'mimir-config.json':
    ($.dashboard('Config') + { uid: '61bb048ced9817b2d3e07677fb1c6290' })
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
