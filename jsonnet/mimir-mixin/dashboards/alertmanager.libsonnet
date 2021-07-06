local utils = import 'mixin-utils/utils.libsonnet';

(import 'dashboard-utils.libsonnet') {
  'alertmanager.json':
    ($.dashboard('Cortex / Alertmanager') + { uid: 'a76bee5913c97c918d9e56a3cc88cc28' })
    .addClusterSelectorTemplates()
    .addRow(
      ($.row('Headlines') + {
         height: '100px',
         showTitle: false,
       })
      .addPanel(
        $.panel('Total Alerts') +
        $.statPanel('sum(cortex_alertmanager_alerts{%s})' % $.jobMatcher('alertmanager'), format='short')
      )
      .addPanel(
        $.panel('Total Silences') +
        $.statPanel('sum(cortex_alertmanager_silences{%s})' % $.jobMatcher('alertmanager'), format='short')
      )
    )
    .addRow(
      $.row('Alerts Received')
      .addPanel(
        $.panel('APS') +
        $.queryPanel(
          [
            |||
              sum(rate(cortex_alertmanager_alerts_received_total{%s}[$__rate_interval]))
              -
              sum(rate(cortex_alertmanager_alerts_invalid_total{%s}[$__rate_interval]))
            ||| % [$.jobMatcher('alertmanager'), $.jobMatcher('alertmanager')],
            'sum(rate(cortex_alertmanager_alerts_invalid_total{%s}[$__rate_interval]))' % $.jobMatcher('alertmanager'),
          ],
          ['success', 'failed']
        )
      )
    )
    .addRow(
      $.row('Alert Notifications')
      .addPanel(
        $.panel('NPS') +
        $.queryPanel(
          [
            |||
              sum(rate(cortex_alertmanager_notifications_total{%s}[$__rate_interval]))
              -
              sum(rate(cortex_alertmanager_notifications_failed_total{%s}[$__rate_interval]))
            ||| % [$.jobMatcher('alertmanager'), $.jobMatcher('alertmanager')],
            'sum(rate(cortex_alertmanager_notifications_failed_total{%s}[$__rate_interval]))' % $.jobMatcher('alertmanager'),
          ],
          ['success', 'failed']
        )
      )
      .addPanel(
        $.panel('NPS by integration') +
        $.queryPanel(
          [
            |||
              (
              sum(rate(cortex_alertmanager_notifications_total{%s}[$__rate_interval])) by(integration)
              -
              sum(rate(cortex_alertmanager_notifications_failed_total{%s}[$__rate_interval])) by(integration)
              ) > 0
              or on () vector(0)
            ||| % [$.jobMatcher('alertmanager'), $.jobMatcher('alertmanager')],
            'sum(rate(cortex_alertmanager_notifications_failed_total{%s}[$__rate_interval])) by(integration)' % $.jobMatcher('alertmanager'),
          ],
          ['success - {{ integration }}', 'failed - {{ integration }}']
        )
      )
      .addPanel(
        $.panel('Latency') +
        $.latencyPanel('cortex_alertmanager_notification_latency_seconds', '{%s}' % $.jobMatcher('alertmanager'))
      )
    )
    .addRow(
      $.row('Configuration API (gateway) + Alertmanager UI')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_request_duration_seconds_count{%s, route=~"api_v1_alerts|alertmanager"}' % $.jobMatcher($._config.job_names.gateway))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector($._config.job_names.gateway) + [utils.selector.re('route', 'api_v1_alerts|alertmanager')])
      )
    )
    .addRows(
      $.getObjectStoreRows('Alertmanager Configuration Object Store (Alertmanager accesses)', 'alertmanager-storage')
    ),
}
