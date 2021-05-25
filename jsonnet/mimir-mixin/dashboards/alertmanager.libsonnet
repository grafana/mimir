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
      .addPanel(
        $.panel('Tenants') +
        $.statPanel('max(cortex_alertmanager_tenants_discovered{%s})' % $.jobMatcher('alertmanager'), format='short')
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
    )
    .addRow(
      $.row('Replication')
      .addPanel(
        $.panel('Tenants (By Instance)') +
        $.queryPanel(
          'sum by(pod) (cortex_alertmanager_tenants_owned{%s})' % $.jobMatcher('alertmanager'),
          '{{pod}}'
        ) +
        $.stack
      )
      .addPanel(
        $.panel('Alerts (By Instance)') +
        $.queryPanel(
          'sum by(pod) (cortex_alertmanager_alerts{%s})' % $.jobMatcher('alertmanager'),
          '{{pod}}'
        ) +
        $.stack
      )
      .addPanel(
        $.panel('Silences (By Instance)') +
        $.queryPanel(
          'sum by(pod) (cortex_alertmanager_silences{%s})' % $.jobMatcher('alertmanager'),
          '{{pod}}'
        ) +
        $.stack
      )
    )
    .addRow(
      $.row('Tenant Configuration Sync')
      .addPanel(
        $.panel('Syncs/sec') +
        $.queryPanel(
          [
            |||
              sum(rate(cortex_alertmanager_sync_configs_total{%s}[$__rate_interval]))
              -
              sum(rate(cortex_alertmanager_sync_configs_failed_total{%s}[$__rate_interval]))
            ||| % [$.jobMatcher('alertmanager'), $.jobMatcher('alertmanager')],
            'sum(rate(cortex_alertmanager_sync_configs_failed_total{%s}[$__rate_interval]))' % $.jobMatcher('alertmanager'),
          ],
          ['success', 'failed']
        )
      )
      .addPanel(
        $.panel('Syncs/sec (By Reason)') +
        $.queryPanel(
          'sum by(reason) (rate(cortex_alertmanager_sync_configs_total{%s}[$__rate_interval]))' % $.jobMatcher('alertmanager'),
          '{{reason}}'
        )
      )
      .addPanel(
        $.panel('Ring Check Errors/sec') +
        $.queryPanel(
          'sum (rate(cortex_alertmanager_ring_check_errors_total{%s}[$__rate_interval]))' % $.jobMatcher('alertmanager'),
          'errors'
        )
      )
    )
    .addRow(
      $.row('Sharding Initial State Sync')
      .addPanel(
        $.panel('Syncs/sec') +
        $.queryPanel(
          [
            |||
              sum(rate(cortex_alertmanager_state_initial_sync_total{%s}[$__rate_interval]))
              -
              sum(rate(cortex_alertmanager_state_initial_sync_completed_total{outcome="failed",%s}[$__rate_interval]))
            ||| % [$.jobMatcher('alertmanager'), $.jobMatcher('alertmanager')],
            'sum(rate(cortex_alertmanager_state_initial_sync_completed_total{outcome="failed",%s}[$__rate_interval]))' % $.jobMatcher('alertmanager'),
          ],
          ['success', 'failed']
        )
      )
      .addPanel(
        $.panel('Syncs/sec (By Outcome)') +
        $.queryPanel(
          'sum by(outcome) (rate(cortex_alertmanager_state_initial_sync_completed_total{%s}[$__rate_interval]))' % $.jobMatcher('alertmanager'),
          '{{outcome}}'
        )
      )
      .addPanel(
        $.panel('Duration') +
        utils.latencyRecordingRulePanel('cortex_alertmanager_state_initial_sync_duration_seconds', $.jobSelector('alertmanager'))
      )
    )
    .addRow(
      $.row('Sharding State Operations')
      .addPanel(
        $.panel('Replica Fetches/sec') +
        $.queryPanel(
          [
            |||
              sum(rate(cortex_alertmanager_state_fetch_replica_state_total{%s}[$__rate_interval]))
              -
              sum(rate(cortex_alertmanager_state_fetch_replica_state_failed_total{%s}[$__rate_interval]))
            ||| % [$.jobMatcher('alertmanager'), $.jobMatcher('alertmanager')],
            'sum(rate(cortex_alertmanager_state_fetch_replica_state_failed_total{%s}[$__rate_interval]))' % $.jobMatcher('alertmanager'),
          ],
          ['success', 'failed']
        )
      )
      .addPanel(
        $.panel('Replica Updates/sec') +
        $.queryPanel(
          [
            |||
              sum(rate(cortex_alertmanager_state_replication_total{%s}[$__rate_interval]))
              -
              sum(rate(cortex_alertmanager_state_replication_failed_total{%s}[$__rate_interval]))
            ||| % [$.jobMatcher('alertmanager'), $.jobMatcher('alertmanager')],
            'sum(rate(cortex_alertmanager_state_replication_failed_total{%s}[$__rate_interval]))' % $.jobMatcher('alertmanager'),
          ],
          ['success', 'failed']
        )
      )
      .addPanel(
        $.panel('Partial Merges/sec') +
        $.queryPanel(
          [
            |||
              sum(rate(cortex_alertmanager_partial_state_merges_total{%s}[$__rate_interval]))
              -
              sum(rate(cortex_alertmanager_partial_state_merges_failed_total{%s}[$__rate_interval]))
            ||| % [$.jobMatcher('alertmanager'), $.jobMatcher('alertmanager')],
            'sum(rate(cortex_alertmanager_partial_state_merges_failed_total{%s}[$__rate_interval]))' % $.jobMatcher('alertmanager'),
          ],
          ['success', 'failed']
        )
      )
      .addPanel(
        $.panel('Remote Storage Persists/sec') +
        $.queryPanel(
          [
            |||
              sum(rate(cortex_alertmanager_state_persist_total{%s}[$__rate_interval]))
              -
              sum(rate(cortex_alertmanager_state_persist_failed_total{%s}[$__rate_interval]))
            ||| % [$.jobMatcher('alertmanager'), $.jobMatcher('alertmanager')],
            'sum(rate(cortex_alertmanager_state_persist_failed_total{%s}[$__rate_interval]))' % $.jobMatcher('alertmanager'),
          ],
          ['success', 'failed']
        )
      )
    ),
}
