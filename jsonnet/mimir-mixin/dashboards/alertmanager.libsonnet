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
        $.panel('Per %s Tenants' % $._config.per_instance_label) +
        $.queryPanel(
          'max by(%s) (cortex_alertmanager_tenants_owned{%s})' % [$._config.per_instance_label, $.jobMatcher('alertmanager')],
          '{{%s}}' % $._config.per_instance_label
        ) +
        $.stack
      )
      .addPanel(
        $.panel('Per %s Alerts' % $._config.per_instance_label) +
        $.queryPanel(
          'sum by(%s) (cortex_alertmanager_alerts{%s})' % [$._config.per_instance_label, $.jobMatcher('alertmanager')],
          '{{%s}}' % $._config.per_instance_label
        ) +
        $.stack
      )
      .addPanel(
        $.panel('Per %s Silences' % $._config.per_instance_label) +
        $.queryPanel(
          'sum by(%s) (cortex_alertmanager_silences{%s})' % [$._config.per_instance_label, $.jobMatcher('alertmanager')],
          '{{%s}}' % $._config.per_instance_label
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
        $.panel('Tenant initial sync outcomes') +
        $.queryPanel(
          'sum by(outcome) (cortex_alertmanager_state_initial_sync_completed_total{%s})' % $.jobMatcher('alertmanager'),
          '{{outcome}}'
        ) +
        $.stack
      )
      .addPanel(
        $.panel('Initial sync duration') +
        $.latencyPanel('cortex_alertmanager_state_initial_sync_duration_seconds', '{%s}' % $.jobMatcher('alertmanager')) + {
          targets: [
            target {
              interval: '1m',
            }
            for target in super.targets
          ],
        }
      )
      .addPanel(
        $.panel('Fetch state from other alertmanagers /sec') +
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
        ) + {
          targets: [
            target {
              interval: '1m',
            }
            for target in super.targets
          ],
        }
      )
    )
    .addRow(
      $.row('Sharding Runtime State Sync')
      .addPanel(
        $.panel('Replicate state to other alertmanagers /sec') +
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
        $.panel('Merge state from other alertmanagers /sec') +
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
        $.panel('Persist state to remote storage /sec') +
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
