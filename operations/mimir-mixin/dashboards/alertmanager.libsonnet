local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-alertmanager.json';

(import 'dashboard-utils.libsonnet') {
  [filename]:
    local jobSelector = $.jobSelector($._config.job_names.alertmanager);
    local jobInstanceSelector = jobSelector + [utils.selector.noop($._config.per_instance_label)];
    local jobIntegrationSelector = jobSelector + [utils.selector.noop('integration')];
    ($.dashboard('Alertmanager') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates()
    .addRow(
      ($.row('Headlines') + {
         height: '100px',
         showTitle: false,
       })
      .addPanel(
        $.panel('Total alerts') +
        $.statPanel('sum(%s:cortex_alertmanager_alerts:sum%s)' % [$.recordingRulePrefix(jobInstanceSelector), utils.toPrometheusSelector(jobInstanceSelector)], format='short')
      )
      .addPanel(
        $.panel('Total silences') +
        $.statPanel('sum(%s:cortex_alertmanager_silences:sum%s)' % [$.recordingRulePrefix(jobInstanceSelector), utils.toPrometheusSelector(jobInstanceSelector)], format='short')
      )
      .addPanel(
        $.panel('Tenants') +
        $.statPanel('max(cortex_alertmanager_tenants_discovered{%s})' % $.jobMatcher($._config.job_names.alertmanager), format='short')
      )
    )
    .addRow(
      $.row('Alertmanager Distributor')
      .addPanel(
        $.panel('QPS') +
        $.qpsPanel('cortex_request_duration_seconds_count{%s, route=~"/alertmanagerpb.Alertmanager/HandleRequest"}' % $.jobMatcher($._config.job_names.alertmanager))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector($._config.job_names.alertmanager) + [utils.selector.re('route', '/alertmanagerpb.Alertmanager/HandleRequest')])
      )
    )
    .addRow(
      $.row('Alerts received')
      .addPanel(
        $.panel('APS') +
        $.successFailurePanel(
          |||
            sum(%(prefix)s:cortex_alertmanager_alerts_received_total:rate5m%(selectors)s)
            -
            sum(%(prefix)s:cortex_alertmanager_alerts_invalid_total:rate5m%(selectors)s)
          ||| % {
            prefix: $.recordingRulePrefix(jobSelector),
            selectors: utils.toPrometheusSelector(jobSelector),
          },
          'sum(%(prefix)s:cortex_alertmanager_alerts_invalid_total:rate5m%(selectors)s)' % {
            prefix: $.recordingRulePrefix(jobSelector),
            selectors: utils.toPrometheusSelector(jobSelector),
          },
        )
      )
    )
    .addRow(
      $.row('Alert notifications')
      .addPanel(
        $.panel('NPS') +
        $.successFailurePanel(
          $.queries.alertmanager.notifications.successPerSecond,
          $.queries.alertmanager.notifications.failurePerSecond,
        )
      )
      .addPanel(
        $.panel('NPS by integration') +
        $.queryPanel(
          [
            |||
              (
              sum(%(prefix)s:cortex_alertmanager_notifications_total:rate5m%(selectors)s) by(integration)
              -
              sum(%(prefix)s:cortex_alertmanager_notifications_failed_total:rate5m%(selectors)s) by(integration)
              ) > 0
              or on () vector(0)
            ||| % {
              prefix: $.recordingRulePrefix(jobIntegrationSelector),
              selectors: utils.toPrometheusSelector(jobIntegrationSelector),
            },
            'sum(%(prefix)s:cortex_alertmanager_notifications_failed_total:rate5m%(selectors)s) by(integration)' % {
              prefix: $.recordingRulePrefix(jobIntegrationSelector),
              selectors: utils.toPrometheusSelector(jobIntegrationSelector),
            },
          ],
          ['success - {{ integration }}', 'failed - {{ integration }}']
        )
      )
      .addPanel(
        $.panel('Latency') +
        $.latencyPanel('cortex_alertmanager_notification_latency_seconds', '{%s}' % $.jobMatcher($._config.job_names.alertmanager))
      )
    )
    .addRowIf(
      $._config.gateway_enabled,
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
        $.panel('Per %s tenants' % $._config.per_instance_label) +
        $.queryPanel(
          'max by(%s) (cortex_alertmanager_tenants_owned{%s})' % [$._config.per_instance_label, $.jobMatcher($._config.job_names.alertmanager)],
          '{{%s}}' % $._config.per_instance_label
        ) +
        $.stack
      )
      .addPanel(
        $.panel('Per %s alerts' % $._config.per_instance_label) +
        $.queryPanel(
          'sum by(%s) (%s:cortex_alertmanager_alerts:sum%s)' % [$._config.per_instance_label, $.recordingRulePrefix(jobInstanceSelector), utils.toPrometheusSelector(jobInstanceSelector)],
          '{{%s}}' % $._config.per_instance_label
        ) +
        $.stack
      )
      .addPanel(
        $.panel('Per %s silences' % $._config.per_instance_label) +
        $.queryPanel(
          'sum by(%s) (%s:cortex_alertmanager_silences:sum%s)' % [$._config.per_instance_label, $.recordingRulePrefix(jobInstanceSelector), utils.toPrometheusSelector(jobInstanceSelector)],
          '{{%s}}' % $._config.per_instance_label
        ) +
        $.stack
      )
    )
    .addRow(
      $.row('Tenant configuration sync')
      .addPanel(
        $.panel('Syncs/sec') +
        $.successFailurePanel(
          |||
            sum(rate(cortex_alertmanager_sync_configs_total{%s}[$__rate_interval]))
            -
            sum(rate(cortex_alertmanager_sync_configs_failed_total{%s}[$__rate_interval]))
          ||| % [$.jobMatcher($._config.job_names.alertmanager), $.jobMatcher($._config.job_names.alertmanager)],
          'sum(rate(cortex_alertmanager_sync_configs_failed_total{%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.alertmanager),
        )
      )
      .addPanel(
        $.panel('Syncs/sec (by reason)') +
        $.queryPanel(
          'sum by(reason) (rate(cortex_alertmanager_sync_configs_total{%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.alertmanager),
          '{{reason}}'
        )
      )
      .addPanel(
        $.panel('Ring check errors/sec') +
        $.queryPanel(
          'sum (rate(cortex_alertmanager_ring_check_errors_total{%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.alertmanager),
          'errors'
        )
      )
    )
    .addRow(
      $.row('Sharding initial state sync')
      .addPanel(
        $.panel('Initial syncs /sec') +
        $.queryPanel(
          'sum by(outcome) (rate(cortex_alertmanager_state_initial_sync_completed_total{%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.alertmanager),
          '{{outcome}}'
        ) + {
          targets: [
            target {
              interval: '1m',
            }
            for target in super.targets
          ],
        }
      )
      .addPanel(
        $.panel('Initial sync duration') +
        $.latencyPanel('cortex_alertmanager_state_initial_sync_duration_seconds', '{%s}' % $.jobMatcher($._config.job_names.alertmanager)) + {
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
        $.successFailurePanel(
          |||
            sum(rate(cortex_alertmanager_state_fetch_replica_state_total{%s}[$__rate_interval]))
            -
            sum(rate(cortex_alertmanager_state_fetch_replica_state_failed_total{%s}[$__rate_interval]))
          ||| % [$.jobMatcher($._config.job_names.alertmanager), $.jobMatcher($._config.job_names.alertmanager)],
          'sum(rate(cortex_alertmanager_state_fetch_replica_state_failed_total{%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.alertmanager),
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
      $.row('Sharding runtime state sync')
      .addPanel(
        $.panel('Replicate state to other alertmanagers /sec') +
        $.successFailurePanel(
          |||
            sum(%(prefix)s:cortex_alertmanager_state_replication_total:rate5m%(selectors)s)
            -
            sum(%(prefix)s:cortex_alertmanager_state_replication_failed_total:rate5m%(selectors)s)
          ||| % {
            prefix: $.recordingRulePrefix(jobSelector),
            selectors: utils.toPrometheusSelector(jobSelector),
          },
          'sum(%(prefix)s:cortex_alertmanager_state_replication_failed_total:rate5m%(selectors)s)' % {
            prefix: $.recordingRulePrefix(jobSelector),
            selectors: utils.toPrometheusSelector(jobSelector),
          },
        )
      )
      .addPanel(
        $.panel('Merge state from other alertmanagers /sec') +
        $.successFailurePanel(
          |||
            sum(%(prefix)s:cortex_alertmanager_partial_state_merges_total:rate5m%(selectors)s)
            -
            sum(%(prefix)s:cortex_alertmanager_partial_state_merges_failed_total:rate5m%(selectors)s)
          ||| % {
            prefix: $.recordingRulePrefix(jobSelector),
            selectors: utils.toPrometheusSelector(jobSelector),
          },
          'sum(%(prefix)s:cortex_alertmanager_partial_state_merges_failed_total:rate5m%(selectors)s)' % {
            prefix: $.recordingRulePrefix(jobSelector),
            selectors: utils.toPrometheusSelector(jobSelector),
          },
        )
      )
      .addPanel(
        $.panel('Persist state to remote storage /sec') +
        $.successFailurePanel(
          |||
            sum(rate(cortex_alertmanager_state_persist_total{%s}[$__rate_interval]))
            -
            sum(rate(cortex_alertmanager_state_persist_failed_total{%s}[$__rate_interval]))
          ||| % [$.jobMatcher($._config.job_names.alertmanager), $.jobMatcher($._config.job_names.alertmanager)],
          'sum(rate(cortex_alertmanager_state_persist_failed_total{%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.alertmanager),
        )
      )
    ),
}
