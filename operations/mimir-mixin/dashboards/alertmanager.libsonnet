local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-alertmanager.json';

(import 'dashboard-utils.libsonnet') {
  [filename]:
    local jobSelector = $.jobSelector($._config.job_names.alertmanager);
    local jobInstanceSelector = jobSelector + [utils.selector.noop($._config.per_instance_label)];
    local jobIntegrationSelector = jobSelector + [utils.selector.noop('integration')];
    assert std.md5(filename) == 'b0d38d318bbddd80476246d4930f9e55' : 'UID of the dashboard has changed, please update references to dashboard.';
    ($.dashboard('Alertmanager') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates()
    .addShowNativeLatencyVariable()
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
      local alertmanagerGRPCRoutesRegex = utils.selector.re('route', '%s' % $.queries.alertmanager_grpc_routes_regex);
      $.row('Alertmanager Distributor')
      .addPanel(
        $.timeseriesPanel('QPS') +
        $.qpsPanelNativeHistogram($.queries.alertmanager.requestsPerSecondMetric, utils.toPrometheusSelectorNaked($.jobSelector($._config.job_names.alertmanager) + [alertmanagerGRPCRoutesRegex]))
      )
      .addPanel(
        $.timeseriesPanel('Latency') +
        $.latencyRecordingRulePanelNativeHistogram($.queries.alertmanager.requestsPerSecondMetric, $.jobSelector($._config.job_names.alertmanager) + [alertmanagerGRPCRoutesRegex])
      )
    )
    .addRow(
      $.row('Alerts received')
      .addPanel(
        $.timeseriesPanel('APS') +
        $.successFailurePanel(
          |||
            sum(%(prefix)s:cortex_alertmanager_alerts_received_total:rate5m%(selectors)s)
            -
            (sum(%(prefix)s:cortex_alertmanager_alerts_invalid_total:rate5m%(selectors)s) or vector(0))
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
      $.row('Alerts grouping')
      .addPanel(
        $.timeseriesPanel('per %s Active Aggregation Groups' % $._config.per_instance_label) +
        $.queryPanel(
          'cortex_alertmanager_dispatcher_aggregation_groups{%s}' % $.jobMatcher($._config.job_names.alertmanager),
          '{{%s}}' % $._config.per_instance_label
        ) +
        $.stack
      )
    )
    .addRow(
      $.row('Alert notifications')
      .addPanel(
        $.timeseriesPanel('NPS') +
        $.successFailurePanel(
          $.queries.alertmanager.notifications.successPerSecond,
          $.queries.alertmanager.notifications.failurePerSecond,
        )
      )
      .addPanel(
        $.timeseriesPanel('NPS by integration') +
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
        $.timeseriesPanel('Latency') +
        $.latencyPanel('cortex_alertmanager_notification_latency_seconds', '{%s}' % $.jobMatcher($._config.job_names.alertmanager))
      )
    )
    .addRowIf(
      $._config.gateway_enabled,
      $.row('Configuration API (gateway) + Alertmanager UI')
      .addPanel(
        $.timeseriesPanel('QPS') +
        $.qpsPanelNativeHistogram($.queries.alertmanager.requestsPerSecondMetric, utils.toPrometheusSelectorNaked($.jobSelector($._config.job_names.gateway) + [utils.selector.re('route', '%s' % $.queries.alertmanager_http_routes_regex)]))
      )
      .addPanel(
        $.timeseriesPanel('Latency') +
        $.latencyRecordingRulePanelNativeHistogram($.queries.gateway.requestsPerSecondMetric, $.jobSelector($._config.job_names.gateway) + [utils.selector.re('route', '%s' % $.queries.alertmanager_http_routes_regex)])
      )
    )
    .addRows(
      $.getObjectStoreRows('Alertmanager Configuration Object Store (Alertmanager accesses)', 'alertmanager-storage')
    )
    .addRow(
      $.row('Replication')
      .addPanel(
        $.timeseriesPanel('Per %s tenants' % $._config.per_instance_label) +
        $.queryPanel(
          'max by(%s) (cortex_alertmanager_tenants_owned{%s})' % [$._config.per_instance_label, $.jobMatcher($._config.job_names.alertmanager)],
          '{{%s}}' % $._config.per_instance_label
        ) +
        $.stack
      )
      .addPanel(
        $.timeseriesPanel('Per %s alerts' % $._config.per_instance_label) +
        $.queryPanel(
          'sum by(%s) (%s:cortex_alertmanager_alerts:sum%s)' % [$._config.per_instance_label, $.recordingRulePrefix(jobInstanceSelector), utils.toPrometheusSelector(jobInstanceSelector)],
          '{{%s}}' % $._config.per_instance_label
        ) +
        $.stack
      )
      .addPanel(
        $.timeseriesPanel('Per %s silences' % $._config.per_instance_label) +
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
        $.timeseriesPanel('Syncs/sec') +
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
        $.timeseriesPanel('Syncs/sec (by reason)') +
        $.queryPanel(
          'sum by(reason) (rate(cortex_alertmanager_sync_configs_total{%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.alertmanager),
          '{{reason}}'
        )
      )
      .addPanel(
        $.timeseriesPanel('Ring check errors/sec') +
        $.queryPanel(
          'sum (rate(cortex_alertmanager_ring_check_errors_total{%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.alertmanager),
          'errors'
        )
      )
    )
    .addRow(
      $.row('Sharding initial state sync')
      .addPanel(
        $.timeseriesPanel('Initial syncs /sec') +
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
        $.timeseriesPanel('Initial sync duration') +
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
        $.timeseriesPanel('Fetch state from other alertmanagers /sec') +
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
        $.timeseriesPanel('Replicate state to other alertmanagers /sec') +
        $.successFailurePanel(
          |||
            sum(%(prefix)s:cortex_alertmanager_state_replication_total:rate5m%(selectors)s)
            -
            (sum(%(prefix)s:cortex_alertmanager_state_replication_failed_total:rate5m%(selectors)s) or vector(0))
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
        $.timeseriesPanel('Merge state from other alertmanagers /sec') +
        $.successFailurePanel(
          |||
            sum(%(prefix)s:cortex_alertmanager_partial_state_merges_total:rate5m%(selectors)s)
            -
            (sum(%(prefix)s:cortex_alertmanager_partial_state_merges_failed_total:rate5m%(selectors)s) or vector(0))
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
        $.timeseriesPanel('Persist state to remote storage /sec') +
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
