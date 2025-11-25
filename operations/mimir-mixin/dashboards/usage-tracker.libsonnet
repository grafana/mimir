local filename = 'mimir-usage-tracker.json';

(import 'dashboard-utils.libsonnet') +
(import 'dashboard-queries.libsonnet') +
{
  [filename]:
    assert std.md5(filename) == '59bf558b63012caa3c5871e6c512eb10' : 'UID of the dashboard has changed, please update references to dashboard.';
    ($.dashboard('Usage-tracker') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates()
    .addActiveUserSelectorTemplates()
    // Section 1: Requests
    .addRow(
      $.row('Tracking requests')
      .addPanel(
        $.timeseriesPanel('TrackSeries latency (client-side, aggregated, excl load-generator)') +
        $.panelDescription(
          'TrackSeries latency (client-side, aggregated, excl load-generator)',
          'Client-side latency for TrackSeries requests, aggregated and excluding load-generator.',
        ) +
        $.queryPanel(
          [
            'histogram_quantile(1.0, sum(rate(cortex_usage_tracker_client_track_series_duration_seconds_bucket{%s, job!~".*load-generator.*"}[$__rate_interval])) by (le))' % [$.jobMatcher($._config.job_names.usage_tracker)],
            'histogram_quantile(0.9999, sum(rate(cortex_usage_tracker_client_track_series_duration_seconds_bucket{%s, job!~".*load-generator.*"}[$__rate_interval])) by (le))' % [$.jobMatcher($._config.job_names.usage_tracker)],
            'histogram_quantile(0.99, sum(rate(cortex_usage_tracker_client_track_series_duration_seconds_bucket{%s, job!~".*load-generator.*"}[$__rate_interval])) by (le))' % [$.jobMatcher($._config.job_names.usage_tracker)],
            'histogram_quantile(0.95, sum(rate(cortex_usage_tracker_client_track_series_duration_seconds_bucket{%s, job!~".*load-generator.*"}[$__rate_interval])) by (le))' % [$.jobMatcher($._config.job_names.usage_tracker)],
          ],
          [
            'p100',
            'p99.99',
            'p99',
            'p95',
          ],
        ) +
        { fieldConfig+: { defaults+: { unit: 's', custom+: { fillOpacity: 0 } } } },
      )
      .addPanel(
        $.timeseriesPanel('TrackSeries request rates by server %s' % $._config.per_instance_label) +
        $.panelDescription(
          'TrackSeries request rates by server %s' % $._config.per_instance_label,
          'Request rate for TrackSeries requests broken down by server %s.' % $._config.per_instance_label,
        ) +
        $.queryPanel(
          'histogram_count(sum by (%s) (rate(%s{%s, route=~"%s"}[$__rate_interval])))' % [$._config.per_instance_label, $.queries.requests_per_second_metric, $.jobMatcher($._config.job_names.usage_tracker), $.queries.usage_tracker.trackSeriesRequestsPerSecondRouteRegex],
          '{{%s}}' % $._config.per_instance_label,
        ) +
        { fieldConfig+: { defaults+: { unit: 'reqps', custom+: { fillOpacity: 0 } } } },
      )
      .addPanel(
        $.timeseriesPanel('TrackSeries request latency (server-side)') +
        $.panelDescription(
          'TrackSeries request latency (server-side)',
          'Server-side latency for TrackSeries requests.',
        ) +
        $.latencyPanel(
          $.queries.requests_per_second_metric,
          $.queries.usage_tracker.trackSeriesRequestsPerSecondSelector,
          multiplier='1e6',
        ) +
        {
          fieldConfig+: {
            defaults+: {
              unit: 'µs',
              custom+: {
                fillOpacity: 0,
              },
            },
          },
        }
      )
    )
    .addRow(
      $.row('GetUsersCloseToLimit requests')
      .addPanel(
        $.timeseriesPanel('GetUsersCloseToLimit request rates by status code') +
        $.panelDescription(
          'GetUsersCloseToLimit request rates by status code',
          'Request rate for GetUsersCloseToLimit requests broken down by status code.',
        ) +
        $.qpsPanelNativeHistogram(
          $.queries.usage_tracker.getUsersCloseToLimitRequestsPerSecondSelector
        ) +
        {
          fieldConfig+: {
            defaults+: {
              custom+: {
                fillOpacity: 0,
              },
            },
          },
        }
      )
      .addPanel(
        $.timeseriesPanel('GetUsersCloseToLimit request rates by server %s' % $._config.per_instance_label) +
        $.panelDescription(
          'GetUsersCloseToLimit request rates by server %s' % $._config.per_instance_label,
          'Request rate for GetUsersCloseToLimit requests broken down by server %s.' % $._config.per_instance_label,
        ) +
        $.queryPanel(
          'histogram_count(sum by (%s) (rate(%s{%s, route=~"%s"}[$__rate_interval])))' % [$._config.per_instance_label, $.queries.requests_per_second_metric, $.jobMatcher($._config.job_names.usage_tracker), $.queries.usage_tracker.getUsersCloseToLimitRequestsPerSecondRouteRegex],
          '{{%s}}' % $._config.per_instance_label,
        ) +
        { fieldConfig+: { defaults+: { unit: 'reqps', custom+: { fillOpacity: 0 } } } },
      )
      .addPanel(
        $.timeseriesPanel('GetUsersCloseToLimit request latency (server-side)') +
        $.panelDescription(
          'GetUsersCloseToLimit request latency (server-side)',
          'Server-side latency for GetUsersCloseToLimit requests.',
        ) +
        $.ncLatencyPanel(
          $.queries.requests_per_second_metric,
          $.queries.usage_tracker.getUsersCloseToLimitRequestsPerSecondSelector,
          multiplier='1e6',
        ) +
        {
          fieldConfig+: {
            defaults+: {
              unit: 'µs',
              custom+: {
                fillOpacity: 0,
              },
            },
          },
        }
      )
    )
    // Section 2: Event publishing
    .addRow(
      $.row('Event publishing')
      .addPanel(
        $.timeseriesPanel('Event records published rate') +
        $.panelDescription(
          'Event records published rate',
          'Rate of event records published to Kafka.',
        ) +
        $.queryPanel(
          'sum by (pod) (rate(cortex_usage_tracker_kafka_writer_produce_records_total{%s, component="usage-tracker-events-writer"}[$__rate_interval]))' % [$.jobMatcher($._config.job_names.usage_tracker)],
          '{{pod}}',
        ) +
        { fieldConfig+: { defaults+: { unit: 'evtps', custom+: { fillOpacity: 0 } } } },
      )
      .addPanel(
        $.timeseriesPanel('Snapshot metadata records published rate') +
        $.panelDescription(
          'Snapshot metadata records published rate',
          'Rate of snapshot metadata records published to Kafka.',
        ) +
        $.queryPanel(
          'sum by (pod) (rate(cortex_usage_tracker_kafka_writer_produce_records_enqueued_total{%s, component="usage-tracker-snapshots-metadata-writer"}[$__rate_interval]))' % [$.jobMatcher($._config.job_names.usage_tracker)],
          '{{pod}}',
        ) +
        { fieldConfig+: { defaults+: { unit: 'evtps', custom+: { fillOpacity: 0 } } } },
      )
    )
    // Section 3: Object Store
    .addRows($.getObjectStoreRows('Object Store', 'usage-tracker-snapshots'))
    // Section 4: Resources
    .addRow(
      $.row('Usage-tracker resources')
      .addPanel(
        $.containerCPUUsagePanelByComponent('usage_tracker'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanelByComponent('usage_tracker'),
      )
    ),
}
