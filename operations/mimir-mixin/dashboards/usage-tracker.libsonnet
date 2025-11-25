local filename = 'mimir-usage-tracker.json';

(import 'dashboard-utils.libsonnet') +
(import 'dashboard-queries.libsonnet') +
{
  [filename]:
    assert std.md5(filename) == '59bf558b63012caa3c5871e6c512eb10' : 'UID of the dashboard has changed, please update references to dashboard.';
    ($.dashboard('Usage-tracker') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates()
    .addTemplate('namespace', 'label_values(cortex_request_duration_seconds{%s}, %s)' % [$.namespaceMatcher(), $._config.per_namespace_label], 'namespace', sort=1)
    .addTemplate('user', 'label_values(cortex_request_duration_seconds{%s, %s=~"$namespace"}, user)' % [$.namespaceMatcher(), $._config.per_namespace_label], 'user', sort=7)
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
            'histogram_quantile(1.0, sum(rate(cortex_usage_tracker_client_track_series_duration_seconds_bucket{%s, %s=~"$namespace", job!~".*load-generator.*"}[$__rate_interval])) by (le))' % [$.namespaceMatcher(), $._config.per_namespace_label],
            'histogram_quantile(0.9999, sum(rate(cortex_usage_tracker_client_track_series_duration_seconds_bucket{%s, %s=~"$namespace", job!~".*load-generator.*"}[$__rate_interval])) by (le))' % [$.namespaceMatcher(), $._config.per_namespace_label],
            'histogram_quantile(0.99, sum(rate(cortex_usage_tracker_client_track_series_duration_seconds_bucket{%s, %s=~"$namespace", job!~".*load-generator.*"}[$__rate_interval])) by (le))' % [$.namespaceMatcher(), $._config.per_namespace_label],
            'histogram_quantile(0.95, sum(rate(cortex_usage_tracker_client_track_series_duration_seconds_bucket{%s, %s=~"$namespace", job!~".*load-generator.*"}[$__rate_interval])) by (le))' % [$.namespaceMatcher(), $._config.per_namespace_label],
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
        $.timeseriesPanel('TrackSeries request rates by status code') +
        $.panelDescription(
          'TrackSeries request rates by status code',
          'Request rate for TrackSeries requests broken down by status code.',
        ) +
        $.qpsPanelNativeHistogram(
          $.queries.usage_tracker.writeRequestsPerSecondSelector + ', %s=~"$namespace"' % $._config.per_namespace_label,
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
        $.timeseriesPanel('TrackSeries request rates by server pod') +
        $.panelDescription(
          'TrackSeries request rates by server pod',
          'Request rate for TrackSeries requests broken down by server pod.',
        ) +
        $.queryPanel(
          'sum by (pod) (rate(cortex_request_duration_seconds_count{%s, %s=~"$namespace", route=~"%s"}[$__rate_interval]))' % [$.jobMatcher($._config.job_names.usage_tracker), $._config.per_namespace_label, $.queries.usage_tracker_track_series_routes_regex],
          '{{pod}}',
        ) +
        { fieldConfig+: { defaults+: { unit: 'reqps', custom+: { fillOpacity: 0 } } } },
      )
      .addPanel(
        $.timeseriesPanel('TrackSeries request latency (server-side)') +
        $.panelDescription(
          'TrackSeries request latency (server-side)',
          'Server-side latency for TrackSeries requests.',
        ) +
        $.ncLatencyPanel(
          $.queries.requests_per_second_metric,
          $.queries.usage_tracker.writeRequestsPerSecondSelector + ', %s=~"$namespace"' % $._config.per_namespace_label,
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
          '%s, %s=~"$namespace", route=~"%s"' % [$.jobMatcher($._config.job_names.usage_tracker), $._config.per_namespace_label, $.queries.usage_tracker_get_users_close_to_limit_routes_regex],
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
        $.timeseriesPanel('GetUsersCloseToLimit request rates by server pod') +
        $.panelDescription(
          'GetUsersCloseToLimit request rates by server pod',
          'Request rate for GetUsersCloseToLimit requests broken down by server pod.',
        ) +
        $.queryPanel(
          'sum by (pod) (rate(cortex_request_duration_seconds_count{%s, %s=~"$namespace", route=~"%s"}[$__rate_interval]))' % [$.jobMatcher($._config.job_names.usage_tracker), $._config.per_namespace_label, $.queries.usage_tracker_get_users_close_to_limit_routes_regex],
          '{{pod}}',
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
          '%s, %s=~"$namespace", route=~"%s"' % [$.jobMatcher($._config.job_names.usage_tracker), $._config.per_namespace_label, $.queries.usage_tracker_get_users_close_to_limit_routes_regex],
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
          'sum by (pod) (rate(cortex_usage_tracker_kafka_writer_produce_records_enqueued_total{%s, %s=~"$namespace", component="usage-tracker-events-writer"}[$__rate_interval]))' % [$.namespaceMatcher(), $._config.per_namespace_label],
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
          'sum by (pod) (rate(cortex_usage_tracker_kafka_writer_produce_records_enqueued_total{%s, %s=~"$namespace", component="usage-tracker-snapshots-metadata-writer"}[$__rate_interval]))' % [$.namespaceMatcher(), $._config.per_namespace_label],
          '{{pod}}',
        ) +
        { fieldConfig+: { defaults+: { unit: 'evtps', custom+: { fillOpacity: 0 } } } },
      )
    )
    // Section 3: Object Store
    .addRows($.getObjectStoreRows('Object Store', 'usage-tracker'))
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
