local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-active-series-billing-estimate.json';

(import 'dashboard-utils.libsonnet') +
(import 'dashboard-queries.libsonnet') {
  local ingesterMatcher = $.jobMatcher($._config.job_names.ingester),
  local distributorMatcher = $.jobMatcher($._config.job_names.distributor),

  // NH-adjusted active series: classic series + NH series weighted by bucket count / 4.
  // NH series are subtracted from the raw count and re-added as (buckets / 4) to reflect
  // their lower storage cost relative to classic series. The `or * 0` fallback ensures
  // a zero contribution when the NH metrics are absent (i.e. no NH series exist).
  local nhAdjustedSeriesQuery = |||
    cortex_ingester_active_series{%(ingester)s}
    - (
        cortex_ingester_active_native_histogram_series{%(ingester)s}
        or cortex_ingester_active_series{%(ingester)s} * 0
      )
    + (
        cortex_ingester_active_native_histogram_buckets{%(ingester)s}
        or cortex_ingester_active_series{%(ingester)s} * 0
      ) / 4
  ||| % { ingester: ingesterMatcher },

  [filename]:
    assert std.md5(filename) == '1c4ea050bd8bace2a40c21285c956649' : 'UID of the dashboard has changed, please update references to dashboard.';
    ($.dashboard('Active series billing estimate') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates()
    .addRowIf(
      $._config.show_dashboard_descriptions.active_series_billing_estimate,
      ($.row('Active series billing estimate dashboard description') { height: '25px', showTitle: false })
      .addPanel(
        $.textPanel('', |||
          <p>
            This dashboard estimates billable active series, adjusted for native histograms (NH).
            Classic series count at full weight; NH series are replaced by <code>buckets / 4</code>
            to reflect their lower relative storage cost.
          </p>
          <p>
            The <strong>combined</strong> row handles clusters running either classic or ingest storage automatically.
            Classic storage divides by the replication factor; ingest storage deduplicates via ingester partition ID.
            The <strong>classic</strong> and <strong>ingest</strong> rows are simpler variants for homogeneous clusters.
          </p>
          <p>
            The <strong>billable estimate</strong> row shows the trailing 30-day p95 of the instant value —
            an expensive query; collapse it when not needed.
          </p>
        |||),
      )
    )

    .addRow(
      $.row('Active series (combined: classic + ingest storage)')
      .addPanel(
        $.timeseriesPanel('NH-adjusted active series') +
        $.queryPanel(
          $.queries.ingester.ingestOrClassicDeduplicatedQuery(nhAdjustedSeriesQuery),
          '{{ %s }}' % $._config.per_cluster_label,
        ) +
        { fieldConfig+: { defaults+: { unit: 'short' } } }
      )
    )

    .addRow(
      ($.row('Billable estimate (trailing 30-day p95)') + { collapse: true })
      .addPanel(
        $.timeseriesPanel('Trailing 30-day p95 NH-adjusted active series') +
        $.queryPanel(
          |||
            quantile_over_time(
              0.95,
              (
                %s
              )[30d:]
            )
          ||| % $.queries.ingester.ingestOrClassicDeduplicatedQuery(nhAdjustedSeriesQuery),
          '{{ %s }}' % $._config.per_cluster_label,
        ) +
        { fieldConfig+: { defaults+: { unit: 'short' } } }
      )
    )

    .addRow(
      ($.row('Classic storage only') + { collapse: true })
      .addPanel(
        $.timeseriesPanel('NH-adjusted active series (classic storage)') +
        $.queryPanel(
          |||
            sum by (%(groupByCluster)s) (
                cortex_ingester_active_series{%(ingester)s}
              - (
                  cortex_ingester_active_native_histogram_series{%(ingester)s}
                  or cortex_ingester_active_series{%(ingester)s} * 0
                )
              + (
                  cortex_ingester_active_native_histogram_buckets{%(ingester)s}
                  or cortex_ingester_active_series{%(ingester)s} * 0
                ) / 4
            )
            / on (%(groupByCluster)s) group_left ()
            max by (%(groupByCluster)s) (cortex_distributor_replication_factor{%(distributor)s})
          ||| % {
            ingester: ingesterMatcher,
            distributor: distributorMatcher,
            groupByCluster: $._config.group_by_cluster,
          },
          '{{ %s }}' % $._config.per_cluster_label,
        ) +
        { fieldConfig+: { defaults+: { unit: 'short' } } }
      )
    )

    .addRow(
      ($.row('Ingest storage only') + { collapse: true })
      .addPanel(
        $.timeseriesPanel('NH-adjusted active series (ingest storage)') +
        $.queryPanel(
          |||
            sum by (%(groupByCluster)s) (
              max by (ingester_id, %(groupByCluster)s) (
                label_replace(
                    cortex_ingester_active_series{%(ingester)s}
                  - (
                      cortex_ingester_active_native_histogram_series{%(ingester)s}
                      or cortex_ingester_active_series{%(ingester)s} * 0
                    )
                  + (
                      cortex_ingester_active_native_histogram_buckets{%(ingester)s}
                      or cortex_ingester_active_series{%(ingester)s} * 0
                    ) / 4,
                  "ingester_id", "$1", "%(instance)s", ".*-([0-9]+)$"
                )
              )
            )
          ||| % {
            ingester: ingesterMatcher,
            groupByCluster: $._config.group_by_cluster,
            instance: $._config.per_instance_label,
          },
          '{{ %s }}' % $._config.per_cluster_label,
        ) +
        { fieldConfig+: { defaults+: { unit: 'short' } } }
      )
    ),
}
