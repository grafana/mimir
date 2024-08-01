local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-top-tenants.json';

(import 'dashboard-utils.libsonnet') +
(import 'dashboard-queries.libsonnet') {
  local in_memory_series_per_user_query(at='') = (
    local perIngesterQuery = |||
      (
          cortex_ingester_memory_series_created_total{%(ingester)s} %(at)s
          -
          cortex_ingester_memory_series_removed_total{%(ingester)s} %(at)s
      )
    ||| % {
      at: at,
      ingester: $.jobMatcher($._config.job_names.ingester),
    };
    $.queries.ingester.ingestOrClassicDeduplicatedQuery(perIngesterQuery, groupByLabels='user')
  ),

  [filename]:
    assert std.md5(filename) == 'bc6e12d4fe540e4a1785b9d3ca0ffdd9' : 'UID of the dashboard has changed, please update references to dashboard.';
    ($.dashboard('Top tenants') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates()
    .addCustomTemplate(
      'limit', 'limit', [
        { label: '10', value: '10' },
        { label: '50', value: '50' },
        { label: '100', value: '100' },
      ]
    )
    .addRowIf(
      $._config.show_dashboard_descriptions.top_tenants,
      ($.row('Top tenants dashboard description') { height: '25px', showTitle: false })
      .addPanel(
        $.textPanel('', |||
          <p>
            This dashboard shows the top tenants based on multiple selection criterias.
            Rows are collapsed by default to avoid querying all of them.
            Use the templating variable "limit" above to select the amount of users to be shown.
          </p>
        |||),
      )
    )

    .addRow(
      ($.row('By active series') + { collapse: true })
      .addPanel(
        $.panel('Top $limit users by active series') +
        { sort: { col: 2, desc: true } } +
        $.tablePanel(
          [
            |||
              topk($limit, %s)
            ||| % [
              $.queries.ingester.ingestOrClassicDeduplicatedQuery('cortex_ingester_active_series{%s}' % [$.jobMatcher($._config.job_names.ingester)], groupByLabels='user'),
            ],
          ], {
            user: { alias: 'user', unit: 'string' },
            Value: { alias: 'series' },
          }
        )
      ),
    )

    .addRow(
      ($.row('By in-memory series') + { collapse: true })
      .addPanel(
        $.panel('Top $limit users by in-memory series (series created - series removed)') +
        { sort: { col: 2, desc: true } } +
        $.tablePanel(
          [
            'topk($limit, %(in_memory_series_per_user)s)' % { in_memory_series_per_user: in_memory_series_per_user_query() },
          ], {
            user: { alias: 'user', unit: 'string' },
            Value: { alias: 'series' },
          }
        )
      ),
    )

    .addRow(
      ($.row('By in-memory series growth') + { collapse: true })
      .addPanel(
        local title = 'Top $limit users by in-memory series (series created - series removed) that grew the most between query range start and query range end';
        $.timeseriesPanel(title) +
        $.queryPanel(
          |||
            (%(in_memory_series_per_user)s)
            and
            topk($limit,
              (
                %(in_memory_series_per_user_at_end)s
              )
              -
              (
                %(in_memory_series_per_user_at_start)s
              )
            )
          ||| % {
            in_memory_series_per_user: in_memory_series_per_user_query(),
            in_memory_series_per_user_at_end: in_memory_series_per_user_query(at='@ end()'),
            in_memory_series_per_user_at_start: in_memory_series_per_user_query(at='@ start()'),
          },
          '{{ user }}',
        )
      ),
    )

    .addRow(
      ($.row('By samples rate') + { collapse: true })
      .addPanel(
        $.panel('Top $limit users by received samples rate in last 5m') +
        { sort: { col: 2, desc: true } } +
        $.tablePanel(
          [
            'topk($limit, sum by (user) (rate(cortex_distributor_received_samples_total{%(job)s}[5m])))'
            % { job: $.jobMatcher($._config.job_names.distributor) },
          ], {
            user: { alias: 'user', unit: 'string' },
            Value: { alias: 'samples/s' },
          }
        )
      ),
    )

    .addRow(
      ($.row('By samples rate growth') + { collapse: true })
      .addPanel(
        $.timeseriesPanel('Top $limit users by received samples rate that grew the most between query range start and query range end') +
        $.queryPanel(
          |||
            sum by (user) (rate(cortex_distributor_received_samples_total{%(job)s}[$__rate_interval]))
            and
            topk($limit,
              sum by (user) (rate(cortex_distributor_received_samples_total{%(job)s}[$__rate_interval] @ end()))
              -
              sum by (user) (rate(cortex_distributor_received_samples_total{%(job)s}[$__rate_interval] @ start()))
            )
          ||| % {
            job: $.jobMatcher($._config.job_names.distributor),
          },
          '{{ user }}',
        )
      ),
    )

    .addRow(
      ($.row('By discarded samples rate') + { collapse: true })
      .addPanel(
        $.panel('Top $limit users by discarded samples rate in last 5m') +
        { sort: { col: 2, desc: true } } +
        $.tablePanel(
          [
            'topk($limit, sum by (user) (rate(cortex_discarded_samples_total{%(job)s}[5m])))'
            % { job: $.jobMatcher($._config.job_names.ingester + $._config.job_names.distributor) },
          ], {
            user: { alias: 'user', unit: 'string' },
            Value: { alias: 'samples/s' },
          }
        )
      ),
    )

    .addRow(
      ($.row('By discarded samples rate growth') + { collapse: true })
      .addPanel(
        $.timeseriesPanel('Top $limit users by discarded samples rate that grew the most between query range start and query range end') +
        $.queryPanel(
          |||
            sum by (user) (rate(cortex_discarded_samples_total{%(job)s}[$__rate_interval]))
            and
            topk($limit,
              sum by (user) (rate(cortex_discarded_samples_total{%(job)s}[$__rate_interval] @ end()))
              -
              sum by (user) (rate(cortex_discarded_samples_total{%(job)s}[$__rate_interval] @ start()))
            )
          ||| % {
            job: $.jobMatcher($._config.job_names.ingester + $._config.job_names.distributor),
          },
          '{{ user }}',
        )
      ),
    )

    .addRow(
      ($.row('By series with exemplars') + { collapse: true })
      .addPanel(
        $.panel('Top $limit users by series with exemplars') +
        { sort: { col: 2, desc: true } } +
        $.tablePanel(
          [
            |||
              topk($limit, %s)
            ||| % [
              $.queries.ingester.ingestOrClassicDeduplicatedQuery('cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage{%s}' % [$.jobMatcher($._config.job_names.ingester)], groupByLabels='user'),
            ],
          ], {
            user: { alias: 'user', unit: 'string' },
            Value: { alias: 'series' },
          }
        )
      ),
    )

    .addRow(
      ($.row('By exemplars rate') + { collapse: true })
      .addPanel(
        $.panel('Top $limit users by received exemplars rate in last 5m') +
        { sort: { col: 2, desc: true } } +
        $.tablePanel(
          [
            'topk($limit, sum by (user) (rate(cortex_distributor_received_exemplars_total{%(job)s}[5m])))'
            % { job: $.jobMatcher($._config.job_names.distributor) },
          ], {
            user: { alias: 'user', unit: 'string' },
            Value: { alias: 'exemplars/s' },
          }
        )
      ),
    )


    .addRow(
      ($.row('By rule group size') + { collapse: true })
      .addPanel(
        $.panel('Top $limit biggest groups') +
        { sort: { col: 3, desc: true } } +
        $.tablePanel(
          [
            'topk($limit, sum by (rule_group, user) (cortex_prometheus_rule_group_rules{%(job)s}))'
            % { job: $.jobMatcher($._config.job_names.ruler) },
          ], {
            user: { alias: 'user', unit: 'string' },
            Value: { alias: 'rules' },
          }
        )
      ),
    )

    .addRow(
      ($.row('By rule group evaluation time') + { collapse: true })
      .addPanel(
        $.panel('Top $limit slowest groups (last evaluation)') +
        { sort: { col: 3, desc: true } } +
        $.tablePanel(
          [
            'topk($limit, sum by (rule_group, user) (cortex_prometheus_rule_group_last_duration_seconds{%(job)s}))'
            % { job: $.jobMatcher($._config.job_names.ruler) },
          ], {
            user: { alias: 'user', unit: 'string' },
            Value: { alias: 'seconds' },
          }
        )
      )
    )

    .addRow(
      ($.row('By estimated compaction jobs from bucket-index') + { collapse: true })
      .addPanel(
        $.panel('Top $limit users by estimated compaction jobs from bucket-index') +
        { sort: { col: 2, desc: true } } +
        $.tablePanel(
          [
            |||
              topk($limit,
                sum by (user) (cortex_bucket_index_estimated_compaction_jobs{%s})
                and ignoring(user)
                (sum(rate(cortex_bucket_index_estimated_compaction_jobs_errors_total{%s}[$__rate_interval])) == 0)
              )
            ||| % [$.jobMatcher($._config.job_names.compactor), $.jobMatcher($._config.job_names.compactor)],
          ], {
            user: { alias: 'user', unit: 'string' },
            Value: { alias: 'Compaction Jobs', decimals: 0 },
          }
        )
      ),
    ),
}
