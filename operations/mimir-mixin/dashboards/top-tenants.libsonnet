local utils = import 'mixin-utils/utils.libsonnet';

(import 'dashboard-utils.libsonnet') {
  local in_memory_series_per_user_query(at='') = |||
    sum by (user) (
      (
          sum by (user, %(group_by_cluster)s) (cortex_ingester_memory_series_created_total{%(ingester)s} %(at)s)
          -
          sum by (user, %(group_by_cluster)s) (cortex_ingester_memory_series_removed_total{%(ingester)s} %(at)s)
      )
      / on(%(group_by_cluster)s) group_left
      max by (%(group_by_cluster)s) (cortex_distributor_replication_factor{%(distributor)s} %(at)s)
    )
  ||| % {
    at: at,
    ingester: $.jobMatcher($._config.job_names.ingester),
    distributor: $.jobMatcher($._config.job_names.distributor),
    group_by_cluster: $._config.group_by_cluster,
  },

  'mimir-top-tenants.json':
    ($.dashboard('Top tenants') + { uid: 'bc6e12d4fe540e4a1785b9d3ca0ffdd9' })
    .addClusterSelectorTemplates()
    .addCustomTemplate('limit', ['10', '50', '100'])
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
              topk($limit,
                sum by (user) (
                  cortex_ingester_active_series{%(ingester)s}
                  / on(%(group_by_cluster)s) group_left
                  max by (%(group_by_cluster)s) (cortex_distributor_replication_factor{%(distributor)s})
                )
              )
            ||| % {
              ingester: $.jobMatcher($._config.job_names.ingester),
              distributor: $.jobMatcher($._config.job_names.distributor),
              group_by_cluster: $._config.group_by_cluster,
            },
          ],
          { 'Value #A': { alias: 'series' } }
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
          ],
          { 'Value #A': { alias: 'series' } }
        )
      ),
    )

    .addRow(
      ($.row('By in-memory series growth') + { collapse: true })
      .addPanel(
        local title = 'Top $limit users by in-memory series (series created - series removed) that grew the most between query range start and query range end';
        $.panel(title) +
        $.queryPanel(
          |||
            %(in_memory_series_per_user)s
            and
            topk($limit, %(in_memory_series_per_user_at_end)s - %(in_memory_series_per_user_at_start)s)
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
          ],
          { 'Value #A': { alias: 'samples/s' } }
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
              topk($limit,
                sum by (user) (
                  cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage{%(ingester)s}
                  / on(%(group_by_cluster)s) group_left
                  max by (%(group_by_cluster)s) (cortex_distributor_replication_factor{%(distributor)s})
                )
              )
            ||| % {
              ingester: $.jobMatcher($._config.job_names.ingester),
              distributor: $.jobMatcher($._config.job_names.distributor),
              group_by_cluster: $._config.group_by_cluster,
            },
          ],
          { 'Value #A': { alias: 'series' } }
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
          ],
          { 'Value #A': { alias: 'exemplars/s' } }
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
          ],
          { 'Value #A': { alias: 'rules' } }
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
          ],
          { 'Value #A': { alias: 'seconds' } }
        )
      )
    ),
}
