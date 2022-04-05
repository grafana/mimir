local utils = import 'mixin-utils/utils.libsonnet';

(import 'dashboard-utils.libsonnet') {
  'mimir-tenants.json':
    ($.dashboard('Tenants') + { uid: '35fa247ce651ba189debf33d7ae41611' })
    .addClusterSelectorTemplates()
    .addActiveUserSelectorTemplates()
    .addCustomTemplate('limit', ['10', '50', '100', '500', '1000'])
    .addRowIf(
      $._config.show_dashboard_descriptions.tenants,
      ($.row('Tenants dashboard description') { height: '25px', showTitle: false })
      .addPanel(
        $.textPanel('', |||
          <p>
            This dashboard shows various metrics detailed by tenant (user) selected above.
          </p>
        |||),
      )
    )

    .addRow(
      $.row('Series and exemplars')
      .addPanel(
        local title = 'Series';
        $.panel(title) +
        $.queryPanel(
          [
            |||
              sum(
                (
                  cortex_ingester_memory_series_created_total{%(ingester)s, user="$user"}
                  - cortex_ingester_memory_series_removed_total{%(ingester)s, user="$user"}
                )
                / on(%(group_by_cluster)s) group_left
                max by (%(group_by_cluster)s) (cortex_distributor_replication_factor{%(distributor)s})
              )
            ||| % {
              ingester: $.jobMatcher($._config.job_names.ingester),
              distributor: $.jobMatcher($._config.job_names.distributor),
              group_by_cluster: $._config.group_by_cluster,
            },
            |||
              max(cortex_limits_overrides{%(overrides_exporter)s, limit_name="max_global_series_per_user", user="$user"})
              or
              max(cortex_limits_defaults{%(overrides_exporter)s, limit_name="max_global_series_per_user"})
            ||| % {
              overrides_exporter: $.jobMatcher($._config.job_names.overrides_exporter),
            },
            |||
              sum(
                cortex_ingester_active_series{%(ingester)s, user="$user"}
                / on(%(group_by_cluster)s) group_left
                max by (%(group_by_cluster)s) (cortex_distributor_replication_factor{%(distributor)s})
              )
            ||| % {
              ingester: $.jobMatcher($._config.job_names.ingester),
              distributor: $.jobMatcher($._config.job_names.distributor),
              group_by_cluster: $._config.group_by_cluster,
            },
            |||
              sum by (name) (
                cortex_ingester_active_series_custom_tracker{%(ingester)s, user="$user"}
                / on(%(group_by_cluster)s) group_left
                max by (%(group_by_cluster)s) (cortex_distributor_replication_factor{%(distributor)s})
              ) > 0
            ||| % {
              ingester: $.jobMatcher($._config.job_names.ingester),
              distributor: $.jobMatcher($._config.job_names.distributor),
              group_by_cluster: $._config.group_by_cluster,
            },
          ],
          [
            'in-memory',
            'limit',
            'active',
            'active ({{ name }})',
          ],
        ) +
        {
          seriesOverrides: [
            {
              alias: 'limit',
              fill: 0,
              dashes: true,
            },
          ],
        } +
        $.panelDescription(
          title,
          |||
            Number of active and in-memory series per user, and active series matching custom trackers (in parenthesis).
            Note that active series matching custom trackers are included in the total active series count.
          |||
        ),
      )
      .addPanel(
        local title = 'Series with exemplars';
        $.panel(title) +
        $.queryPanel(
          |||
            sum(
              cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage{%(ingester)s, user="$user"}
              / on(%(group_by_cluster)s) group_left
              max by (%(group_by_cluster)s) (cortex_distributor_replication_factor{%(distributor)s})
            )
          ||| % {
            ingester: $.jobMatcher($._config.job_names.ingester),
            distributor: $.jobMatcher($._config.job_names.distributor),
            group_by_cluster: $._config.group_by_cluster,
          },
          'series',
        ) +
        {
          legend: { show: false },
        } +
        $.panelDescription(
          title,
          |||
            Number of series with exemplars currently in storage.
          |||
        ),
      )
      .addPanel(
        local title = 'Newest seen sample age';
        $.panel(title) +
        $.queryPanel(
          'time() - max(cortex_distributor_latest_seen_sample_timestamp_seconds{%(distributor)s, user="$user"} > 0)'
          % { distributor: $.jobMatcher($._config.job_names.distributor) },
          'age',
        ) +
        { legend: { show: false }, yaxes: $.yaxes('s') } +
        $.panelDescription(
          title,
          |||
            The age of the newest received sample seen in the distributors.
          |||
        ),
      )
      .addPanel(
        local title = 'Oldest exemplar age';
        $.panel(title) +
        $.queryPanel(
          'time() - min(cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds{%(ingester)s, user="$user"} > 0)'
          % { ingester: $.jobMatcher($._config.job_names.ingester) },
          'age',
        ) +
        { legend: { show: false }, yaxes: $.yaxes('s') } +
        $.panelDescription(
          title,
          |||
            The age of the oldest exemplar stored in circular storage.
            Useful to check for what time range the current exemplar buffer limit allows.
            This usually means the max age for all exemplars for a typical setup.
            This is not true though if one of the series timestamp is in future compared to rest series.
          |||
        ),
      ),
    )

    .addRow(
      $.row('Samples ingestion funnel')
      .addPanel(
        local title = 'Distributor samples incoming rate';
        $.panel(title) +
        $.queryPanel(
          'sum(rate(cortex_distributor_samples_in_total{%(job)s, user="$user"}[$__rate_interval]))'
          % { job: $.jobMatcher($._config.job_names.distributor) },
          'rate',
        ) +
        { legend: { show: false } } +
        $.panelDescription(
          title,
          |||
            The rate of samples that have come in to the distributor, including rejected or deduped exemplars.
          |||
        ),
      )
      .addPanel(
        local title = 'Distributor samples received (accepted) rate';
        $.panel(title) +
        $.queryPanel(
          'sum(rate(cortex_distributor_received_samples_total{%(job)s, user="$user"}[$__rate_interval]))'
          % { job: $.jobMatcher($._config.job_names.distributor) },
          'rate',
        ) +
        { legend: { show: false } } +
        $.panelDescription(
          title,
          |||
            The rate of received samples, excluding rejected and deduped samples.
          |||
        ),
      )
      .addPanel(
        local title = 'Distributor deduplicated/non-HA';
        $.panel(title) +
        $.queryPanel(
          [
            'sum(rate(cortex_distributor_deduped_samples_total{%(job)s, user="$user"}[$__rate_interval]))'
            % { job: $.jobMatcher($._config.job_names.distributor) },
            'sum(rate(cortex_distributor_non_ha_samples_received_total{%(job)s, user="$user"}[$__rate_interval]))'
            % { job: $.jobMatcher($._config.job_names.distributor) },
          ],
          [
            'deduplicated',
            'non-HA',
          ]
        ) +
        $.panelDescription(
          title,
          |||
            The rate of deduplicated samples and the rate of received samples for a user that has HA tracking turned on, but the sample didn't contain both HA labels.
          |||
        ),
      )
      .addPanel(
        local title = 'Distributor and ingester discarded samples rate';
        $.panel(title) +
        $.queryPanel(
          [
            'sum by (reason) (rate(cortex_discarded_samples_total{%(job)s, user="$user"}[$__rate_interval]))'
            % { job: $.jobMatcher($._config.job_names.distributor) },
            'sum by (reason) (rate(cortex_discarded_samples_total{%(job)s, user="$user"}[$__rate_interval]))'
            % { job: $.jobMatcher($._config.job_names.ingester) },
          ],
          [
            '{{ reason }} (distributor)',
            '{{ reason }} (ingester)',
          ]
        ) +
        $.panelDescription(
          title,
          |||
            The rate of each sample's discarding reason.
          |||
        ),
      ),
    )

    .addRow(
      $.row('Exemplars ingestion funnel')
      .addPanel(
        local title = 'Distributor exemplars incoming rate';
        $.panel(title) +
        $.queryPanel(
          'sum(rate(cortex_distributor_exemplars_in_total{%(job)s, user="$user"}[$__rate_interval]))'
          % { job: $.jobMatcher($._config.job_names.distributor) },
          'rate',
        ) +
        { legend: { show: false } } +
        $.panelDescription(
          title,
          |||
            The rate of exemplars that have come in to the distributor, including rejected or deduped exemplars.
          |||
        ),
      )
      .addPanel(
        local title = 'Distributor exemplars received (accepted) rate';
        $.panel(title) +
        $.queryPanel(
          'sum(rate(cortex_distributor_received_exemplars_total{%(job)s, user="$user"}[$__rate_interval]))'
          % { job: $.jobMatcher($._config.job_names.distributor), group_prefix_users: $._config.group_prefix_users },
          'rate',
        ) +
        { legend: { show: false } } +
        $.panelDescription(
          title,
          |||
            The rate of received exemplars, excluding rejected and deduped exemplars.
            This number can be sensibly lower than incoming rate because we dedupe the HA sent exemplars, and then reject based on time.
            See discarded rate for reasons why exemplars are being discarded.
          |||
        ),
      )
      .addPanel(
        local title = 'Distributor discarded exemplars rate';
        $.panel(title) +
        $.queryPanel(
          'sum by (reason) (rate(cortex_discarded_exemplars_total{%(job)s, user="$user"}[$__rate_interval]))'
          % { job: $.jobMatcher($._config.job_names.distributor) },
          '{{ reason }}',
        ) +
        $.panelDescription(
          title,
          |||
            The rate of each exmplars' discarding reason.
          |||
        ),
      )
      .addPanel(
        local title = 'Ingester appended exemplars rate';
        $.panel(title) +
        $.queryPanel(
          |||
            sum(
              rate(cortex_ingester_tsdb_exemplar_exemplars_appended_total{%(ingester)s, user="$user"}[$__rate_interval])
              / on(%(group_by_cluster)s) group_left
              max by (%(group_by_cluster)s) (cortex_distributor_replication_factor{%(distributor)s})
            )
          ||| % {
            ingester: $.jobMatcher($._config.job_names.ingester),
            distributor: $.jobMatcher($._config.job_names.distributor),
            group_by_cluster: $._config.group_by_cluster,
          },
          'rate',
        ) +
        { legend: { show: false } } +
        $.panelDescription(
          title,
          |||
            Total number of exemplars appended in the ingesters.
            This can be lower than ingested exemplars rate since TSDB does not append the same exemplar twice, and those can be frequent.
          |||
        ),
      ),
    )

    .addRow(
      ($.row("Ingesters' storage") + { collapse: true })
      .addPanel(
        local title = 'Symbol table size for loaded blocks';
        $.panel(title) +
        $.queryPanel(
          'sum by (job) (cortex_ingester_tsdb_symbol_table_size_bytes{%(ingester)s, user="$user"})'
          % { ingester: $.jobMatcher($._config.job_names.ingester) },
          '{{ job }}',
        ) +
        { yaxes: $.yaxes('bytes') } +
        $.panelDescription(
          title,
          |||
            Size of symbol table in memory for loaded blocks, averaged by ingester.
          |||
        ),
      )
      .addPanel(
        local title = 'Space used by local blocks';
        $.panel(title) +
        $.queryPanel(
          'sum by (job) (cortex_ingester_tsdb_storage_blocks_bytes{%(ingester)s, user="$user"})'
          % { ingester: $.jobMatcher($._config.job_names.ingester) },
          '{{ job }}',
        ) +
        { yaxes: $.yaxes('bytes') } +
        $.panelDescription(
          title,
          |||
            The number of bytes that are currently used for local storage by all blocks.
          |||
        ),
      ),
    )

    .addRow(
      $.row('Rules')
      .addPanel(
        local title = 'Number of groups';
        $.panel(title) +
        $.queryPanel(
          'count(sum by (rule_group) (cortex_prometheus_rule_group_rules{%(job)s, user="$user"}))'
          % { job: $.jobMatcher($._config.job_names.ruler) },
          'groups',
        ) +
        { legend: { show: false } } +
        $.panelDescription(
          title,
          |||
            Total number of rule groups for a tenant.
          |||
        ),
      )
      .addPanel(
        local title = 'Number of rules';
        $.panel(title) +
        $.queryPanel(
          'sum(cortex_prometheus_rule_group_rules{%(job)s, user="$user"})'
          % { job: $.jobMatcher($._config.job_names.ruler) },
          'rules',
        ) +
        { legend: { show: false } } +
        $.panelDescription(
          title,
          |||
            Total number of rules for a tenant.
          |||
        ),
      )
      .addPanel(
        local title = 'Total evaluations rate';
        $.panel(title) +
        $.queryPanel(
          'sum(rate(cortex_prometheus_rule_evaluations_total{%(job)s, user="$user"}[$__rate_interval]))'
          % { job: $.jobMatcher($._config.job_names.ruler) },
          'rate',
        ) +
        { legend: { show: false } },
      )
      .addPanel(
        local title = 'Failed evaluations rate';
        $.panel(title) +
        $.queryPanel(
          'sum by (rule_group) (rate(cortex_prometheus_rule_evaluation_failures_total{%(job)s, user="$user"}[$__rate_interval])) > 0'
          % { job: $.jobMatcher($._config.job_names.ruler) },
          '{{ rule_group }}',
        ) + { stack: true },
      )
    )

    .addRow(
      ($.row('Top rules') + { collapse: true })
      .addPanel(
        $.panel('Top $limit biggest groups') +
        { sort: { col: 2, desc: true } } +
        $.tablePanel(
          [
            'topk($limit, sum by (rule_group) (cortex_prometheus_rule_group_rules{%(job)s, user="$user"}))'
            % { job: $.jobMatcher($._config.job_names.ruler) },
          ],
          { 'Value #A': { alias: 'rules' } }
        )
      )
      .addPanel(
        $.panel('Top $limit slowest groups (last evaluation)') +
        { sort: { col: 2, desc: true } } +
        $.tablePanel(
          [
            'topk($limit, sum by (rule_group) (cortex_prometheus_rule_group_last_duration_seconds{%(job)s, user="$user"}))'
            % { job: $.jobMatcher($._config.job_names.ruler) },
          ],
          { 'Value #A': { alias: 'seconds' } }
        )
      )
    )

    .addRow(
      $.row('Notifications')
      .addPanel(
        local title = 'Sent notifications rate';
        $.panel(title) +
        $.queryPanel(
          'sum(rate(cortex_prometheus_notifications_sent_total{%(job)s, user="$user"}[$__rate_interval]))'
          % { job: $.jobMatcher($._config.job_names.ruler) },
          'rate',
        ) +
        { legend: { show: false } },
      )
      .addPanel(
        local title = 'Failed notifications rate';
        $.panel(title) +
        $.queryPanel(
          'sum(rate(cortex_prometheus_notifications_errors_total{%(job)s, user="$user"}[$__rate_interval]))'
          % { job: $.jobMatcher($._config.job_names.ruler) },
          'rate',
        ) +
        { legend: { show: false } },
      )
    ),
}
