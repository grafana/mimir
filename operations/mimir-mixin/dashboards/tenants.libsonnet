local utils = import 'mixin-utils/utils.libsonnet';

(import 'dashboard-utils.libsonnet') {
  'mimir-tenants.json':
    ($.dashboard('Mimir / Tenants') + { uid: '35fa247ce651ba189debf33d7ae41611' })
    .addClusterSelectorTemplates()
    .addActiveUserSelectorTemplates()
    .addRowIf(
      $._config.show_dashboard_descriptions.tenants,
      ($.row('Tenants dashboard description') { height: '25px', showTitle: false })
      .addPanel(
        $.textPanel('', |||
          <p>
            This dashboard shows various metrics for detailed by tenant (user) selected above.
          </p>
        |||),
      )
    )

    .addRow(
      $.row('Active series & exemplars')
      .addPanel(
        local title = 'Active series';
        $.panel(title) +
        $.queryPanel(
          [
            |||
              sum by (user) (
                cortex_ingester_active_series{%(ingester)s, user=~"$user"}
                / on(%(group_by_cluster)s) group_left
                max by (%(group_by_cluster)s) (cortex_distributor_replication_factor{%(distributor)s})
              )
            ||| % {
              ingester: $.jobMatcher($._config.job_names.ingester),
              distributor: $.jobMatcher($._config.job_names.distributor),
              group_by_cluster: $._config.group_by_cluster,
            },
            |||
              sum by (user, name) (
                cortex_ingester_active_series_custom_tracker{%(ingester)s, user=~"$user"}
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
            '{{ user }}',
            '{{ user }} ({{ name }})',
          ],
        ) +
        $.panelDescription(
          title,
          |||
            Number of active series per user, and active series matching custom trackers (in parenthesis).
            Note that active series matching custom trackers are included in the total active series count.
          |||
        ),
      )
      .addPanel(
        local title = 'Series with exemplars';
        $.panel(title) +
        $.queryPanel(
          |||
            sum by (user) (
              cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage{%(ingester)s, user=~"$user"}
              / on(%(group_by_cluster)s) group_left
              max by (%(group_by_cluster)s) (cortex_distributor_replication_factor{%(distributor)s})
            )
          ||| % {
            ingester: $.jobMatcher($._config.job_names.ingester),
            distributor: $.jobMatcher($._config.job_names.distributor),
            group_by_cluster: $._config.group_by_cluster,
          },
          '{{ user }}',
        ) +
        $.panelDescription(
          title,
          |||
            Number of TSDB series with exemplars currently in storage.
          |||
        ),
      )
      .addPanel(
        local title = 'Newest seen sample age';
        $.panel(title) +
        $.queryPanel(
          'time() - max by (user) (cortex_distributor_latest_seen_sample_timestamp_seconds{%(distributor)s, user=~"$user"} > 0)'
          % { distributor: $.jobMatcher($._config.job_names.distributor) },
          '{{ user }}',
        ) +
        { yaxes: $.yaxes('s') } +
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
          'time() - min by (user) (cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds{%(ingester)s, user=~"$user"} > 0)'
          % { ingester: $.jobMatcher($._config.job_names.ingester) },
          '{{ user }}',
        ) +
        { yaxes: $.yaxes('s') } +
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
          'sum by (user) (rate(cortex_distributor_samples_in_total{%(job)s, user=~"$user"}[5m]))'
          % { job: $.jobMatcher($._config.job_names.distributor) },
          '{{ user }}',
        ) +
        $.panelDescription(
          title,
          |||
            The rate of samples that have come in to the distributor, including rejected or deduped exemplars.
          |||
        ),
      )
      .addPanel(
        local title = 'Distributor samples received rate';
        $.panel(title) +
        $.queryPanel(
          'sum by (user) (rate(cortex_distributor_received_samples_total{%(job)s, user=~"$user"}[5m]))'
          % { job: $.jobMatcher($._config.job_names.distributor) },
          '{{ user }}',
        ) +
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
            'sum by (user) (rate(distributor_deduped_samples_total{%(job)s, user=~"$user"}[5m]))'
            % { job: $.jobMatcher($._config.job_names.distributor) },
            'sum by (user) (rate(distributor_non_ha_samples_received_total{%(job)s, user=~"$user"}[5m]))'
            % { job: $.jobMatcher($._config.job_names.distributor) },
          ],
          [
            '{{ user }}: deduplicated',
            '{{ user }}: non-HA',
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
        local title = 'Distributor samples discarded rate';
        $.panel(title) +
        $.queryPanel(
          'sum by (user, reason) (rate(cortex_discarded_samples_total{%(job)s, user=~"$user"}[5m]))'
          % { job: $.jobMatcher($._config.job_names.distributor) },
          '{{ user }}: {{ reason }}',
        ) +
        $.panelDescription(
          title,
          |||
            The rate of each samples' discarding reason.
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
          'sum by (user) (rate(cortex_distributor_exemplars_in_total{%(job)s, user=~"$user"}[5m]))'
          % { job: $.jobMatcher($._config.job_names.distributor) },
          '{{ user }}',
        ) +
        $.panelDescription(
          title,
          |||
            The rate of exemplars that have come in to the distributor, including rejected or deduped exemplars.
          |||
        ),
      )
      .addPanel(
        local title = 'Distributor exemplars received rate';
        $.panel(title) +
        $.queryPanel(
          'sum by (user) (rate(cortex_distributor_received_exemplars_total{%(job)s, user=~"$user"}[5m]))'
          % { job: $.jobMatcher($._config.job_names.distributor), group_prefix_users: $._config.group_prefix_users },
          '{{ user }}',
        ) +
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
        local title = 'Distributor exemplars discarded rate';
        $.panel(title) +
        $.queryPanel(
          'sum by (user, reason) (rate(cortex_discarded_exemplars_total{%(job)s, user=~"$user"}[5m]))'
          % { job: $.jobMatcher($._config.job_names.distributor) },
          '{{ user }}: {{ reason }}',
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
            sum by (user) (
              rate(cortex_ingester_tsdb_exemplar_exemplars_appended_total{%(ingester)s, user=~"$user"}[5m])
              / on(%(group_by_cluster)s) group_left
              max by (%(group_by_cluster)s) (cortex_distributor_replication_factor{%(distributor)s})
            )
          ||| % {
            ingester: $.jobMatcher($._config.job_names.ingester),
            distributor: $.jobMatcher($._config.job_names.distributor),
            group_by_cluster: $._config.group_by_cluster,
          },
          '{{ user }}',
        ) +
        $.panelDescription(
          title,
          |||
            Total number of TSDB exemplars appended in the ingesters.
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
          'sum by (user, job) (cortex_ingester_tsdb_symbol_table_size_bytes{%(ingester)s, user=~"$user"})'
          % { ingester: $.jobMatcher($._config.job_names.ingester) },
          '{{ user }} in {{ job }}',
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
          'sum by (user, job) (cortex_ingester_tsdb_storage_blocks_bytes{%(ingester)s, user=~"$user"})'
          % { ingester: $.jobMatcher($._config.job_names.ingester) },
          '{{ user }} in {{ job }}',
        ) +
        { yaxes: $.yaxes('bytes') } +
        $.panelDescription(
          title,
          |||
            The number of bytes that are currently used for local storage by all blocks.
          |||
        ),
      ),
    ),
}
