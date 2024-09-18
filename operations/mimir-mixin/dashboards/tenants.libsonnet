local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-tenants.json';

(import 'dashboard-utils.libsonnet') +
(import 'dashboard-queries.libsonnet') {
  local user_limits_overrides_query(limit_name) = |||
    max(cortex_limits_overrides{%(overrides_exporter)s, limit_name="%(limit_name)s", user="$user"})
    or
    max(cortex_limits_defaults{%(overrides_exporter)s, limit_name="%(limit_name)s"})
  ||| % {
    overrides_exporter: $.jobMatcher($._config.job_names.overrides_exporter),
    limit_name: limit_name,
  },

  local limitStyle = $.overrideFieldByName('limit', [
    $.overrideProperty('custom.fillOpacity', 0),
    $.overrideProperty('custom.lineStyle', { fill: 'dash' }),
  ]),

  // These are the IDs of the 'All series', 'Native histogram series', and 'Total number of buckets used by native histogram series' panels
  // Not ideal, since these will change if the dashboard is rearranged
  local reload_annotation_panel_ids = [2, 7, 8],

  [filename]:
    assert std.md5(filename) == '35fa247ce651ba189debf33d7ae41611' : 'UID of the dashboard has changed, please update references to dashboard.';
    ($.dashboard('Tenants') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates()
    .addActiveUserSelectorTemplates()
    .addCustomTemplate(
      'limit', 'limit', [
        { label: '10', value: '10' },
        { label: '50', value: '50' },
        { label: '100', value: '100' },
        { label: '500', value: '500' },
        { label: '1000', value: '1000' },
      ]
    )
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
      $.row('Tenant series counts')
      .addPanel(
        local title = 'All series';
        $.timeseriesPanel(title) +
        $.queryPanel(
          [
            local perIngesterInMemorySeries = |||
              (
                cortex_ingester_memory_series_created_total{%(ingester)s, user="$user"}
                - cortex_ingester_memory_series_removed_total{%(ingester)s, user="$user"}
              )
            ||| % {
              ingester: $.jobMatcher($._config.job_names.ingester),
            };
            $.queries.ingester.ingestOrClassicDeduplicatedQuery(perIngesterInMemorySeries),
            user_limits_overrides_query('max_global_series_per_user'),
            $.queries.ingester.ingestOrClassicDeduplicatedQuery('cortex_ingester_active_series{%s, user="$user"}' % [$.jobMatcher($._config.job_names.ingester)]),
            $.queries.ingester.ingestOrClassicDeduplicatedQuery('cortex_ingester_owned_series{%s, user="$user"}' % [$.jobMatcher($._config.job_names.ingester)]),
            $.queries.ingester.ingestOrClassicDeduplicatedQuery('cortex_ingester_active_series_custom_tracker{%s, user="$user"}' % [$.jobMatcher($._config.job_names.ingester)], groupByLabels='name'),
          ],
          [
            'in-memory',
            'limit',
            'active',
            'owned',
            'active ({{ name }})',
          ],
        ) +
        { fieldConfig+: { overrides+: [limitStyle] } } +
        $.panelDescription(
          title,
          |||
            Number of active, in-memory, and owned series per user, and active series matching custom trackers (in parenthesis).
            Note that these counts include all series regardless of the type of data (counter, gauge, native histogram, etc.).
            Note that active series matching custom trackers are included in the total active series count.
          |||
        ),
      )
      .addPanel(
        local title = 'In-memory series per ingester';
        $.timeseriesPanel(title) +
        $.queryPanel(
          [
            |||
              min by (job) (cortex_ingester_local_limits{%(ingester)s, limit="max_global_series_per_user", user="$user"})
            ||| % {
              ingester: $.jobMatcher($._config.job_names.ingester),
            },
            |||
              cortex_ingester_memory_series_created_total{%(ingester)s, user="$user"}
              - cortex_ingester_memory_series_removed_total{%(ingester)s, user="$user"}
            ||| % {
              ingester: $.jobMatcher($._config.job_names.ingester),
            },
          ],
          [
            'local limit ({{job}})',
            '{{pod}}',
          ],
        ) +
        $.showAllTooltip +
        {
          fieldConfig+: {
            defaults+: { custom+: { fillOpacity: 0 } },
            overrides+: [
              $.overrideField('byRegexp', '/local limit .+/', [
                $.overrideProperty('custom.lineStyle', { fill: 'dash' }),
                $.overrideProperty('color', { mode: 'fixed', fixedColor: 'yellow' }),
              ]),
            ],
          },
          options+: {
            legend+: { showLegend: false },
          },
        } +
        $.panelDescription(
          title,
          |||
            Local tenant series limit and number of in-memory series per ingester.
            Because series can be unevenly distributed across ingesters, ingesters may hit the local limit at different times.
            Note that in-memory series may exceed the local limit if limiting based on owned series is enabled.
          |||
        ),
      )
      .addPanel(
        local title = 'Owned series per ingester';
        $.timeseriesPanel(title) +
        $.queryPanel(
          [
            |||
              min by (job) (cortex_ingester_local_limits{%(ingester)s, limit="max_global_series_per_user", user="$user"})
            ||| % {
              ingester: $.jobMatcher($._config.job_names.ingester),
            },
            |||
              cortex_ingester_owned_series{%(ingester)s, user="$user"}
            ||| % {
              ingester: $.jobMatcher($._config.job_names.ingester),
            },
          ],
          [
            'local limit ({{job}})',
            '{{pod}}',
          ],
        ) +
        $.showAllTooltip +
        {
          fieldConfig+: {
            defaults+: { custom+: { fillOpacity: 0 } },
            overrides+: [
              $.overrideField('byRegexp', '/local limit .+/', [
                $.overrideProperty('custom.lineStyle', { fill: 'dash' }),
                $.overrideProperty('color', { mode: 'fixed', fixedColor: 'yellow' }),
              ]),
            ],
          },
          options+: {
            legend+: { showLegend: false },
          },
        } +
        $.panelDescription(
          title,
          |||
            Local tenant series limit and number of owned series per ingester.
            Because series can be unevenly distributed across ingesters, ingesters may hit the local limit at different times.
            Owned series are the subset of an ingester's in-memory series that currently map to it in the ring
          |||
        ),
      )
    )

    .addRow(
      $.row('Exemplars and native histograms')
      .addPanel(
        local title = 'Series with exemplars';
        $.timeseriesPanel(title) +
        $.queryPanel(
          $.queries.ingester.ingestOrClassicDeduplicatedQuery('cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage{%(ingester)s, user="$user"}' % [$.jobMatcher($._config.job_names.ingester)]),
          'series',
        ) +
        { options+: { legend+: { showLegend: false } } } +
        $.panelDescription(
          title,
          |||
            Number of series with exemplars currently in storage.
          |||
        ),
      )
      .addPanel(
        local title = 'Oldest exemplar age';
        $.timeseriesPanel(title) +
        $.queryPanel(
          'time() - min(cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds{%(ingester)s, user="$user"} > 0)'
          % { ingester: $.jobMatcher($._config.job_names.ingester) },
          'age',
        ) +
        {
          fieldConfig+: { defaults+: { unit: 's' } },
          options+: { legend+: { showLegend: false } },
        } +
        $.panelDescription(
          title,
          |||
            The age of the oldest exemplar stored in circular storage.
            Useful to check for what time range the current exemplar buffer limit allows.
            This usually means the max age for all exemplars for a typical setup.
            This is not true though if one of the series timestamp is in future compared to rest series.
          |||
        ),
      )
      .addPanel(
        local title = 'Native histogram series';
        $.timeseriesPanel(title) +
        $.queryPanel(
          [
            $.queries.ingester.ingestOrClassicDeduplicatedQuery('cortex_ingester_active_native_histogram_series{%(ingester)s, user="$user"}' % [$.jobMatcher($._config.job_names.ingester)]),
            $.queries.ingester.ingestOrClassicDeduplicatedQuery('cortex_ingester_active_native_histogram_series_custom_tracker{%(ingester)s, user="$user"}' % [$.jobMatcher($._config.job_names.ingester)], groupByLabels='name'),
          ],
          [
            'active',
            'active ({{ name }})',
          ],
        ) +
        { fieldConfig+: { overrides+: [limitStyle] } } +
        $.panelDescription(
          title,
          |||
            Number of active native histogram series per user, and active native histogram series matching custom trackers (in parenthesis).
            Note that active series matching custom trackers are included in the total active series count.
          |||
        ),
      )
      .addPanel(
        local title = 'Total number of buckets used by native histogram series';
        $.timeseriesPanel(title) +
        $.queryPanel(
          [
            $.queries.ingester.ingestOrClassicDeduplicatedQuery('cortex_ingester_active_native_histogram_buckets{%(ingester)s, user="$user"}' % [$.jobMatcher($._config.job_names.ingester)]),
            $.queries.ingester.ingestOrClassicDeduplicatedQuery('cortex_ingester_active_native_histogram_buckets_custom_tracker{%(ingester)s, user="$user"}' % [$.jobMatcher($._config.job_names.ingester)], groupByLabels='name'),
          ],
          [
            'buckets',
            'buckets ({{ name }})',
          ],
        ) +
        { fieldConfig+: { overrides+: [limitStyle] } } +
        $.panelDescription(
          title,
          |||
            Total number of buckets in active native histogram series per user, and total active native histogram buckets matching custom trackers (in parenthesis).
          |||
        ),
      ),
    )

    .addRow(
      $.row('Distributor ingestion requests')
      .addPanel(
        local title = 'Distributor requests incoming rate';
        $.timeseriesPanel(title) +
        $.queryPanel(
          'sum(rate(cortex_distributor_requests_in_total{%(job)s, user="$user"}[$__rate_interval]))'
          % { job: $.jobMatcher($._config.job_names.distributor) },
          'rate',
        ) +
        { options+: { legend+: { showLegend: false } } } +
        $.panelDescription(
          title,
          |||
            The rate of requests that have come in to the distributor, including rejected requests.
          |||
        ),
      )
      .addPanel(
        local title = 'Distributor requests received (accepted) rate';
        $.timeseriesPanel(title) +
        $.queryPanel(
          [
            'sum(rate(cortex_distributor_received_requests_total{%(job)s, user="$user"}[$__rate_interval]))'
            % { job: $.jobMatcher($._config.job_names.distributor) },
            user_limits_overrides_query('request_rate'),
          ],
          [
            'rate',
            'limit',
          ],
        ) +
        { fieldConfig+: { overrides+: [limitStyle] } } +
        $.panelDescription(
          title,
          |||
            The rate of received requests, excluding rejected requests.
          |||
        ),
      )
      .addPanel(
        local title = 'Newest seen sample age';
        $.timeseriesPanel(title) +
        $.queryPanel(
          'time() - max(cortex_distributor_latest_seen_sample_timestamp_seconds{%(distributor)s, user="$user"} > 0)'
          % { distributor: $.jobMatcher($._config.job_names.distributor) },
          'age',
        ) +
        {
          fieldConfig+: { defaults+: { unit: 's' } },
          options+: { legend+: { showLegend: false } },
        } +
        $.panelDescription(
          title,
          |||
            The age of the newest received sample seen in the distributors.
          |||
        ),
      )
      .addPanel(
        local title = 'Distributor discarded requests rate';
        $.timeseriesPanel(title) +
        $.queryPanel(
          [
            'sum by (reason) (rate(cortex_discarded_requests_total{%(job)s, user="$user"}[$__rate_interval]))'
            % { job: $.jobMatcher($._config.job_names.distributor) },
          ],
          [
            '{{ reason }}',
          ]
        ) +
        $.panelDescription(
          title,
          |||
            The rate of each request's discarding reason.
          |||
        ),
      ),
    )

    .addRow(
      $.row('Samples ingestion funnel')
      .justifyPanels()  // Make sure panels use all width even if 12 columns are not divisible by the number of panels.
      .addPanel(
        local title = 'Distributor samples incoming rate';
        $.timeseriesPanel(title) +
        $.queryPanel(
          'sum(rate(cortex_distributor_samples_in_total{%(job)s, user="$user"}[$__rate_interval]))'
          % { job: $.jobMatcher($._config.job_names.distributor) },
          'rate',
        ) +
        { options+: { legend+: { showLegend: false } } } +
        $.panelDescription(
          title,
          |||
            The rate of samples that have come in to the distributor, including rejected or deduped exemplars.
          |||
        ),
      )
      .addPanel(
        local title = 'Distributor samples received (accepted) rate';
        $.timeseriesPanel(title) +
        $.queryPanel(
          [
            'sum(rate(cortex_distributor_received_samples_total{%(job)s, user="$user"}[$__rate_interval]))'
            % { job: $.jobMatcher($._config.job_names.distributor) },
            user_limits_overrides_query('ingestion_rate'),
          ],
          [
            'rate',
            'limit',
          ],
        ) +
        { fieldConfig+: { overrides+: [limitStyle] } } +
        $.panelDescription(
          title,
          |||
            The rate of received samples, excluding rejected and deduped samples.
          |||
        ),
      )
      .addPanel(
        local title = 'Distributor deduplicated/non-HA';
        $.timeseriesPanel(title) +
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
        $.timeseriesPanel(title) +
        $.queryPanel(
          [
            'sum by (reason) (rate(cortex_discarded_samples_total{%(job)s, user="$user"}[$__rate_interval]))'
            % { job: $.jobMatcher($._config.job_names.distributor) },
            'sum by (reason) (rate(cortex_discarded_samples_total{%(job)s, user="$user"}[$__rate_interval]))'
            % { job: $.jobMatcher($._config.job_names.ingester) },
          ],
          [
            '{{ reason }} (distributor)',
            '{{ reason }} (ingester, replicated)',
          ]
        ) +
        $.panelDescription(
          title,
          |||
            The rate of each sample's discarding reason.
            This doesn't account for the replication factor.
          |||
        ),
      )
      .addPanel(
        local title = 'Out-of-order samples appended';
        $.timeseriesPanel(title) +
        $.queryPanel(
          'sum(rate(cortex_ingester_tsdb_out_of_order_samples_appended_total{%(job)s, user="$user"}[$__rate_interval]))'
          % { job: $.jobMatcher($._config.job_names.ingester) },
          'rate',
        ) +
        { options+: { legend+: { showLegend: false } } } +
        $.panelDescription(
          title,
          |||
            The rate of OOO samples that have been appended.
            This doesn't account for the replication factor.
          |||
        ),
      ),
    )

    .addRow(
      $.row('Exemplars ingestion funnel')
      .addPanel(
        local title = 'Distributor exemplars incoming rate';
        $.timeseriesPanel(title) +
        $.queryPanel(
          'sum(rate(cortex_distributor_exemplars_in_total{%(job)s, user="$user"}[$__rate_interval]))'
          % { job: $.jobMatcher($._config.job_names.distributor) },
          'rate',
        ) +
        { options+: { legend+: { showLegend: false } } } +
        $.panelDescription(
          title,
          |||
            The rate of exemplars that have come in to the distributor, including rejected or deduped exemplars.
          |||
        ),
      )
      .addPanel(
        local title = 'Distributor exemplars received (accepted) rate';
        $.timeseriesPanel(title) +
        $.queryPanel(
          'sum(rate(cortex_distributor_received_exemplars_total{%(job)s, user="$user"}[$__rate_interval]))'
          % { job: $.jobMatcher($._config.job_names.distributor), group_prefix_users: $._config.group_prefix_users },
          'rate',
        ) +
        { options+: { legend+: { showLegend: false } } } +
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
        $.timeseriesPanel(title) +
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
        $.timeseriesPanel(title) +
        $.queryPanel(
          $.queries.ingester.ingestOrClassicDeduplicatedQuery('rate(cortex_ingester_tsdb_exemplar_exemplars_appended_total{%(ingester)s, user="$user"}[$__rate_interval])' % [$.jobMatcher($._config.job_names.ingester)]),
          'rate',
        ) +
        { options+: { legend+: { showLegend: false } } } +
        $.panelDescription(
          title,
          |||
            Total number of exemplars appended in the ingesters.
            This can be lower than ingested exemplars rate since TSDB does not append the same exemplar twice, and those can be frequent.
            This doesn't account for the replication factor.
          |||
        ),
      ),
    )

    .addRow(
      ($.row("Ingesters' storage") + { collapse: true })
      .addPanel(
        local title = 'Symbol table size for loaded blocks';
        $.timeseriesPanel(title) +
        $.queryPanel(
          'sum by (job) (cortex_ingester_tsdb_symbol_table_size_bytes{%(ingester)s, user="$user"})'
          % { ingester: $.jobMatcher($._config.job_names.ingester) },
          '{{ job }}',
        ) +
        { fieldConfig+: { defaults+: { unit: 'bytes' } } } +
        $.panelDescription(
          title,
          |||
            Size of symbol table in memory for loaded blocks, averaged by ingester.
          |||
        ),
      )
      .addPanel(
        local title = 'Space used by local blocks';
        $.timeseriesPanel(title) +
        $.queryPanel(
          'sum by (job) (cortex_ingester_tsdb_storage_blocks_bytes{%(ingester)s, user="$user"})'
          % { ingester: $.jobMatcher($._config.job_names.ingester) },
          '{{ job }}',
        ) +
        { fieldConfig+: { defaults+: { unit: 'bytes' } } } +
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
        $.timeseriesPanel(title) +
        $.queryPanel(
          [
            'count(sum by (rule_group) (cortex_prometheus_rule_group_rules{%(job)s, user="$user"}))'
            % { job: $.jobMatcher($._config.job_names.ruler) },
            user_limits_overrides_query('ruler_max_rule_groups_per_tenant'),
          ],
          [
            'groups',
            'limit',
          ]
        ) +
        { fieldConfig+: { overrides+: [limitStyle] } } +
        $.panelDescription(
          title,
          |||
            Total number of rule groups for a tenant.
          |||
        ),
      )
      .addPanel(
        local title = 'Number of rules';
        $.timeseriesPanel(title) +
        $.queryPanel(
          'sum(cortex_prometheus_rule_group_rules{%(job)s, user="$user"})'
          % { job: $.jobMatcher($._config.job_names.ruler) },
          'rules',
        ) +
        { options+: { legend+: { showLegend: false } } } +
        $.panelDescription(
          title,
          |||
            Total number of rules for a tenant.
          |||
        ),
      )
      .addPanel(
        local title = 'Total evaluations rate';
        $.timeseriesPanel(title) +
        $.queryPanel(
          'sum(rate(cortex_prometheus_rule_evaluations_total{%(job)s, user="$user"}[$__rate_interval]))'
          % { job: $.jobMatcher($._config.job_names.ruler) },
          'rate',
        ) +
        { options+: { legend+: { showLegend: false } } },
      )
      .addPanel(
        local title = 'Failed evaluations rate (top 50 rule groups)';
        $.timeseriesPanel(title) +
        $.queryPanel(
          'topk(50, sum by (rule_group) (rate(cortex_prometheus_rule_evaluation_failures_total{%(job)s, user="$user"}[$__rate_interval])) > 0)'
          % { job: $.jobMatcher($._config.job_names.ruler) },
          '{{ rule_group }}',
        ) +
        {
          fieldConfig+: {
            defaults+: {
              custom+: {
                stacking+: { mode: 'normal' },
              },
            },
          },
        },
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
        $.timeseriesPanel(title) +
        $.queryPanel(
          'sum(rate(cortex_prometheus_notifications_sent_total{%(job)s, user="$user"}[$__rate_interval]))'
          % { job: $.jobMatcher($._config.job_names.ruler) },
          'rate',
        ) +
        { options+: { legend+: { showLegend: false } } },
      )
      .addPanel(
        local title = 'Failed notifications rate';
        $.timeseriesPanel(title) +
        $.failurePanel(
          'sum(rate(cortex_prometheus_notifications_errors_total{%(job)s, user="$user"}[$__rate_interval]))'
          % { job: $.jobMatcher($._config.job_names.ruler) },
          'rate',
        ) +
        { options+: { legend+: { showLegend: false } } },
      )
    )

    .addRow(
      $.row('Alertmanager')
      .addPanel(
        $.timeseriesPanel('Alerts') +
        $.queryPanel(
          [
            'sum by (user) (cortex_alertmanager_alerts{%(job)s, user="$user"})' % { job: $.jobMatcher($._config.job_names.alertmanager) },
            'sum by (user) (cortex_alertmanager_silences{%(job)s, user="$user"})' % { job: $.jobMatcher($._config.job_names.alertmanager) },
          ],
          ['alerts', 'silences']
        )
      )
      .addPanel(
        $.timeseriesPanel('NPS') +
        $.successFailurePanel(
          |||
            (
            sum(rate(cortex_alertmanager_notifications_total{%(job)s, user="$user"}[$__rate_interval]))
            -
            on() (sum(rate(cortex_alertmanager_notifications_failed_total{%(job)s, user="$user"}[$__rate_interval])) or on () vector(0))
            ) > 0
          ||| % {
            job: $.jobMatcher($._config.job_names.alertmanager),
          },
          'sum(rate(cortex_alertmanager_notifications_failed_total{%(job)s, user="$user"}[$__rate_interval]))' % {
            job: $.jobMatcher($._config.job_names.alertmanager),
          },
        )
      )
      .addPanel(
        $.timeseriesPanel('NPS by integration') +
        $.queryPanel(
          [
            |||
              (
              sum(rate(cortex_alertmanager_notifications_total{%(job)s, user="$user"}[$__rate_interval])) by(integration)
              -
              (sum(rate(cortex_alertmanager_notifications_failed_total{%(job)s, user="$user"}[$__rate_interval])) by(integration) or
               (sum(rate(cortex_alertmanager_notifications_total{%(job)s, user="$user"}[$__rate_interval])) by(integration) * 0)
              )) > 0
            ||| % {
              job: $.jobMatcher($._config.job_names.alertmanager),
            },
            'sum(rate(cortex_alertmanager_notifications_failed_total{%(job)s, user="$user"}[$__rate_interval])) by(integration)' % {
              job: $.jobMatcher($._config.job_names.alertmanager),
            },
          ],
          ['success - {{ integration }}', 'failed - {{ integration }}']
        )
      )
    )

    .addRow(
      $.row('Read Path - Queries (User)')
      .addPanel(
        local title = 'Rate of Read Requests - query-frontend';
        $.timeseriesPanel(title) +
        $.queryPanel(
          'sum(rate(cortex_query_frontend_queries_total{%(job)s, user="$user"}[$__rate_interval]))'
          % { job: $.jobMatcher($._config.job_names.query_frontend) },
          'Queries / Sec'
        )
      )
      .addPanel(
        local title = 'Number of Queries Queued - query-scheduler';
        $.timeseriesPanel(title) +
        $.queryPanel(
          [
            'sum(cortex_query_scheduler_queue_length{%(job)s, user="$user"})'
            % { job: $.jobMatcher($._config.job_names.query_scheduler) },
          ],
          [
            'Queue Length',
          ],
        )
      )
    )
    .addRow(
      $.row('Read Path - Queries (Ruler)')
      .addPanel(
        local title = 'Rate of Read Requests - ruler-query-frontend';
        $.timeseriesPanel(title) +
        $.queryPanel(
          'sum(rate(cortex_query_frontend_queries_total{%(job)s, user="$user"}[$__rate_interval]))'
          % { job: $.jobMatcher($._config.job_names.ruler_query_frontend) },
          'Queries / Sec'
        )
      )
      .addPanel(
        local title = 'Number of Queries Queued - ruler-query-scheduler';
        $.timeseriesPanel(title) +
        $.queryPanel(
          [
            'sum(cortex_query_scheduler_queue_length{%(job)s, user="$user"})'
            % { job: $.jobMatcher($._config.job_names.ruler_query_scheduler) },
          ],
          [
            'Queue Length',
          ],
        )
      )
    )

    .addRow(
      ($.row('Compactions') + { collapse: true })
      .addPanel(
        $.timeseriesPanel('Estimated Compaction Jobs') +
        $.queryPanel(
          |||
            sum by (type) (cortex_bucket_index_estimated_compaction_jobs{%s, user="$user"})
            and ignoring(type)
            (sum(rate(cortex_bucket_index_estimated_compaction_jobs_errors_total{%s}[$__rate_interval])) == 0)
          ||| % [$.jobMatcher($._config.job_names.compactor), $.jobMatcher($._config.job_names.compactor)],
          '{{ job }}',
        ) +
        $.showAllTooltip + {
          fieldConfig+: {
            defaults+: {
              custom+: {
                fillOpacity: 50,
                stacking+: { mode: 'normal' },
              },
            },
          },
        } +
        $.panelDescription(
          'Estimated Compaction Jobs',
          |||
            Estimated number of compaction jobs for selected user, based on latest version of bucket index. When user sends data, ingesters upload new user blocks every 2 hours
            (shortly after 01:00 UTC, 03:00 UTC, 05:00 UTC, etc.), and compactors should process all of the blocks within 2h interval.
            If this graph regularly goes to zero (or close to zero) in 2 hour intervals, then compaction for this user works correctly.

            Depending on the configuration, there are two types of jobs: `split` jobs and `merge` jobs. Split jobs will only show up when user is configured with positive number of `compactor_split_and_merge_shards`.
            Values for split and merge jobs are stacked.
          |||
        )
      )

      .addPanel(
        $.timeseriesPanel('Blocks') +
        $.queryPanel(
          |||
            max by (user) (cortex_bucket_blocks_count{%s, user="$user"})
          ||| % [$.jobMatcher($._config.job_names.compactor)],
          '{{ job }}',
        ) +
        $.panelDescription(
          'Number of blocks',
          |||
            Number of blocks stored in long-term storage for this user.
          |||
        )
      ),
    ) + {
      annotations+: {
        list+: [
          {
            name: 'Active Series Reload',
            datasource: '$datasource',
            expr: 'sum by (user) (cortex_ingester_active_series_loading{%s, user="$user"}) > 0' % [$.jobMatcher($._config.job_names.ingester)],
            titleFormat: 'Active series reloading for user {{user}}',
            enable: true,
            hide: true,
            iconColor: 'yellow',
            filter: {
              exclude: false,
              ids: reload_annotation_panel_ids,
            },
          },
        ],
      },
    },
}
