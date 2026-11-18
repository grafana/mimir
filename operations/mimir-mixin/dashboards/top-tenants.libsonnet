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

  local othersLegend = 'others',

  local tenantDashboardLink(tenant) = {
    title: 'Tenant dashboard for ${%s}' % tenant,
    url: $.dashboardURL('mimir-tenants.json') + '&var-user=${%s:percentencode}' % tenant,
    targetBlank: true,
  },

  // In a table, makes the tenant column link to the tenant's dashboard.
  local tableTenantLinks(tenantLabel) = {
    fieldConfig+: {
      overrides+: [
        $.overrideFieldByName(tenantLabel, [
          $.overrideProperty('links', [tenantDashboardLink('__value.raw')]),
        ]),
      ],
    },
  },


  // Shows the share of each of the top $limit tenants by the given per-tenant query, plus the sum of
  // all the other tenants as "others", so that each slice is proportional to the real total.
  // Only use it for values that can be summed up across tenants.
  local topTenantsPieChartPanel(title, query, legend='{{ user }}', unit='short') = {
    datasource: '$datasource',
    title: title,
    type: 'piechart',
    targets: [
      {
        expr: 'sort_desc(topk($limit, %s))' % query,
        format: 'time_series',
        instant: true,
        legendFormat: legend,
        refId: 'A',
      },
      {
        expr: |||
          sum(
            (%(query)s)
            unless
            topk($limit, %(query)s)
          )
        ||| % { query: query },
        format: 'time_series',
        instant: true,
        legendFormat: othersLegend,
        refId: 'B',
      },
    ],
    fieldConfig: {
      defaults: {
        unit: unit,
      },
      overrides: [
        $.overrideFieldByName(othersLegend, [
          $.overrideProperty('color', { mode: 'fixed', fixedColor: '#8F8F8F' }),
        ]),
      ],
    },
    options: {
      pieType: 'donut',
      // Keep the queries' order: tenants from largest to smallest, then "others".
      sort: 'none',
      displayLabels: [],
      reduceOptions: {
        calcs: ['lastNotNull'],
        fields: '',
        values: false,
      },
      // Only the names: the table next to the pie chart shows the values and percentages.
      legend: {
        showLegend: true,
        displayMode: 'list',
        placement: 'right',
        values: [],
      },
      tooltip: {
        mode: 'single',
        sort: 'none',
      },
    },
  },

  // Builds a row with a table of the top $limit tenants by the given per-tenant query, with their
  // percentage of the total, next to a pie chart with their share of the total.
  local topTenantsPieChartRow(rowTitle, panelTitle, query, tableStyles, sortCol=2, tenantLabel='user', legend='{{ user }}', unit='short') =
    local styles = {
      [k]: tableStyles[k]
      for k in std.objectFields(tableStyles)
      if k != 'Value'
    } + {
      'Value #A': tableStyles.Value,
      'Value #B': { alias: '% of total', unit: 'percentunit', decimals: 1 },
    };
    $.row(rowTitle) { collapse: true }
    .addPanel(
      $.panel(panelTitle) +
      { sort: { col: sortCol, desc: true } } +
      $.tablePanel(
        [
          'topk($limit, %s)' % query,
          'topk($limit, %(query)s) / scalar(sum(%(query)s))' % { query: query },
        ],
        styles,
      ) +
      {
        // Join the two queries' rows by their labels, into one row per tenant.
        transformations: [{ id: 'merge', options: {} }],
      } +
      tableTenantLinks(tenantLabel)
    )
    .addPanel(topTenantsPieChartPanel('Share of the total', query, legend, unit))
    + {
      panels: [super.panels[0] { span: 9 }, super.panels[1] { span: 3 }],
    },

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
      topTenantsPieChartRow(
        'By active series',
        'Top $limit users by active series',
        $.queries.ingester.ingestOrClassicDeduplicatedQuery('cortex_ingester_active_series{%s}' % [$.jobMatcher($._config.job_names.ingester)], groupByLabels='user'),
        {
          user: { alias: 'user', unit: 'string' },
          Value: { alias: 'series' },
        },
      )
    )

    .addRow(
      topTenantsPieChartRow(
        'By in-memory series',
        'Top $limit users by in-memory series (series created - series removed)',
        in_memory_series_per_user_query(),
        {
          user: { alias: 'user', unit: 'string' },
          Value: { alias: 'series' },
        },
      )
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
        ) +
        $.showAllTooltip,
      ),
    )

    .addRow(
      topTenantsPieChartRow(
        'By samples rate',
        'Top $limit users by received samples rate in last 5m',
        'sum by (user) (rate(cortex_distributor_received_samples_total{%(job)s}[5m]))'
        % { job: $.jobMatcher($._config.job_names.distributor) },
        {
          user: { alias: 'user', unit: 'string' },
          Value: { alias: 'samples/s' },
        },
      )
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
        ) +
        $.showAllTooltip,
      ),
    )

    .addRow(
      ($.row('By query expression length') + { collapse: true })
      .addPanel(
        $.panel('Top $limit users by query expression length') +
        $.tablePanel(
          |||
            topk($limit,
              histogram_quantile(
                0.99,
                sum by(user) (rate(cortex_query_frontend_queries_expression_bytes{%(job)s}[$__rate_interval]))
              )
            )
          ||| % {
            job: $.jobMatcher($._config.job_names.query_frontend),
          },
          {
            user: { alias: 'User', unit: 'string' },
            Value: { alias: 'Bytes (99th Percentile)', unit: 'bytes' },
          },
        ) +
        tableTenantLinks('user')
      ),
    )

    .addRowIf(
      $._config.gateway_per_tenant_metrics_enabled,
      topTenantsPieChartRow(
        'By gateway read requests rate',
        'Top $limit users by gateway read requests rate in last 5m',
        'sum by (tenant) (rate(cortex_per_tenant_request_total{route=~"%s", %s}[5m]))'
        % [$.queries.read_http_routes_regex, $.namespaceMatcher()],
        {
          tenant: { alias: 'tenant', unit: 'string' },
          Value: { alias: 'requests/s' },
        },
        tenantLabel='tenant',
        legend='{{ tenant }}',
        unit='reqps',
      )
    )

    .addRowIf(
      $._config.gateway_per_tenant_metrics_enabled,
      topTenantsPieChartRow(
        'By gateway write requests rate',
        'Top $limit users by gateway write requests rate in last 5m',
        'sum by (tenant) (rate(cortex_per_tenant_request_total{route=~"%s", %s}[5m]))'
        % [$.queries.write_http_routes_regex, $.namespaceMatcher()],
        {
          tenant: { alias: 'tenant', unit: 'string' },
          Value: { alias: 'requests/s' },
        },
        tenantLabel='tenant',
        legend='{{ tenant }}',
        unit='reqps',
      )
    )


    .addRow(
      topTenantsPieChartRow(
        'By discarded samples rate',
        'Top $limit users by discarded samples rate in last 5m',
        'sum by (user) (rate(cortex_discarded_samples_total{%(job)s}[5m]))'
        % { job: $.jobMatcher($._config.job_names.ingester + $._config.job_names.distributor) },
        {
          user: { alias: 'user', unit: 'string' },
          Value: { alias: 'samples/s' },
        },
      )
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
        ) +
        $.showAllTooltip,
      ),
    )

    .addRow(
      topTenantsPieChartRow(
        'By series with exemplars',
        'Top $limit users by series with exemplars',
        $.queries.ingester.ingestOrClassicDeduplicatedQuery('cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage{%s}' % [$.jobMatcher($._config.job_names.ingester)], groupByLabels='user'),
        {
          user: { alias: 'user', unit: 'string' },
          Value: { alias: 'series' },
        },
      )
    )

    .addRow(
      topTenantsPieChartRow(
        'By exemplars rate',
        'Top $limit users by received exemplars rate in last 5m',
        'sum by (user) (rate(cortex_distributor_received_exemplars_total{%(job)s}[5m]))'
        % { job: $.jobMatcher($._config.job_names.distributor) },
        {
          user: { alias: 'user', unit: 'string' },
          Value: { alias: 'exemplars/s' },
        },
      )
    )


    .addRow(
      topTenantsPieChartRow(
        'By rule group size',
        'Top $limit biggest groups',
        'sum by (rule_group, user) (cortex_prometheus_rule_group_rules{%(job)s})'
        % { job: $.jobMatcher($._config.job_names.ruler) },
        {
          user: { alias: 'user', unit: 'string' },
          Value: { alias: 'rules' },
        },
        sortCol=3,
        legend='{{ user }} / {{ rule_group }}',
      )
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
        ) +
        tableTenantLinks('user')
      )
    )

    .addRow(
      topTenantsPieChartRow(
        'By estimated compaction jobs from bucket-index',
        'Top $limit users by estimated compaction jobs from bucket-index',
        |||
          sum by (user) (cortex_bucket_index_estimated_compaction_jobs{%s})
          and ignoring(user)
          (sum(rate(cortex_bucket_index_estimated_compaction_jobs_errors_total{%s}[$__rate_interval])) == 0)
        ||| % [$.jobMatcher($._config.job_names.compactor), $.jobMatcher($._config.job_names.compactor)],
        {
          user: { alias: 'user', unit: 'string' },
          Value: { alias: 'Compaction Jobs', decimals: 0 },
        },
      )
    )

    .addRow(
      ($.row('By store-gateway disk utilization') + { collapse: true })
      .addPanel(
        $.panel('Top $limit users by per-store-gateway disk utilization') +
        { sort: { col: 2, desc: true } } +
        $.tablePanel(
          [
            |||
              topk($limit,
                max by (user) (sum by (user, %s) (cortex_bucket_store_blocks_loaded_size_bytes{%s}))
              )
            ||| % [$._config.per_instance_label, $.jobMatcher($._config.job_names.store_gateway)],
          ], {
            user: { alias: 'user', unit: 'string' },
            Value: { alias: 'per-instance disk usage', unit: 'bytes' },
          }
        ) +
        tableTenantLinks('user')
      ),
    ),
}
