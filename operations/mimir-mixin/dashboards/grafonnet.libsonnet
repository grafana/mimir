local g = import 'github.com/grafana/grafonnet/gen/grafonnet-latest/main.libsonnet';
local sortAscending = 1;


function(config) g {
  // Overrides of grafonnet functions
  dashboard+:: {
    new(title)::
      g.dashboard.new('%(prefix)s%(title)s' % { prefix: config.dashboard_prefix, title: title })
      + g.dashboard.withVariables(
        g.dashboard.variable.datasource.new('datasource', 'prometheus')
        + g.dashboard.variable.datasource.generalOptions.withLabel('Data source')
        + g.dashboard.variable.datasource.generalOptions.withCurrent(config.dashboard_datasource, config.dashboard_datasource)
        + g.dashboard.variable.datasource.withRegex(config.datasource_regex)
      )
      + g.dashboard.time.withFrom('now-1h')
      + g.dashboard.timepicker.withRefreshIntervals(['5s', '10s', '30s', '1m', '5m', '15m', '30m', '1h', '2h', '1d'])
      + g.dashboard.timepicker.withTimeOptions(['5m', '15m', '1h', '6h', '12h', '24h', '2d', '7d', '30d'])
      + g.dashboard.withRefresh('5m'),
  },
  panel+:: {
    row+:: {
      new(title)::
        g.panel.row.new(title)
        + g.panel.row.withCollapsed(false),
    },

    timeSeries+:: {
      new(title)::
        g.panel.timeSeries.new(title)
        + g.panel.timeSeries.gridPos.withH('7')
        + g.panel.timeSeries.gridPos.withW('24')
        + g.panel.timeSeries.options.legend.withShowLegend(true)
        + g.panel.timeSeries.options.tooltip.withMode('multi')
        + g.panel.timeSeries.options.tooltip.withSort('none'),
    },
  },

  // Mimir extensions
  mimir: {
    addClusterSelectorTemplates(multi=true)::
      local variable(name, query, label) =
        g.dashboard.variable.query.new(name, 'label_values(%s, %s)' % [query, label])
        + g.dashboard.variable.query.withDatasource('query', '$datasource')
        + g.dashboard.variable.query.generalOptions.withLabel(label)
        + g.dashboard.variable.query.withSort(sortAscending)
        + (
          if multi
          then
            g.dashboard.variable.query.selectionOptions.withMulti(true)
            + g.dashboard.variable.query.selectionOptions.withIncludeAll(true, '.+')
            + g.dashboard.variable.query.generalOptions.withCurrent('All', '$__all')
          else {}
        );

      local selectors = if config.singleBinary then [
        variable('job', config.dashboard_variables.job_query, config.per_job_label),
      ] else if multi then [
        variable('cluster', config.dashboard_variables.cluster_query, config.per_cluster_label),

        variable('namespace', config.dashboard_variables.namespace_query, config.per_namespace_label)
        + g.dashboard.variable.query.selectionOptions.withIncludeAll(false),
      ] else [
        variable('cluster', config.dashboard_variables.cluster_query, config.per_cluster_label)
        + g.dashboard.variable.query.selectionOptions.withIncludeAll(true, '.*'),

        variable('namespace', config.dashboard_variables.namespace_query, config.per_namespace_label),
      ];

      g.dashboard.withTags(config.tags)
      + g.dashboard.withLinks([
        g.dashboard.link.dashboards.new(title='%(product)s dashboards' % config, tags=config.tags)
        + g.dashboard.link.dashboards.options.withAsDropdown(true)
        + g.dashboard.link.dashboards.options.withIncludeVars(true)
        + g.dashboard.link.dashboards.options.withKeepTime(true)
        + g.dashboard.link.dashboards.options.withTargetBlank(false),
      ])
      + g.dashboard.withVariablesMixin(selectors),

    namespaceMatcher()::
      if config.singleBinary
      then '%s=~"$job"' % config.per_job_label
      else '%s=~"$cluster", %s=~"$namespace"' % [config.per_cluster_label, config.per_namespace_label],

    stack::
      g.panel.timeSeries.fieldConfig.defaults.custom.withLineWidth(0)
      + g.panel.timeSeries.fieldConfig.defaults.custom.withFillOpacity(100)
      + g.panel.timeSeries.fieldConfig.defaults.custom.stacking.withMode('normal'),
  },
}
