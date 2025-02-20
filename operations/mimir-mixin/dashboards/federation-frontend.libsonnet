local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'federation-frontend.json';

(import 'dashboard-utils.libsonnet') {
  [filename]:
    ($.dashboard('Federation-frontend') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates()
    .addTemplate(
      'remote_cluster',
      'cortex_federation_frontend_cluster_remote_latency_seconds',
      'remote_cluster',
      hide=2,
      includeAll=true,
    )
    .addRow(
      $.row('Overview')
      .addPanel(
        $.panel('Requests / sec') +
        $.qpsPanel('cortex_request_duration_seconds_count{%s, route=~"%s"}' % [$.jobMatcher($._config.job_names.federation_frontend), $.queries.read_http_routes_regex]) +
        $.panelDescription(
          'Requests / sec',
          |||
            The number of requests per second to the federation-frontend.
            This includes all read requests: instant & range queries, series, label values, label names, etc.
          |||
        )
      )
      .addPanel(
        $.panel('Latency') +
        // This uses a latency recording rule for "cortex_request_duration_seconds"
        utils.latencyRecordingRulePanel(
          'cortex_request_duration_seconds',
          $.jobSelector($._config.job_names.federation_frontend) +
          [utils.selector.re('route', $.queries.read_http_routes_regex)]
        )
      )
      .addPanel(
        $.timeseriesPanel('Per %s p99 latency' % $._config.per_instance_label) +
        $.hiddenLegendQueryPanel(
          'histogram_quantile(0.99, sum by(le, %s) (rate(cortex_request_duration_seconds_bucket{%s, route=~"%s"}[$__rate_interval])))' % [
            $._config.per_instance_label,
            $.jobMatcher($._config.job_names.federation_frontend),
            $.queries.read_http_routes_regex,
          ],
          ''
        )
      )
    )
    .addRow(
      $.row('Resource usage')
      .addPanel(
        $.containerCPUUsagePanelByComponent('federation_frontend'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanelByComponent('federation_frontend'),
      )
      .addPanel(
        $.containerGoHeapInUsePanelByComponent('federation_frontend'),
      )
    )
    .addRow(
      $.row('Endpoints')
      .addPanel(
        $.timeseriesPanel('Requests by Endpoint') +
        // This panel graphs the cortex_request_duration_seconds_count metric by `route`
        $.queryPanel(
          'sum by (route) (rate(cortex_request_duration_seconds_count{%s, route=~"%s"}[$__rate_interval]))' %
          [$.jobMatcher($._config.job_names.federation_frontend), $.queries.read_http_routes_regex],
          '{{route}}'
        ) { fieldConfig+: { defaults+: { unit: 'req/s' } } }
      )
      .addPanel(
        $.timeseriesPanel('P99 latency by route') +
        // This panel computes p99 latency per route using histogram_quantile with grouping by `route`
        $.queryPanel(
          'histogram_quantile(0.99, sum by(le, route) (rate(cortex_request_duration_seconds_bucket{%s, route=~"%s"}[$__rate_interval])))' %
          [$.jobMatcher($._config.job_names.federation_frontend), $.queries.read_http_routes_regex],
          '{{route}}'
        )
      )
    )
    .addRow(
      $.row('Queries')
      .addPanel(
        $.timeseriesPanel('Sharded queries ratio') +
        $.hiddenLegendQueryPanel(
          |||
            sum(rate(cortex_federation_frontend_cluster_sharding_rewrites_succeeded_total{%s}[$__rate_interval])) /
            sum(rate(cortex_federation_frontend_cluster_sharding_rewrites_attempted_total{%s}[$__rate_interval]))
          ||| % [$.jobMatcher($._config.job_names.federation_frontend), $.jobMatcher($._config.job_names.federation_frontend)],
          ' ',
        )
        { fieldConfig+: { defaults+: { unit: 'percentunit', min: 0, max: 1 } } } +
        $.panelDescription(
          'Sharded queries ratio',
          |||
            The % of queries that have been successfully rewritten and executed in a shardable way.
            Sharded queries are delegated to remote clusters.
            This panel only takes into account the type of queries that are supported by query sharding.
            Remaining queries fall back to the remote read implementation where all the query evaluation happens in the federation-frontend.
          |||
        )
      )
      .addPanel(
        $.panel('Number of sharded queries per query') +
        $.latencyPanel('cortex_federation_frontend_cluster_sharded_queries_per_query', '{%s}' % $.jobMatcher($._config.job_names.federation_frontend), multiplier=1) +
        { yaxes: $.yaxes('short') } +
        $.panelDescription(
          'Number of sharded queries per shardable query',
          |||
            The number of sharded queries that have been executed for a single input query.
            It only tracks queries that have been successfully rewritten in a shardable way.
            If a query is shardable, then there is at least one sharded query per remote cluster.
            If the query is more complex, then there may be more queries per remote cluster.
            For example, `sum(foo) + sum(bar)` will result in two queries per remote cluster.
          |||
        )
      )
      .addPanel(
        $.timeseriesPanel('Complete response ratio quantiles') +
        $.queryPanel(
          '1 - avg by (quantile) (cortex_federation_frontend_partial_results{%s})' %
          [$.jobMatcher($._config.job_names.federation_frontend)],
          '{{quantile}}'
        ) +
        {
          fieldConfig+: {
            defaults+: { unit: 'percentunit', min: 0, max: 1 },
            local overrideName(legend, display_name) = {
              matcher: { id: 'byName', options: legend },
              properties: [{ id: 'displayName', value: display_name }],
            },
            overrides: [
              overrideName('0.5', '50th Percentile'),
              overrideName('0.75', '75th Percentile'),
              overrideName('0.9', '90th Percentile'),
              overrideName('0.99', '99th Percentile'),
            ],
          },
        } +
        $.panelDescription(
          'Complete response ratio quantiles',
          |||
            Fraction of successfully queried remote clusters out of all attempted clusters for each query or request.

            For example, if a query is sent to 3 remote clusters, and 2 are successfully queried, then the complete response ratio is 66.(6)%.

            If the 99th Percentile is 66.(6)%, then 99% of the queries answered by the federation-frontend see more than 66.(6)% of the data they requested and 1% see less.
          |||
        )
      )
    )
    .addRow(
      $.row('Request rate and latency to "$remote_cluster"') {
        repeat: 'remote_cluster',
        repeatDirection: 'h',
      }
      .addPanel(
        $.panel('Remote requests / sec by status') +
        $.qpsPanel('cortex_federation_frontend_cluster_remote_latency_seconds_count{remote_cluster=~"$remote_cluster", %s}' % $.jobMatcher($._config.job_names.federation_frontend)) +
        {
          yaxes: $.yaxes('ops'),
          aliasColors: {
            client_error: 'orange',
            server_error: 'red',
            success: 'green',
          },
        } +
        $.panelDescription(
          'Remote requests / sec by status',
          |||
            Rate of remote requests to $remote_cluster segmented by status.
            This includes all requests sent to the remote clusters.
          |||
        )
      )
      .addPanel(
        $.panel('Remote requests / sec by request type') +
        $.queryPanel(
          'sum by (request_type) (rate(cortex_federation_frontend_cluster_remote_latency_seconds_count{remote_cluster=~"$remote_cluster", %s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.federation_frontend),
          ['{{request_type}}'],
        ) +
        $.stack +
        { yaxes: $.yaxes('ops') } +
        $.panelDescription(
          'Remote requests / sec by request type',
          |||
            Rate of remote requests to $remote_cluster segmented by request type.
          |||
        )
      )
      .addPanel(
        $.panel('Remote latency') +
        $.queryPanel(
          [
            'histogram_quantile(0.99, sum by(le, remote_cluster) (rate(cortex_federation_frontend_cluster_remote_latency_seconds_bucket{remote_cluster=~"$remote_cluster", %s}[$__rate_interval])))' % $.jobMatcher($._config.job_names.federation_frontend),
            'histogram_quantile(0.50, sum by(le, remote_cluster) (rate(cortex_federation_frontend_cluster_remote_latency_seconds_bucket{remote_cluster=~"$remote_cluster", %s}[$__rate_interval])))' % $.jobMatcher($._config.job_names.federation_frontend),
            |||
              sum(rate(cortex_federation_frontend_cluster_remote_latency_seconds_sum{remote_cluster=~"$remote_cluster", %s}[$__rate_interval]))
              /
              sum(rate(cortex_federation_frontend_cluster_remote_latency_seconds_count{remote_cluster=~"$remote_cluster", %s}[$__rate_interval]))
            ||| % [
              $.jobMatcher($._config.job_names.federation_frontend),
              $.jobMatcher($._config.job_names.federation_frontend),
            ],
          ],
          ['99th Percentile', '50th Percentile', 'Average']
        ) +
        $.panelDescription(
          'Cluster response latency',
          |||
            Displays remote latency at different quantiles.
            This includes all requests sent to the remote clusters.
          |||
        )
      )
    ),
}
