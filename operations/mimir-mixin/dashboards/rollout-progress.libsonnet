local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-rollout-progress.json';

(import 'dashboard-utils.libsonnet') +
(import 'dashboard-queries.libsonnet') {
  local config = $.queries {
    namespace_matcher: $.namespaceMatcher(),
    requests_per_second_metric: if $._config.gateway_enabled then $.queries.gateway.requestsPerSecondMetric else $.queries.distributor.requestsPerSecondMetric,
    write_job_selector: (if $._config.gateway_enabled then $.jobSelector($._config.job_names.gateway) else $.jobSelector($._config.job_names.distributor)) + [utils.selector.re('route', $.queries.write_http_routes_regex)],
    read_job_selector: (if $._config.gateway_enabled then $.jobSelector($._config.job_names.gateway) else $.jobSelector($._config.job_names.query_frontend)) + [utils.selector.re('route', $.queries.read_http_routes_regex)],
    workload_label_replace_open:
      std.repeat('label_replace(', std.length($._config.rollout_dashboard.workload_label_replaces)),
    workload_label_replace_close:
      (if std.length($._config.rollout_dashboard.workload_label_replaces) > 0 then ', ' else '')
      + std.join(', ', [
        '"workload", "%(replacement)s", "%(src_label)s", "%(regex)s")' % replace
        for replace in $._config.rollout_dashboard.workload_label_replaces
      ]),
  },

  [filename]:
    assert std.md5(filename) == '7f0b5567d543a1698e695b530eb7f5de' : 'UID of the dashboard has changed, please update references to dashboard.';
    ($.dashboard('Rollout progress') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates(false)
    .addShowNativeLatencyVariable() + {
      // This dashboard uses the new grid system in order to place panels (using gridPos).
      // Because of this we can't use the mixin's addRow() and addPanel().
      schemaVersion: 27,
      rows: null,
      panels: [
        //
        // Rollout progress
        //
        $.timeseriesPanel('Rollout progress') +
        $.barChart(
          [
            // Multi-zone deployments are grouped together removing the "zone-X" suffix.
            // After the grouping, the resulting label is called "cortex_service".
            |||
              (
                sum by (workload) (
                  %(workload_label_replace_open)s
                    kube_deployment_status_replicas_updated{%(namespace_matcher)s}
                    or
                    kube_statefulset_status_replicas_updated{%(namespace_matcher)s}
                  %(workload_label_replace_close)s
                )
                /
                sum by (workload) (
                  %(workload_label_replace_open)s
                    kube_deployment_status_replicas{%(namespace_matcher)s}
                    or
                    kube_statefulset_status_replicas{%(namespace_matcher)s}
                  %(workload_label_replace_close)s
                )
              ) and (
                sum by (workload) (
                  %(workload_label_replace_open)s
                    kube_deployment_status_replicas{%(namespace_matcher)s}
                    or
                    kube_statefulset_status_replicas{%(namespace_matcher)s}
                  %(workload_label_replace_close)s
                )
                > 0
              )
            ||| % config,
            |||
              (
                sum by (workload) (
                  %(workload_label_replace_open)s
                    kube_deployment_status_replicas_ready{%(namespace_matcher)s}
                    or
                    kube_statefulset_status_replicas_ready{%(namespace_matcher)s}
                  %(workload_label_replace_close)s
                )
                /
                sum by (workload) (
                  %(workload_label_replace_open)s
                    kube_deployment_status_replicas{%(namespace_matcher)s}
                    or
                    kube_statefulset_status_replicas{%(namespace_matcher)s}
                  %(workload_label_replace_close)s
                )
              ) and (
                sum by (workload) (
                  %(workload_label_replace_open)s
                    kube_deployment_status_replicas{%(namespace_matcher)s}
                    or
                    kube_statefulset_status_replicas{%(namespace_matcher)s}
                  %(workload_label_replace_close)s
                )
                > 0
              )
            ||| % config,
          ],
          legends=[
            '__auto',
            '__auto',
          ],
          thresholds=[],
          unit='percentunit',
          min=0,
          max=1
        ) +
        $.showAllTooltip +
        {
          id: 1,
          gridPos: { h: 13, w: 10, x: 0, y: 0 },
          fieldConfig+: {
            overrides: [
              {
                matcher: { id: 'byName', options: 'Ready' },
                properties: [
                  { id: 'color', value: { mode: 'fixed', fixedColor: 'green' } },
                ],
              },
              {
                matcher: { id: 'byName', options: 'Updated' },
                properties: [
                  { id: 'color', value: { mode: 'fixed', fixedColor: 'blue' } },
                ],
              },
            ],
          },
          options+: {
            xField: 'Workload',
            orientation: 'horizontal',
          },
          transformations: [
            {
              id: 'joinByField',
              options: { byField: 'workload', mode: 'outer' },
            },
            {
              id: 'organize',
              options: {
                excludeByName: {
                  'Time 1': true,
                  'Time 2': true,
                },
                renameByName: {
                  workload: 'Workload',
                  'Value #A': 'Updated',
                  'Value #B': 'Ready',
                },
              },
            },
            {
              id: 'sortBy',
              options: { sort: [{ field: 'Workload' }] },
            },
          ],
          targets: [
            t {
              format: 'table',
              instant: true,
            }
            for t in super.targets
          ],
        },

        //
        // Writes
        //
        $.panel('Writes - 2xx') +
        $.ncSumCountRateStatPanel(
          metric=config.requests_per_second_metric,
          selectors=config.write_job_selector,
          extra_selector=[utils.selector.re('status_code', '2.+')],
          thresholds=[{ color: 'green', value: null }],
        ) + {
          id: 2,
          gridPos: { h: 4, w: 2, x: 10, y: 0 },
        },

        $.panel('Writes - 4xx') +
        $.ncSumCountRateStatPanel(
          metric=config.requests_per_second_metric,
          selectors=config.write_job_selector,
          extra_selector=[utils.selector.re('status_code', '4.+')],
          thresholds=[
            { color: 'green', value: null },
            { color: 'orange', value: 0.2 },
            { color: 'red', value: 0.5 },
          ]
        ) + {
          id: 3,
          gridPos: { h: 4, w: 2, x: 12, y: 0 },
        },

        $.panel('Writes - 5xx') +
        $.ncSumCountRateStatPanel(
          metric=config.requests_per_second_metric,
          selectors=config.write_job_selector,
          extra_selector=[utils.selector.re('status_code', '5.+')],
          thresholds=[
            { color: 'green', value: null },
            { color: 'red', value: 0.01 },
          ]
        ) + {
          id: 4,
          gridPos: { h: 4, w: 2, x: 14, y: 0 },
        },

        $.panel('Writes 99th latency') +
        $.ncLatencyStatPanel(
          quantile='0.99',
          metric=config.requests_per_second_metric,
          selectors=config.write_job_selector,
          thresholds=[
            { color: 'green', value: null },
            { color: 'orange', value: 0.2 },
            { color: 'red', value: 0.5 },
          ]
        ) + {
          id: 5,
          gridPos: { h: 4, w: 8, x: 16, y: 0 },
        },

        //
        // Reads
        //
        $.panel('Reads - 2xx') +
        $.ncSumCountRateStatPanel(
          metric=config.requests_per_second_metric,
          selectors=config.read_job_selector,
          extra_selector=[utils.selector.re('status_code', '2.+')],
          thresholds=[{ color: 'green', value: null }],
        ) + {
          id: 6,
          gridPos: { h: 4, w: 2, x: 10, y: 4 },
        },

        $.panel('Reads - 4xx') +
        $.ncSumCountRateStatPanel(
          metric=config.requests_per_second_metric,
          selectors=config.read_job_selector,
          extra_selector=[utils.selector.re('status_code', '4.+')],
          thresholds=[
            { color: 'green', value: null },
            { color: 'orange', value: 0.01 },
            { color: 'red', value: 0.05 },
          ]
        ) + {
          id: 7,
          gridPos: { h: 4, w: 2, x: 12, y: 4 },
        },

        $.panel('Reads - 5xx') +
        $.ncSumCountRateStatPanel(
          metric=config.requests_per_second_metric,
          selectors=config.read_job_selector,
          extra_selector=[utils.selector.re('status_code', '5.+')],
          thresholds=[
            { color: 'green', value: null },
            { color: 'red', value: 0.01 },
          ]
        ) + {
          id: 8,
          gridPos: { h: 4, w: 2, x: 14, y: 4 },
        },

        $.panel('Reads 99th latency') +
        $.ncLatencyStatPanel(
          quantile='0.99',
          metric=config.requests_per_second_metric,
          selectors=config.read_job_selector,
          thresholds=[
            { color: 'green', value: null },
            { color: 'orange', value: 1 },
            { color: 'red', value: 2.5 },
          ]
        ) + {
          id: 9,
          gridPos: { h: 4, w: 8, x: 16, y: 4 },
        },

        //
        // Unhealthy pods
        //
        $.panel('Unhealthy pods') +
        $.newStatPanel([
          |||
            kube_deployment_status_replicas_unavailable{%(namespace_matcher)s}
            > 0
          ||| % config,
          |||
            kube_statefulset_status_replicas_current{%(namespace_matcher)s} -
            kube_statefulset_status_replicas_ready {%(namespace_matcher)s}
            > 0
          ||| % config,
        ], legends=[
          '{{deployment}}',
          '{{statefulset}}',
        ], thresholds=[
          { color: 'green', value: null },
          { color: 'orange', value: 1 },
          { color: 'red', value: 2 },
        ], instant=true, novalue='All healthy', unit='short', decimals=0) + {
          options: {
            text: {
              // Small font size since we may have many entries during a rollout.
              titleSize: 14,
              valueSize: 14,
            },
            textMode: 'value_and_name',
          },
          id: 10,
          gridPos: { h: 3, w: 10, x: 0, y: 13 },
        },

        //
        // Versions
        //
        {
          title: 'Pods count per version',
          type: 'table',
          datasource: '$datasource',

          targets: [
            {
              expr: |||
                count by(container, version) (
                  label_replace(
                    kube_pod_container_info{%(namespace_matcher)s},
                    "version", "$1", "image", ".*:(.*)"
                  )
                )
              ||| % config,
              instant: true,
              legendFormat: '',
              refId: 'A',
            },
          ],

          fieldConfig: {
            overrides: [
              {
                // Center align the version.
                matcher: { id: 'byRegexp', options: 'r.*' },
                properties: [{ id: 'custom.align', value: 'center' }],
              },
            ],
          },

          transformations: [
            {
              // Transform the version label to a field.
              id: 'labelsToFields',
              options: { valueLabel: 'version' },
            },
            {
              // Hide time and put the container column first.
              id: 'organize',
              options: { excludeByName: { Time: true }, indexByName: { Time: 0, container: 1 } },
            },
            {
              // Sort by container.
              id: 'sortBy',
              options: { fields: {}, sort: [{ field: 'container' }] },
            },
          ],

          id: 11,
          gridPos: { h: 8, w: 6, x: 10, y: 8 },
        },

        //
        // Performance comparison with 24h ago
        //
        $.timeseriesPanel('Latency vs 24h ago') +
        $.queryPanel(
          local write = $.ncAvgHistogramQuantile(
            quantile='0.99',
            metric=config.requests_per_second_metric,
            selectors=config.write_job_selector,
            offset='24h',
            rate_interval='1h:'
          );
          local read = $.ncAvgHistogramQuantile(
            quantile='0.99',
            metric=config.requests_per_second_metric,
            selectors=config.read_job_selector,
            offset='24h',
            rate_interval='1h:'
          );
          [
            utils.showClassicHistogramQuery(write),
            utils.showNativeHistogramQuery(write),
            utils.showClassicHistogramQuery(read),
            utils.showNativeHistogramQuery(read),
          ],
          ['writes', 'writes', 'reads', 'reads']
        ) +
        {
          fieldConfig: {
            defaults: {
              unit: 'percentunit',
              custom: {
                fillOpacity: 10,
              },
            },
          },
        } +
        {
          id: 12,
          gridPos: { h: 8, w: 8, x: 16, y: 8 },
        },
      ],

      templating+: {
        list: [
          // Do not allow to include all namespaces cause this dashboard is designed to show
          // 1 cluster at a time.
          l + (if (l.name == 'namespace') then { includeAll: false } else {})
          for l in super.list
        ],
      },
    },
}
