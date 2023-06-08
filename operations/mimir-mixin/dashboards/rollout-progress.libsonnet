local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-rollout-progress.json';

(import 'dashboard-utils.libsonnet') +
(import 'dashboard-queries.libsonnet') {
  local config = $.queries {
    namespace_matcher: $.namespaceMatcher(),
    per_cluster_label: $._config.per_cluster_label,
    write_job_matcher: if $._config.gateway_enabled then $.jobMatcher($._config.job_names.gateway) else $.jobMatcher($._config.job_names.distributor),
    read_job_matcher: if $._config.gateway_enabled then $.jobMatcher($._config.job_names.gateway) else $.jobMatcher($._config.job_names.query_frontend),
  },

  [filename]:
    ($.dashboard('Rollout progress') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates(false) + {
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
                sum by(cortex_service) (
                  label_replace(
                    label_replace(
                      label_replace(
                        {__name__=~"kube_(deployment|statefulset)_status_replicas_updated", %(namespace_matcher)s},
                        "cortex_service", "$1", "deployment", "(.+)"
                      ),
                      "cortex_service", "$1", "statefulset", "(.+)"
                    ),
                    # Strip the -zone-X suffix, if there is one
                    "cortex_service", "$1", "cortex_service", "(.*?)(?:-zone-[a-z])?"
                  )
                )
                /
                sum by(cortex_service) (
                  label_replace(
                    label_replace(
                      label_replace(
                        {__name__=~"kube_(deployment|statefulset)_status_replicas", %(namespace_matcher)s},
                        "cortex_service", "$1", "deployment", "(.+)"
                      ),
                      "cortex_service", "$1", "statefulset", "(.+)"
                    ),
                    # Strip the -zone-X suffix, if there is one
                    "cortex_service", "$1", "cortex_service", "(.*?)(?:-zone-[a-z])?"
                  )
                )
              ) and (
                sum by(cortex_service) (
                  label_replace(
                    label_replace(
                      label_replace(
                        {__name__=~"kube_(deployment|statefulset)_status_replicas", %(namespace_matcher)s},
                        "cortex_service", "$1", "deployment", "(.+)"
                      ),
                      "cortex_service", "$1", "statefulset", "(.+)"
                    ),
                    # Strip the -zone-X suffix, if there is one
                    "cortex_service", "$1", "cortex_service", "(.*?)(?:-zone-[a-z])?"
                  )
                )
                > 0
              )
            ||| % config,
            |||
              (
                sum by(cortex_service) (
                  label_replace(
                    label_replace(
                      label_replace(
                        {__name__=~"kube_(deployment|statefulset)_status_replicas_ready", %(namespace_matcher)s},
                        "cortex_service", "$1", "deployment", "(.+)"
                      ),
                      "cortex_service", "$1", "statefulset", "(.+)"
                    ),
                    # Strip the -zone-X suffix, if there is one
                    "cortex_service", "$1", "cortex_service", "(.*?)(?:-zone-[a-z])?"
                  )
                )
                /
                sum by(cortex_service) (
                  label_replace(
                    label_replace(
                      label_replace(
                        {__name__=~"kube_(deployment|statefulset)_status_replicas", %(namespace_matcher)s},
                        "cortex_service", "$1", "deployment", "(.+)"
                      ),
                      "cortex_service", "$1", "statefulset", "(.+)"
                    ),
                    # Strip the -zone-X suffix, if there is one
                    "cortex_service", "$1", "cortex_service", "(.*?)(?:-zone-[a-z])?"
                  )
                )
              ) and (
                sum by(cortex_service) (
                  label_replace(
                    label_replace(
                      label_replace(
                        {__name__=~"kube_(deployment|statefulset)_status_replicas", %(namespace_matcher)s},
                        "cortex_service", "$1", "deployment", "(.+)"
                      ),
                      "cortex_service", "$1", "statefulset", "(.+)"
                    ),
                    # Strip the -zone-X suffix, if there is one
                    "cortex_service", "$1", "cortex_service", "(.*?)(?:-zone-[a-z])?"
                  )
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
            tooltip+: {
              mode: 'multi',
            },
          },
          transformations: [
            {
              id: 'joinByField',
              options: { byField: 'cortex_service', mode: 'outer' },
            },
            {
              id: 'organize',
              options: {
                excludeByName: {
                  'Time 1': true,
                  'Time 2': true,
                },
                renameByName: {
                  cortex_service: 'Workload',
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
        $.newStatPanel(|||
          sum(rate(cortex_request_duration_seconds_count{%(write_job_matcher)s, route=~"%(write_http_routes_regex)s",status_code=~"2.+"}[$__rate_interval])) /
          sum(rate(cortex_request_duration_seconds_count{%(write_job_matcher)s, route=~"%(write_http_routes_regex)s"}[$__rate_interval]))
        ||| % config, thresholds=[
          { color: 'green', value: null },
        ]) + {
          id: 2,
          gridPos: { h: 4, w: 2, x: 10, y: 0 },
        },

        $.panel('Writes - 4xx') +
        $.newStatPanel(|||
          sum(rate(cortex_request_duration_seconds_count{%(write_job_matcher)s, route=~"%(write_http_routes_regex)s",status_code=~"4.+"}[$__rate_interval])) /
          sum(rate(cortex_request_duration_seconds_count{%(write_job_matcher)s, route=~"%(write_http_routes_regex)s"}[$__rate_interval]))
        ||| % config, thresholds=[
          { color: 'green', value: null },
          { color: 'orange', value: 0.2 },
          { color: 'red', value: 0.5 },
        ]) + {
          id: 3,
          gridPos: { h: 4, w: 2, x: 12, y: 0 },
        },

        $.panel('Writes - 5xx') +
        $.newStatPanel(|||
          sum(rate(cortex_request_duration_seconds_count{%(write_job_matcher)s, route=~"%(write_http_routes_regex)s",status_code=~"5.+"}[$__rate_interval])) /
          sum(rate(cortex_request_duration_seconds_count{%(write_job_matcher)s, route=~"%(write_http_routes_regex)s"}[$__rate_interval]))
        ||| % config, thresholds=[
          { color: 'green', value: null },
          { color: 'red', value: 0.01 },
        ]) + {
          id: 4,
          gridPos: { h: 4, w: 2, x: 14, y: 0 },
        },

        $.panel('Writes 99th latency') +
        $.newStatPanel(|||
          histogram_quantile(0.99, sum by (le) (%(per_cluster_label)s_job_route:cortex_request_duration_seconds_bucket:sum_rate{%(write_job_matcher)s, route=~"%(write_http_routes_regex)s"}))
        ||| % config, unit='s', thresholds=[
          { color: 'green', value: null },
          { color: 'orange', value: 0.2 },
          { color: 'red', value: 0.5 },
        ]) + {
          id: 5,
          gridPos: { h: 4, w: 8, x: 16, y: 0 },
        },

        //
        // Reads
        //
        $.panel('Reads - 2xx') +
        $.newStatPanel(|||
          sum(rate(cortex_request_duration_seconds_count{%(read_job_matcher)s, route=~"%(read_http_routes_regex)s",status_code=~"2.+"}[$__rate_interval])) /
          sum(rate(cortex_request_duration_seconds_count{%(read_job_matcher)s, route=~"%(read_http_routes_regex)s"}[$__rate_interval]))
        ||| % config, thresholds=[
          { color: 'green', value: null },
        ]) + {
          id: 6,
          gridPos: { h: 4, w: 2, x: 10, y: 4 },
        },

        $.panel('Reads - 4xx') +
        $.newStatPanel(|||
          sum(rate(cortex_request_duration_seconds_count{%(read_job_matcher)s, route=~"%(read_http_routes_regex)s",status_code=~"4.+"}[$__rate_interval])) /
          sum(rate(cortex_request_duration_seconds_count{%(read_job_matcher)s, route=~"%(read_http_routes_regex)s"}[$__rate_interval]))
        ||| % config, thresholds=[
          { color: 'green', value: null },
          { color: 'orange', value: 0.01 },
          { color: 'red', value: 0.05 },
        ]) + {
          id: 7,
          gridPos: { h: 4, w: 2, x: 12, y: 4 },
        },

        $.panel('Reads - 5xx') +
        $.newStatPanel(|||
          sum(rate(cortex_request_duration_seconds_count{%(read_job_matcher)s, route=~"%(read_http_routes_regex)s",status_code=~"5.+"}[$__rate_interval])) /
          sum(rate(cortex_request_duration_seconds_count{%(read_job_matcher)s, route=~"%(read_http_routes_regex)s"}[$__rate_interval]))
        ||| % config, thresholds=[
          { color: 'green', value: null },
          { color: 'red', value: 0.01 },
        ]) + {
          id: 8,
          gridPos: { h: 4, w: 2, x: 14, y: 4 },
        },

        $.panel('Reads 99th latency') +
        $.newStatPanel(|||
          histogram_quantile(0.99, sum by (le) (%(per_cluster_label)s_job_route:cortex_request_duration_seconds_bucket:sum_rate{%(read_job_matcher)s, route=~"%(read_http_routes_regex)s"}))
        ||| % config, unit='s', thresholds=[
          { color: 'green', value: null },
          { color: 'orange', value: 1 },
          { color: 'red', value: 2.5 },
        ]) + {
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
        $.panel('Latency vs 24h ago') +
        $.queryPanel([|||
          1 - (
            avg_over_time(histogram_quantile(0.99, sum by (le) (%(per_cluster_label)s_job_route:cortex_request_duration_seconds_bucket:sum_rate{%(write_job_matcher)s, route=~"%(write_http_routes_regex)s"} offset 24h))[1h:])
            /
            avg_over_time(histogram_quantile(0.99, sum by (le) (%(per_cluster_label)s_job_route:cortex_request_duration_seconds_bucket:sum_rate{%(write_job_matcher)s, route=~"%(write_http_routes_regex)s"}))[1h:])
          )
        ||| % config, |||
          1 - (
            avg_over_time(histogram_quantile(0.99, sum by (le) (%(per_cluster_label)s_job_route:cortex_request_duration_seconds_bucket:sum_rate{%(read_job_matcher)s, route=~"%(read_http_routes_regex)s"} offset 24h))[1h:])
            /
            avg_over_time(histogram_quantile(0.99, sum by (le) (%(per_cluster_label)s_job_route:cortex_request_duration_seconds_bucket:sum_rate{%(read_job_matcher)s, route=~"%(read_http_routes_regex)s"}))[1h:])
          )
        ||| % config], ['writes', 'reads']) + {
          yaxes: $.yaxes({
            format: 'percentunit',
            min: null,  // Can be negative.
          }),

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
