local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-slow-queries.json';

(import 'dashboard-utils.libsonnet') {
  [filename]:
    ($.dashboard('Slow queries') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates(false)
    .addRow(
      $.row('Accross tenants')
      .addPanel(
        $.panel('Response time') +
        $.lokiMetricsQueryPanel(
          [
            'quantile_over_time(0.99, {%s=~"$cluster",%s=~"$namespace",name=~"query-frontend.*"} |= "query stats" != "/api/v1/read" | logfmt | user=~"${tenant_id}" | user_agent=~"${user_agent}" | response_time > ${min_duration} | unwrap duration_seconds(response_time) [$__auto]) by ()' % [$._config.per_cluster_label, $._config.per_namespace_label],
            'quantile_over_time(0.5, {%s=~"$cluster",%s=~"$namespace",name=~"query-frontend.*"} |= "query stats" != "/api/v1/read" | logfmt | user=~"${tenant_id}" | user_agent=~"${user_agent}" | response_time > ${min_duration} | unwrap duration_seconds(response_time) [$__auto]) by ()' % [$._config.per_cluster_label, $._config.per_namespace_label],
          ],
          ['p99', 'p50'],
          unit='s',
        )
      )
      .addPanel(
        $.panel('Fetched series') +
        $.lokiMetricsQueryPanel(
          [
            'quantile_over_time(0.99, {%s=~"$cluster",%s=~"$namespace",name=~"query-frontend.*"} |= "query stats" != "/api/v1/read" | logfmt | user=~"${tenant_id}" | user_agent=~"${user_agent}" | response_time > ${min_duration} | unwrap fetched_series_count[$__auto]) by ()' % [$._config.per_cluster_label, $._config.per_namespace_label],
            'quantile_over_time(0.5, {%s=~"$cluster",%s=~"$namespace",name=~"query-frontend.*"} |= "query stats" != "/api/v1/read" | logfmt | user=~"${tenant_id}" | user_agent=~"${user_agent}" | response_time > ${min_duration} | unwrap fetched_series_count[$__auto]) by ()' % [$._config.per_cluster_label, $._config.per_namespace_label],
          ],
          ['p99', 'p50'],
        )
      )
      .addPanel(
        $.panel('Fetched chunks') +
        $.lokiMetricsQueryPanel(
          [
            'quantile_over_time(0.99, {%s=~"$cluster",%s=~"$namespace",name=~"query-frontend.*"} |= "query stats" != "/api/v1/read" | logfmt | user=~"${tenant_id}" | user_agent=~"${user_agent}" | response_time > ${min_duration} | unwrap fetched_chunk_bytes[$__auto]) by ()' % [$._config.per_cluster_label, $._config.per_namespace_label],
            'quantile_over_time(0.5, {%s=~"$cluster",%s=~"$namespace",name=~"query-frontend.*"} |= "query stats" != "/api/v1/read" | logfmt | user=~"${tenant_id}" | user_agent=~"${user_agent}" | response_time > ${min_duration} | unwrap fetched_chunk_bytes[$__auto]) by ()' % [$._config.per_cluster_label, $._config.per_namespace_label],
          ],
          ['p99', 'p50'],
          unit='bytes',
        )
      )
      .addPanel(
        $.panel('Response size') +
        $.lokiMetricsQueryPanel(
          [
            'quantile_over_time(0.99, {%s=~"$cluster",%s=~"$namespace",name=~"query-frontend.*"} |= "query stats" != "/api/v1/read" | logfmt | user=~"${tenant_id}" | user_agent=~"${user_agent}" | response_time > ${min_duration} | unwrap response_size_bytes[$__auto]) by ()' % [$._config.per_cluster_label, $._config.per_namespace_label],
            'quantile_over_time(0.5, {%s=~"$cluster",%s=~"$namespace",name=~"query-frontend.*"} |= "query stats" != "/api/v1/read" | logfmt | user=~"${tenant_id}" | user_agent=~"${user_agent}" | response_time > ${min_duration} | unwrap response_size_bytes[$__auto]) by ()' % [$._config.per_cluster_label, $._config.per_namespace_label],
          ],
          ['p99', 'p50'],
          unit='bytes',
        )
      )
      .addPanel(
        $.panel('Time span') +
        $.lokiMetricsQueryPanel(
          [
            'quantile_over_time(0.99, {%s=~"$cluster",%s=~"$namespace",name=~"query-frontend.*"} |= "query stats" != "/api/v1/read" | logfmt | user=~"${tenant_id}" | user_agent=~"${user_agent}" | response_time > ${min_duration} | unwrap duration_seconds(length) [$__auto]) by ()' % [$._config.per_cluster_label, $._config.per_namespace_label],
            'quantile_over_time(0.5, {%s=~"$cluster",%s=~"$namespace",name=~"query-frontend.*"} |= "query stats" != "/api/v1/read" | logfmt | user=~"${tenant_id}" | user_agent=~"${user_agent}" | response_time > ${min_duration} | unwrap duration_seconds(length) [$__auto]) by ()' % [$._config.per_cluster_label, $._config.per_namespace_label],
          ],
          ['p99', 'p50'],
          unit='s',
        )
      )
    )
    .addRow(
      $.row('Top 10 tenants') { collapse: true }
      .addPanel(
        $.panel('P99 response time') +
        $.lokiMetricsQueryPanel(
          'topk(10, quantile_over_time(0.99, {%s=~"$cluster",%s=~"$namespace",name=~"query-frontend.*"} |= "query stats" != "/api/v1/read" | logfmt | user=~"${tenant_id}" | user_agent=~"${user_agent}" | response_time > ${min_duration} | unwrap duration_seconds(response_time) [$__auto]) by (user))' % [$._config.per_cluster_label, $._config.per_namespace_label],
          '{{user}}',
          unit='s',
        )
      )
      .addPanel(
        $.panel('P99 fetched series') +
        $.lokiMetricsQueryPanel(
          'topk(10, quantile_over_time(0.99, {%s=~"$cluster",%s=~"$namespace",name=~"query-frontend.*"} |= "query stats" != "/api/v1/read" | logfmt | user=~"${tenant_id}" | user_agent=~"${user_agent}" | response_time > ${min_duration} | unwrap fetched_series_count[$__auto]) by (user))' % [$._config.per_cluster_label, $._config.per_namespace_label],
          '{{user}}',
        )
      )
      .addPanel(
        $.panel('P99 fetched chunks') +
        $.lokiMetricsQueryPanel(
          'topk(10, quantile_over_time(0.99, {%s=~"$cluster",%s=~"$namespace",name=~"query-frontend.*"} |= "query stats" != "/api/v1/read" | logfmt | user=~"${tenant_id}" | user_agent=~"${user_agent}" | response_time > ${min_duration} | unwrap fetched_chunk_bytes[$__auto]) by (user))' % [$._config.per_cluster_label, $._config.per_namespace_label],
          '{{user}}',
          unit='bytes',
        )
      )
      .addPanel(
        $.panel('P99 response size') +
        $.lokiMetricsQueryPanel(
          'topk(10, quantile_over_time(0.99, {%s=~"$cluster",%s=~"$namespace",name=~"query-frontend.*"} |= "query stats" != "/api/v1/read" | logfmt | user=~"${tenant_id}" | user_agent=~"${user_agent}" | response_time > ${min_duration} | unwrap response_size_bytes[$__auto]) by (user))' % [$._config.per_cluster_label, $._config.per_namespace_label],
          '{{user}}',
          unit='bytes',
        )
      )
      .addPanel(
        $.panel('P99 time span') +
        $.lokiMetricsQueryPanel(
          'topk(10, quantile_over_time(0.99, {%s=~"$cluster",%s=~"$namespace",name=~"query-frontend.*"} |= "query stats" != "/api/v1/read" | logfmt | user=~"${tenant_id}" | user_agent=~"${user_agent}" | response_time > ${min_duration} | unwrap duration_seconds(length) [$__auto]) by (user))' % [$._config.per_cluster_label, $._config.per_namespace_label],
          '{{user}}',
          unit='s',
        )
      )
    )
    .addRow(
      (
        $.row('Top 10 User-Agents') { collapse: true }
        .addPanel(
          $.panel('P99 response time') +
          $.lokiMetricsQueryPanel(
            'topk(10, quantile_over_time(0.99, {%s=~"$cluster",%s=~"$namespace",name=~"query-frontend.*"} |= "query stats" != "/api/v1/read" | logfmt | user=~"${tenant_id}" | user_agent=~"${user_agent}" | response_time > ${min_duration} | unwrap duration_seconds(response_time) [$__auto]) by (user_agent))' % [$._config.per_cluster_label, $._config.per_namespace_label],
            '{{user_agent}}',
            unit='s',
          )
        )
        .addPanel(
          $.panel('P99 fetched series') +
          $.lokiMetricsQueryPanel(
            'topk(10, quantile_over_time(0.99, {%s=~"$cluster",%s=~"$namespace",name=~"query-frontend.*"} |= "query stats" != "/api/v1/read" | logfmt | user=~"${tenant_id}" | user_agent=~"${user_agent}" | response_time > ${min_duration} | unwrap fetched_series_count[$__auto]) by (user_agent))' % [$._config.per_cluster_label, $._config.per_namespace_label],
            '{{user_agent}}',
          )
        )
        .addPanel(
          $.panel('P99 fetched chunks') +
          $.lokiMetricsQueryPanel(
            'topk(10, quantile_over_time(0.99, {%s=~"$cluster",%s=~"$namespace",name=~"query-frontend.*"} |= "query stats" != "/api/v1/read" | logfmt | user=~"${tenant_id}" | user_agent=~"${user_agent}" | response_time > ${min_duration} | unwrap fetched_chunk_bytes[$__auto]) by (user_agent))' % [$._config.per_cluster_label, $._config.per_namespace_label],
            '{{user_agent}}',
            unit='bytes',
          )
        )
        .addPanel(
          $.panel('P99 response size') +
          $.lokiMetricsQueryPanel(
            'topk(10, quantile_over_time(0.99, {%s=~"$cluster",%s=~"$namespace",name=~"query-frontend.*"} |= "query stats" != "/api/v1/read" | logfmt | user=~"${tenant_id}" | user_agent=~"${user_agent}" | response_time > ${min_duration} | unwrap response_size_bytes[$__auto]) by (user_agent))' % [$._config.per_cluster_label, $._config.per_namespace_label],
            '{{user_agent}}',
            unit='bytes',
          )
        )
        .addPanel(
          $.panel('P99 time span') +
          $.lokiMetricsQueryPanel(
            'topk(10, quantile_over_time(0.99, {%s=~"$cluster",%s=~"$namespace",name=~"query-frontend.*"} |= "query stats" != "/api/v1/read" | logfmt | user=~"${tenant_id}" | user_agent=~"${user_agent}" | response_time > ${min_duration} | unwrap duration_seconds(length) [$__auto]) by (user_agent))' % [$._config.per_cluster_label, $._config.per_namespace_label],
            '{{user_agent}}',
            unit='s',
          )
        )
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        {
          height: '500px',
          title: 'Slow queries',
          type: 'table',
          datasource: '${lokidatasource}',

          // Query logs from Loki.
          targets: [
            {
              // Filter out the remote read endpoint.
              expr: '{%s=~"$cluster",%s=~"$namespace",name=~"query-frontend.*"} |= "query stats" != "/api/v1/read" | logfmt | user=~"${tenant_id}" | user_agent=~"${user_agent}" | response_time > ${min_duration}' % [$._config.per_cluster_label, $._config.per_namespace_label],
              instant: false,
              legendFormat: '',
              range: true,
              refId: 'A',
            },
          ],

          // Use Grafana transformations to display fields in a table.
          transformations: [
            {
              // Convert labels to fields.
              id: 'extractFields',
              options: {
                source: 'labels',
              },
            },
            {
              id: 'organize',
              options: {
                // Hide fields we don't care.
                local hiddenFields = ['caller', 'cluster', 'container', 'host', 'id', 'job', 'level', 'line', 'method', 'msg', 'name', 'namespace', 'path', 'pod', 'pod_template_hash', 'query_wall_time_seconds', 'stream', 'traceID', 'tsNs', 'labels', 'Line', 'Time'],

                excludeByName: {
                  [field]: true
                  for field in hiddenFields
                },

                // Order fields.
                local orderedFields = ['ts', 'user', 'length', 'param_start', 'param_end', 'param_time', 'param_step', 'param_query', 'response_time'],

                indexByName: {
                  [orderedFields[i]]: i
                  for i in std.range(0, std.length(orderedFields) - 1)
                },

                // Rename fields.
                renameByName: {
                  org_id: 'Tenant ID',
                  param_query: 'Query',
                  param_step: 'Step',
                  response_time: 'Duration',
                },
              },
            },
          ],

          fieldConfig: {
            // Configure overrides to nicely format field values.
            overrides: [
              {
                matcher: { id: 'byName', options: 'Time range' },
                properties: [
                  {
                    id: 'mappings',
                    value: [
                      {
                        from: '',
                        id: 1,
                        text: 'Instant query',
                        to: '',
                        type: 1,
                        value: '0',
                      },
                    ],
                  },
                  { id: 'unit', value: 's' },
                ],
              },
              {
                matcher: { id: 'byName', options: 'Step' },
                properties: [{ id: 'unit', value: 's' }],
              },
            ],
          },
        },
      )
    )
    + {
      templating+: {
        list+: [
          // Add the Loki datasource.
          {
            type: 'datasource',
            name: 'lokidatasource',
            label: 'Logs datasource',
            query: 'loki',
            hide: 0,
            includeAll: false,
            multi: false,
          },
          // Add a variable to configure the min duration.
          {
            local defaultValue = '5s',

            type: 'textbox',
            name: 'min_duration',
            label: 'Min duration',
            hide: 0,
            options: [
              {
                selected: true,
                text: defaultValue,
                value: defaultValue,
              },
            ],
            current: {
              // Default value.
              selected: true,
              text: defaultValue,
              value: defaultValue,
            },
            query: defaultValue,
          },
          // Add a variable to configure the tenant to filter on.
          {
            local defaultValue = '.*',

            type: 'textbox',
            name: 'tenant_id',
            label: 'Tenant ID',
            hide: 0,
            options: [
              {
                selected: true,
                text: defaultValue,
                value: defaultValue,
              },
            ],
            current: {
              // Default value.
              selected: true,
              text: defaultValue,
              value: defaultValue,
            },
            query: defaultValue,
          },
          // Add a variable to configure the tenant to filter on.
          {
            local defaultValue = '.*',

            type: 'textbox',
            name: 'user_agent',
            label: 'User-Agent HTTP Header',
            hide: 0,
            options: [
              {
                selected: true,
                text: defaultValue,
                value: defaultValue,
              },
            ],
            current: {
              // Default value.
              selected: true,
              text: defaultValue,
              value: defaultValue,
            },
            query: defaultValue,
          },
        ],
      },
    } + {
      templating+: {
        list: [
          // Do not allow to include all namespaces otherwise this dashboard
          // risks to explode because it shows resources per pod.
          l + (if (l.name == 'namespace') then { includeAll: false } else {})
          for l in super.list
        ],
      },
    } + {
      // No auto-refresh by default.
      refresh: '',
    },
}
