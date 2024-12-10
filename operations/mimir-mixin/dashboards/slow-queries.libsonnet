local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-slow-queries.json';

(import 'dashboard-utils.libsonnet') {
  [filename]:
    assert std.md5(filename) == '6089e1ce1e678788f46312a0a1e647e6' : 'UID of the dashboard has changed, please update references to dashboard.';
    ($.dashboard('Slow queries') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates(false)
    .addRow(
      $.row('Across tenants')
      .addPanel(
        $.timeseriesPanel('Response time') +
        $.lokiMetricsQueryPanel(
          [
            'quantile_over_time(0.99, {%s=~"$cluster",%s=~"$namespace",%s=~"$component.*"} |= "query stats" != "/api/v1/read" | logfmt | user=~"${tenant_id}" | user_agent=~"${user_agent}" | response_time > ${min_duration} | unwrap duration_seconds(response_time) [$__auto]) by ()' % [$._config.per_cluster_label, $._config.per_namespace_label, $._config.per_component_loki_label],
            'quantile_over_time(0.5, {%s=~"$cluster",%s=~"$namespace",%s=~"$component.*"} |= "query stats" != "/api/v1/read" | logfmt | user=~"${tenant_id}" | user_agent=~"${user_agent}" | response_time > ${min_duration} | unwrap duration_seconds(response_time) [$__auto]) by ()' % [$._config.per_cluster_label, $._config.per_namespace_label, $._config.per_component_loki_label],
          ],
          ['p99', 'p50'],
          unit='s',
        )
      )
      .addPanel(
        $.timeseriesPanel('Fetched series') +
        $.lokiMetricsQueryPanel(
          [
            'quantile_over_time(0.99, {%s=~"$cluster",%s=~"$namespace",%s=~"$component.*"} |= "query stats" != "/api/v1/read" | logfmt | user=~"${tenant_id}" | user_agent=~"${user_agent}" | response_time > ${min_duration} | unwrap fetched_series_count[$__auto]) by ()' % [$._config.per_cluster_label, $._config.per_namespace_label, $._config.per_component_loki_label],
            'quantile_over_time(0.5, {%s=~"$cluster",%s=~"$namespace",%s=~"$component.*"} |= "query stats" != "/api/v1/read" | logfmt | user=~"${tenant_id}" | user_agent=~"${user_agent}" | response_time > ${min_duration} | unwrap fetched_series_count[$__auto]) by ()' % [$._config.per_cluster_label, $._config.per_namespace_label, $._config.per_component_loki_label],
          ],
          ['p99', 'p50'],
        )
      )
      .addPanel(
        $.timeseriesPanel('Fetched chunks') +
        $.lokiMetricsQueryPanel(
          [
            'quantile_over_time(0.99, {%s=~"$cluster",%s=~"$namespace",%s=~"$component.*"} |= "query stats" != "/api/v1/read" | logfmt | user=~"${tenant_id}" | user_agent=~"${user_agent}" | response_time > ${min_duration} | unwrap fetched_chunk_bytes[$__auto]) by ()' % [$._config.per_cluster_label, $._config.per_namespace_label, $._config.per_component_loki_label],
            'quantile_over_time(0.5, {%s=~"$cluster",%s=~"$namespace",%s=~"$component.*"} |= "query stats" != "/api/v1/read" | logfmt | user=~"${tenant_id}" | user_agent=~"${user_agent}" | response_time > ${min_duration} | unwrap fetched_chunk_bytes[$__auto]) by ()' % [$._config.per_cluster_label, $._config.per_namespace_label, $._config.per_component_loki_label],
          ],
          ['p99', 'p50'],
          unit='bytes',
        )
      )
      .addPanel(
        $.timeseriesPanel('Response size') +
        $.lokiMetricsQueryPanel(
          [
            'quantile_over_time(0.99, {%s=~"$cluster",%s=~"$namespace",%s=~"$component.*"} |= "query stats" != "/api/v1/read" | logfmt | user=~"${tenant_id}" | user_agent=~"${user_agent}" | response_time > ${min_duration} | unwrap response_size_bytes[$__auto]) by ()' % [$._config.per_cluster_label, $._config.per_namespace_label, $._config.per_component_loki_label],
            'quantile_over_time(0.5, {%s=~"$cluster",%s=~"$namespace",%s=~"$component.*"} |= "query stats" != "/api/v1/read" | logfmt | user=~"${tenant_id}" | user_agent=~"${user_agent}" | response_time > ${min_duration} | unwrap response_size_bytes[$__auto]) by ()' % [$._config.per_cluster_label, $._config.per_namespace_label, $._config.per_component_loki_label],
          ],
          ['p99', 'p50'],
          unit='bytes',
        )
      )
      .addPanel(
        $.timeseriesPanel('Time span') +
        $.lokiMetricsQueryPanel(
          [
            'quantile_over_time(0.99, {%s=~"$cluster",%s=~"$namespace",%s=~"$component.*"} |= "query stats" != "/api/v1/read" | logfmt | user=~"${tenant_id}" | user_agent=~"${user_agent}" | response_time > ${min_duration} | unwrap duration_seconds(length) [$__auto]) by ()' % [$._config.per_cluster_label, $._config.per_namespace_label, $._config.per_component_loki_label],
            'quantile_over_time(0.5, {%s=~"$cluster",%s=~"$namespace",%s=~"$component.*"} |= "query stats" != "/api/v1/read" | logfmt | user=~"${tenant_id}" | user_agent=~"${user_agent}" | response_time > ${min_duration} | unwrap duration_seconds(length) [$__auto]) by ()' % [$._config.per_cluster_label, $._config.per_namespace_label, $._config.per_component_loki_label],
          ],
          ['p99', 'p50'],
          unit='s',
        )
      )
      .addPanel(
        $.timeseriesPanel('Query wall time') +
        $.lokiMetricsQueryPanel(
          [
            'quantile_over_time(0.99, {%s=~"$cluster",%s=~"$namespace",%s=~"$component.*"} |= "query stats" != "/api/v1/read" | logfmt | user=~"${tenant_id}" | user_agent=~"${user_agent}" | response_time > ${min_duration} | unwrap query_wall_time_seconds [$__auto]) by ()' % [$._config.per_cluster_label, $._config.per_namespace_label, $._config.per_component_loki_label],
            'quantile_over_time(0.5, {%s=~"$cluster",%s=~"$namespace",%s=~"$component.*"} |= "query stats" != "/api/v1/read" | logfmt | user=~"${tenant_id}" | user_agent=~"${user_agent}" | response_time > ${min_duration} | unwrap query_wall_time_seconds [$__auto]) by ()' % [$._config.per_cluster_label, $._config.per_namespace_label, $._config.per_component_loki_label],
          ],
          ['p99', 'p50'],
          unit='s',
        )
        + $.panelDescription(
          'Query wall time',
          |||
            Seconds per second spent by queriers evaluating queries.
            This is roughly the product of the number of subqueries for a query and how long they took.
            In increase in this metric means that queries take more resources from the query path to evaluate.
          |||
        ),
      )
    )
    .addRow(
      $.row('Top 10 tenants') { collapse: true }
      .addPanel(
        $.timeseriesPanel('P99 response time') +
        $.lokiMetricsQueryPanel(
          'topk(10, quantile_over_time(0.99, {%s=~"$cluster",%s=~"$namespace",%s=~"$component.*"} |= "query stats" != "/api/v1/read" | logfmt | user=~"${tenant_id}" | user_agent=~"${user_agent}" | response_time > ${min_duration} | unwrap duration_seconds(response_time) [$__auto]) by (user))' % [$._config.per_cluster_label, $._config.per_namespace_label, $._config.per_component_loki_label],
          '{{user}}',
          unit='s',
        )
      )
      .addPanel(
        $.timeseriesPanel('P99 fetched series') +
        $.lokiMetricsQueryPanel(
          'topk(10, quantile_over_time(0.99, {%s=~"$cluster",%s=~"$namespace",%s=~"$component.*"} |= "query stats" != "/api/v1/read" | logfmt | user=~"${tenant_id}" | user_agent=~"${user_agent}" | response_time > ${min_duration} | unwrap fetched_series_count[$__auto]) by (user))' % [$._config.per_cluster_label, $._config.per_namespace_label, $._config.per_component_loki_label],
          '{{user}}',
        )
      )
      .addPanel(
        $.timeseriesPanel('P99 fetched chunks') +
        $.lokiMetricsQueryPanel(
          'topk(10, quantile_over_time(0.99, {%s=~"$cluster",%s=~"$namespace",%s=~"$component.*"} |= "query stats" != "/api/v1/read" | logfmt | user=~"${tenant_id}" | user_agent=~"${user_agent}" | response_time > ${min_duration} | unwrap fetched_chunk_bytes[$__auto]) by (user))' % [$._config.per_cluster_label, $._config.per_namespace_label, $._config.per_component_loki_label],
          '{{user}}',
          unit='bytes',
        )
      )
      .addPanel(
        $.timeseriesPanel('P99 response size') +
        $.lokiMetricsQueryPanel(
          'topk(10, quantile_over_time(0.99, {%s=~"$cluster",%s=~"$namespace",%s=~"$component.*"} |= "query stats" != "/api/v1/read" | logfmt | user=~"${tenant_id}" | user_agent=~"${user_agent}" | response_time > ${min_duration} | unwrap response_size_bytes[$__auto]) by (user))' % [$._config.per_cluster_label, $._config.per_namespace_label, $._config.per_component_loki_label],
          '{{user}}',
          unit='bytes',
        )
      )
      .addPanel(
        $.timeseriesPanel('P99 time span') +
        $.lokiMetricsQueryPanel(
          'topk(10, quantile_over_time(0.99, {%s=~"$cluster",%s=~"$namespace",%s=~"$component.*"} |= "query stats" != "/api/v1/read" | logfmt | user=~"${tenant_id}" | user_agent=~"${user_agent}" | response_time > ${min_duration} | unwrap duration_seconds(length) [$__auto]) by (user))' % [$._config.per_cluster_label, $._config.per_namespace_label, $._config.per_component_loki_label],
          '{{user}}',
          unit='s',
        )
      )
      .addPanel(
        $.timeseriesPanel('P99 query wall time') +
        $.lokiMetricsQueryPanel(
          'topk(10, quantile_over_time(0.99, {%s=~"$cluster",%s=~"$namespace",%s=~"$component.*"} |= "query stats" != "/api/v1/read" | logfmt | user=~"${tenant_id}" | user_agent=~"${user_agent}" | response_time > ${min_duration} | unwrap query_wall_time_seconds [$__auto]) by (user))' % [$._config.per_cluster_label, $._config.per_namespace_label, $._config.per_component_loki_label],
          '{{user}}',
          unit='s',
        )
        + $.panelDescription(
          'Query wall time',
          |||
            Seconds per second spent by queriers evaluating queries.
            This is roughly the product of the number of subqueries for a query and how long they took.
            In increase in this metric means that queries take more resources from the query path to evaluate.
          |||
        ),
      )
    )
    .addRow(
      (
        $.row('Top 10 User-Agents') { collapse: true }
        .addPanel(
          $.timeseriesPanel('P99 response time') +
          $.lokiMetricsQueryPanel(
            'topk(10, quantile_over_time(0.99, {%s=~"$cluster",%s=~"$namespace",%s=~"$component.*"} |= "query stats" != "/api/v1/read" | logfmt | user=~"${tenant_id}" | user_agent=~"${user_agent}" | response_time > ${min_duration} | unwrap duration_seconds(response_time) [$__auto]) by (user_agent))' % [$._config.per_cluster_label, $._config.per_namespace_label, $._config.per_component_loki_label],
            '{{user_agent}}',
            unit='s',
          )
        )
        .addPanel(
          $.timeseriesPanel('P99 fetched series') +
          $.lokiMetricsQueryPanel(
            'topk(10, quantile_over_time(0.99, {%s=~"$cluster",%s=~"$namespace",%s=~"$component.*"} |= "query stats" != "/api/v1/read" | logfmt | user=~"${tenant_id}" | user_agent=~"${user_agent}" | response_time > ${min_duration} | unwrap fetched_series_count[$__auto]) by (user_agent))' % [$._config.per_cluster_label, $._config.per_namespace_label, $._config.per_component_loki_label],
            '{{user_agent}}',
          )
        )
        .addPanel(
          $.timeseriesPanel('P99 fetched chunks') +
          $.lokiMetricsQueryPanel(
            'topk(10, quantile_over_time(0.99, {%s=~"$cluster",%s=~"$namespace",%s=~"$component.*"} |= "query stats" != "/api/v1/read" | logfmt | user=~"${tenant_id}" | user_agent=~"${user_agent}" | response_time > ${min_duration} | unwrap fetched_chunk_bytes[$__auto]) by (user_agent))' % [$._config.per_cluster_label, $._config.per_namespace_label, $._config.per_component_loki_label],
            '{{user_agent}}',
            unit='bytes',
          )
        )
        .addPanel(
          $.timeseriesPanel('P99 response size') +
          $.lokiMetricsQueryPanel(
            'topk(10, quantile_over_time(0.99, {%s=~"$cluster",%s=~"$namespace",%s=~"$component.*"} |= "query stats" != "/api/v1/read" | logfmt | user=~"${tenant_id}" | user_agent=~"${user_agent}" | response_time > ${min_duration} | unwrap response_size_bytes[$__auto]) by (user_agent))' % [$._config.per_cluster_label, $._config.per_namespace_label, $._config.per_component_loki_label],
            '{{user_agent}}',
            unit='bytes',
          )
        )
        .addPanel(
          $.timeseriesPanel('P99 time span') +
          $.lokiMetricsQueryPanel(
            'topk(10, quantile_over_time(0.99, {%s=~"$cluster",%s=~"$namespace",%s=~"$component.*"} |= "query stats" != "/api/v1/read" | logfmt | user=~"${tenant_id}" | user_agent=~"${user_agent}" | response_time > ${min_duration} | unwrap duration_seconds(length) [$__auto]) by (user_agent))' % [$._config.per_cluster_label, $._config.per_namespace_label, $._config.per_component_loki_label],
            '{{user_agent}}',
            unit='s',
          )
        )
        .addPanel(
          $.timeseriesPanel('P99 query wall time') +
          $.lokiMetricsQueryPanel(
            'topk(10, quantile_over_time(0.99, {%s=~"$cluster",%s=~"$namespace",%s=~"$component.*"} |= "query stats" != "/api/v1/read" | logfmt | user=~"${tenant_id}" | user_agent=~"${user_agent}" | response_time > ${min_duration} | unwrap query_wall_time_seconds [$__auto]) by (user_agent))' % [$._config.per_cluster_label, $._config.per_namespace_label, $._config.per_component_loki_label],
            '{{user_agent}}',
            unit='s',
          )
          + $.panelDescription(
            'Query wall time',
            |||
              Seconds per second spent by queriers evaluating queries.
              This is roughly the product of the number of subqueries for a query and how long they took.
              In increase in this metric means that queries take more resources from the query path to evaluate.
            |||
          ),
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
          datasource: '${loki_datasource}',

          // Query logs from Loki.
          targets: [
            {
              local extraFields = [
                'response_time_seconds="{{ if .response_time }} {{ duration .response_time }} {{ end }}"',
                'param_step_seconds="{{ if .param_step }} {{ div .param_step 1000 }} {{ end }}"',
                'length_seconds="{{ if .length }} {{ duration .length }} {{ end }}"',
              ],
              // Filter out the remote read endpoint.
              expr: '{%s=~"$cluster",%s=~"$namespace",%s=~"$component.*"} |= "query stats" != "/api/v1/read" | logfmt | user=~"${tenant_id}" | user_agent=~"${user_agent}" | response_time > ${min_duration} | label_format %s' % [$._config.per_cluster_label, $._config.per_namespace_label, $._config.per_component_loki_label, std.join(',', extraFields)],
              instant: false,
              legendFormat: '',
              range: true,
              refId: 'A',
            },
          ],

          local bytesFields = ['fetched_chunk_bytes', 'fetched_index_bytes', 'response_size_bytes', 'results_cache_hit_bytes', 'results_cache_miss_bytes'],
          local shortFields = ['estimated_series_count', 'fetched_chunks_count', 'fetched_series_count'],
          local secondsFields = ['Time span', 'Duration', 'Step', 'queue_time_seconds', 'query_wall_time_seconds'],

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
                local hiddenFields = ['caller', 'cluster', 'container', 'host', 'id', 'job', 'level', 'line', 'method', 'msg', 'name', 'namespace', 'path', 'pod', 'pod_template_hash', 'stream', 'traceID', 'tsNs', 'labels', 'Line', 'Time', 'gossip_ring_member', 'component', 'response_time', 'param_step', 'length'],

                excludeByName: {
                  [field]: true
                  for field in hiddenFields
                },

                // Order fields.
                local orderedFields = ['ts', 'status', 'user', 'length_seconds', 'param_start', 'param_end', 'param_time', 'param_step_seconds', 'param_query', 'response_time_seconds', 'err'],

                indexByName: {
                  [orderedFields[i]]: i
                  for i in std.range(0, std.length(orderedFields) - 1)
                },

                // Rename fields.
                renameByName: {
                  ts: 'Completion date',
                  user: 'Tenant ID',
                  param_query: 'Query',
                  param_step_seconds: 'Step',
                  param_start: 'Start',
                  param_end: 'End',
                  param_time: 'Time (instant query)',
                  response_time_seconds: 'Duration',
                  length_seconds: 'Time span',
                  err: 'Error',
                },
              },
            },
            {
              // Transforma some fields into numbers so sorting in the table doesn't sort them lexicographically.
              id: 'convertFieldType',
              options: {
                local numericFields = ['sharded_queries', 'split_queries'] + bytesFields + shortFields + secondsFields,

                conversions: [
                  {
                    targetField: fieldName,
                    destinationType: 'number',
                  }
                  for fieldName in numericFields
                ],
              },
            },
          ],

          fieldConfig: {
            // Configure overrides to nicely format field values.
            overrides:
              [
                {
                  matcher: { id: 'byName', options: fieldName },
                  properties: [{ id: 'unit', value: 'bytes' }],
                }
                for fieldName in bytesFields
              ] +
              [
                {
                  matcher: { id: 'byName', options: fieldName },
                  properties: [{ id: 'unit', value: 'short' }],
                }
                for fieldName in shortFields
              ] +
              [
                {
                  matcher: { id: 'byName', options: fieldName },
                  properties: [{ id: 'unit', value: 's' }],
                }
                for fieldName in secondsFields
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
            name: 'loki_datasource',
            label: 'Loki data source',
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
          {
            multi: false,
            name: 'component',
            label: 'Component',
            type: 'custom',
            current: {
              selected: true,
              text: 'query-frontend',
              value: 'query-frontend',
            },
            options: [
              {
                selected: true,
                text: 'query-frontend',
                value: 'query-frontend',
              },
              {
                selected: false,
                text: 'ruler-query-frontend',
                value: 'ruler-query-frontend',
              },
            ],
            query: 'query-frontend, ruler-query-frontend',
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
