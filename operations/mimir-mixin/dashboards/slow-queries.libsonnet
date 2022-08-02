local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-slow-queries.json';

(import 'dashboard-utils.libsonnet') {
  [filename]:
    ($.dashboard('Slow queries') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates(false)
    .addRow(
      $.row('')
      .addPanel(
        {
          title: 'Slow queries',
          type: 'table',
          datasource: '${lokidatasource}',

          // Query logs from Loki.
          targets: [
            {
              // Filter out the remote read endpoint.
              expr: '{%s=~"$cluster",namespace=~"$namespace",name=~"query-frontend.*"} |= "query stats" != "/api/v1/read" | logfmt | user=~"${tenant_id}" | response_time > ${min_duration}' % $._config.per_cluster_label,
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
              // Compute the query time range.
              id: 'calculateField',
              options: {
                alias: 'Time range',
                mode: 'binary',
                binary: {
                  left: 'param_end',
                  operator: '-',
                  reducer: 'sum',
                  right: 'param_start',
                },
                reduce: { reducer: 'sum' },
                replaceFields: false,
              },
            },
            {
              id: 'organize',
              options: {
                // Hide fields we don't care.
                local hiddenFields = ['caller', 'cluster', 'container', 'host', 'id', 'job', 'level', 'line', 'method', 'msg', 'name', 'namespace', 'param_end', 'param_start', 'param_time', 'path', 'pod', 'pod_template_hash', 'query_wall_time_seconds', 'stream', 'traceID', 'tsNs', 'labels', 'Line', 'Time'],

                excludeByName: {
                  [field]: true
                  for field in hiddenFields
                },

                // Order fields.
                local orderedFields = ['ts', 'user', 'param_query', 'Time range', 'param_step', 'response_time'],

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
