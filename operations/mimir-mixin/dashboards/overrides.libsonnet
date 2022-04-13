local utils = import 'mixin-utils/utils.libsonnet';

(import 'dashboard-utils.libsonnet') {
  'mimir-overrides.json':
    ($.dashboard('Overrides') + { uid: 'b5c95fee2e5e7c4b5930826ff6e89a12' })
    .addClusterSelectorTemplates(false)
    .addRow(
      $.row('')
      .addPanel(
        {
          title: 'Defaults',
          type: 'table',
          datasource: '${datasource}',
          targets: [
            {
              expr: 'max by(limit_name) (cortex_limits_defaults{%s=~"$cluster",namespace=~"$namespace"})' % $._config.per_cluster_label,
              instant: true,
              legendFormat: '',
              refId: 'A',
            },
          ],

          // Use Grafana transformations to display fields in a table.
          transformations: [
            {
              // Convert labels to fields.
              id: 'labelsToFields',
              options: {},
            },
            {
              // Merge rows.
              id: 'merge',
              options: {},
            },
            {
              // Hide "Time" column and show "limit_name" before "Value".
              id: 'organize',
              options: {
                excludeByName: {
                  Time: true,
                },
                indexByName: {
                  limit_name: 0,
                  Value: 1,
                },
              },
            },
            {
              // Sort by "limit_name".
              id: 'sortBy',
              options: {
                fields: {},
                sort: [
                  {
                    field: 'limit_name',
                  },
                ],
              },
            },
          ],
        },
      )
    ).addRow(
      $.row('')
      .addPanel(
        {
          title: 'Per-tenant overrides',
          type: 'table',
          datasource: '${datasource}',
          targets: [
            {
              expr: 'max by(user, limit_name) (cortex_limits_overrides{%s=~"$cluster",namespace=~"$namespace",user=~"${tenant_id}"})' % $._config.per_cluster_label,
              instant: true,
              legendFormat: '',
              refId: 'A',
            },
          ],

          // Use Grafana transformations to display fields in a table.
          transformations: [
            {
              // Convert "limit_name" labels to columns.
              id: 'labelsToFields',
              options: {
                mode: 'columns',
                valueLabel: 'limit_name',
              },
            },
            {
              // Merge rows by "user" (it's the only label in common after moving "limit_name" to columns).
              id: 'merge',
              options: {},
            },
            {
              // Hide "Time" and move "user" column to first one.
              id: 'organize',
              options: {
                excludeByName: {
                  Time: true,
                },
                indexByName: {
                  user: 0,
                },
              },
            },
          ],
        },
      )
    )
    + {
      templating+: {
        list+: [
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
          // Do not allow to include all clusters/namespaces otherwise this dashboard
          // risks to explode because it shows limits per tenant.
          l + (if (l.name == 'cluster' || l.name == 'namespace') then { includeAll: false } else {})
          for l in super.list
        ],
      },
    } + {
      // No auto-refresh by default.
      refresh: '',
    },
}
