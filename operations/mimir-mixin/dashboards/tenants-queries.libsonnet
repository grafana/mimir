local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-tenants-queries.json';

(import 'dashboard-utils.libsonnet') {
  [filename]:
    ($.dashboard('Tenants (Queries)') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates(multi=false)
    .addActiveUserSelectorTemplates()
    .addRow(
      ($.row('Tenants (Queries) dashboard description') { height: '25px', showTitle: false })
      .addPanel(
        $.textPanel('', |||
          <p>
            This dashboard shows statistics derived from query-frontend logs, detailed by tenant (user) selected above.
          </p>
        |||),
      )
    )


    .addRow(
      $.row('Queries')
      .addPanel(
        $.panel('Requests / sec') +
        $.queryPanel(
          |||
            sum by (status) (mimir_user:queries:rate5m{%(matcher)s, user="$user"})
          |||
          % { matcher: $.namespaceMatcher() },
          '{{ status }}'
        ) +
        $.stack +
        {
          yaxes: $.yaxes('reqps'),
          aliasColors: {
            success: '#7EB26D',
            failed: '#E24D42',
          },
        }
      )
      .addPanel(
        $.panel('Latency') +
        $.queryPanel(
          [
            'mimir_user:query_response_time_seconds_p99:rate5m{%(matcher)s, user="$user"}'
            % { matcher: $.namespaceMatcher() },
            'mimir_user:query_response_time_seconds_p90:rate5m{%(matcher)s, user="$user"}'
            % { matcher: $.namespaceMatcher() },
            'mimir_user:query_response_time_seconds_p50:rate5m{%(matcher)s, user="$user"}'
            % { matcher: $.namespaceMatcher() },
          ],
          [
            '99th Percentile',
            '90th Percentile',
            '50th Percentile',
          ]
        ) +
        { yaxes: $.yaxes('s') }
      )
    )

    .addRow(
      $.row('Statistics: Wall Time')
      .addPanel(
        $.panel('Wall Time (per query)') +
        $.queryPanel(
          [
            'mimir_user:query_wall_time_seconds_p99:rate5m{%(matcher)s, user="$user"}'
            % { matcher: $.namespaceMatcher() },
            'mimir_user:query_wall_time_seconds_p90:rate5m{%(matcher)s, user="$user"}'
            % { matcher: $.namespaceMatcher() },
            'mimir_user:query_wall_time_seconds_p50:rate5m{%(matcher)s, user="$user"}'
            % { matcher: $.namespaceMatcher() },
          ],
          [
            '99th Percentile',
            '90th Percentile',
            '50th Percentile',
          ]
        ) +
        { yaxes: $.yaxes('s') }
      )
      .addPanel(
        $.panel('Wall Time (user total per second)') +
        $.queryPanel(
          [
            'sum by (user) (mimir_user:query_wall_time_seconds:rate5m{%(matcher)s, user="$user"})'
            % { matcher: $.namespaceMatcher() },
          ],
          [
            'total',
          ]
        ) +
        { yaxes: $.yaxes('s') }
      )
      .addPanel(
        $.panel('Wall Time (user total vs namespace total)') +
        $.queryPanel(
          [
            |||
              sum by (namespace, user) (mimir_user:query_wall_time_seconds:rate5m{%(matcher)s, user="$user"})
              / on(namespace) group_left
              sum by (namespace) (mimir_user:query_wall_time_seconds:rate5m{%(matcher)s})
            |||
            % { matcher: $.namespaceMatcher() },
          ],
          [
            'percent',
          ]
        ) +
        { yaxes: $.yaxes('percentunit') }
      )
    )
    .addRow(
      $.row('Statistics: Fetched Chunk Bytes')
      .addPanel(
        $.panel('Fetched Chunk Bytes (per query)') +
        $.queryPanel(
          [
            'mimir_user:query_fetched_chunk_bytes_p99:rate5m{%(matcher)s, user="$user"}'
            % { matcher: $.namespaceMatcher() },
            'mimir_user:query_fetched_chunk_bytes_p90:rate5m{%(matcher)s, user="$user"}'
            % { matcher: $.namespaceMatcher() },
            'mimir_user:query_fetched_chunk_bytes_p50:rate5m{%(matcher)s, user="$user"}'
            % { matcher: $.namespaceMatcher() },
          ],
          [
            '99th Percentile',
            '90th Percentile',
            '50th Percentile',
          ]
        ) +
        { yaxes: $.yaxes('bytes') }
      )
      .addPanel(
        $.panel('Fetched Chunk Bytes (user total per second)') +
        $.queryPanel(
          [
            'sum by (user) (mimir_user:query_fetched_chunk_bytes:rate5m{%(matcher)s, user="$user"})'
            % { matcher: $.namespaceMatcher() },
          ],
          [
            'total',
          ]
        ) +
        { yaxes: $.yaxes('bytes') }
      )
      .addPanel(
        $.panel('Fetched Chunk Bytes (user total vs namespace total)') +
        $.queryPanel(
          [
            |||
              sum by (namespace, user) (mimir_user:query_fetched_chunk_bytes:rate5m{%(matcher)s, user="$user"})
              / on(namespace) group_left
              sum by (namespace) (mimir_user:query_fetched_chunk_bytes:rate5m{%(matcher)s})
            |||
            % { matcher: $.namespaceMatcher() },
          ],
          [
            'percent',
          ]
        ) +
        { yaxes: $.yaxes('percentunit') }
      )
    )
    .addRow(
      $.row('Statistics: Fetched Index Bytes')
      .addPanel(
        $.panel('Fetched Index Bytes (per query)') +
        $.queryPanel(
          [
            'mimir_user:query_fetched_index_bytes_p99:rate5m{%(matcher)s, user="$user"}'
            % { matcher: $.namespaceMatcher() },
            'mimir_user:query_fetched_index_bytes_p90:rate5m{%(matcher)s, user="$user"}'
            % { matcher: $.namespaceMatcher() },
            'mimir_user:query_fetched_index_bytes_p50:rate5m{%(matcher)s, user="$user"}'
            % { matcher: $.namespaceMatcher() },
          ],
          [
            '99th Percentile',
            '90th Percentile',
            '50th Percentile',
          ]
        ) +
        { yaxes: $.yaxes('bytes') }
      )
      .addPanel(
        $.panel('Fetched Index Bytes (user total per second)') +
        $.queryPanel(
          [
            'sum by (user) (mimir_user:query_fetched_index_bytes:rate5m{%(matcher)s, user="$user"})'
            % { matcher: $.namespaceMatcher() },
          ],
          [
            'total',
          ]
        ) +
        { yaxes: $.yaxes('bytes') }
      )
      .addPanel(
        $.panel('Fetched Index Bytes (user total vs namespace total)') +
        $.queryPanel(
          [
            |||
              sum by (namespace, user) (mimir_user:query_fetched_index_bytes:rate5m{%(matcher)s, user="$user"})
              / on(namespace) group_left
              sum by (namespace) (mimir_user:query_fetched_index_bytes:rate5m{%(matcher)s})
            |||
            % { matcher: $.namespaceMatcher() },
          ],
          [
            'percent',
          ]
        ) +
        { yaxes: $.yaxes('percentunit') }
      )
    ),
}
