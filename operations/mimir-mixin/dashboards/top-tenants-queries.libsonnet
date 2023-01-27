local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-top-tenants-queries.json';

(import 'dashboard-utils.libsonnet') {
  [filename]:
    ($.dashboard('Top tenants (Queries)') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates()
    .addCustomTemplate('limit', ['10', '50', '100'])
    .addRow(
      ($.row('Top tenants dashboard description') { height: '25px', showTitle: false })
      .addPanel(
        $.textPanel('', |||
          <p>
            This dashboard shows the top tenants based on statistics derived from query-frontend logs.
            Rows are collapsed by default to avoid querying all of them.
            Use the templating variable "limit" above to select the amount of users to be shown.
          </p>
        |||),
      )
    )

    .addRow(
      ($.row('By query rate') + { collapse: true })
      .addPanel(
        $.panel('Top $limit users by query rate') +
        { sort: { col: 3, desc: true } } +
        $.tablePanel(
          [
            |||
              topk($limit,
                sum by (namespace, user) (mimir_user:queries:rate5m{%(matcher)s})
              )
            ||| % {
              matcher: $.namespaceMatcher(),
            },
          ],
          { Value: { alias: 'queries/sec' } }
        )
      )
    )

    .addRow(
      ($.row('By query failure rate') + { collapse: true })
      .addPanel(
        $.panel('Top $limit users by query failure rate') +
        { sort: { col: 3, desc: true } } +
        $.tablePanel(
          [
            |||
              topk($limit,
                sum by (namespace, user) (mimir_user:queries:rate5m{%(matcher)s,status="failed"})
              )
            ||| % {
              matcher: $.namespaceMatcher(),
            },
          ],
          { Value: { alias: 'failures/sec' } }
        )
      )
    )

    .addRow(
      ($.row('By query failure rate (percentage)') + { collapse: true })
      .addPanel(
        $.panel('Top $limit users by query failure rate (as percentage of total rate)') +
        { sort: { col: 3, desc: true } } +
        $.tablePanel(
          [
            |||
              topk($limit,
                sum by (namespace, user) (mimir_user:queries:rate5m{%(matcher)s,status="failed"})
                /
                sum by (namespace, user) (mimir_user:queries:rate5m{%(matcher)s})
              )
            ||| % {
              matcher: $.namespaceMatcher(),
            },
          ],
          { Value: { alias: '% failures/sec' } }
        )
      )
    )

    .addRow(
      ($.row('By response time (p99)') + { collapse: true })
      .addPanel(
        $.panel('Top $limit users by query response time (p99)') +
        { sort: { col: 3, desc: true } } +
        $.tablePanel(
          [
            |||
              topk($limit,
                max by (namespace, user) (mimir_user:query_response_time_seconds_p99:rate5m{%(matcher)s})
              )
            ||| % {
              matcher: $.namespaceMatcher(),
            },
          ],
          { Value: { alias: 'seconds' } }
        )
      )
    )

    .addRow(
      ($.row('By wall clock time (p99)') + { collapse: true })
      .addPanel(
        $.panel('Top $limit users by wall clock time per query (p99)') +
        { sort: { col: 3, desc: true } } +
        $.tablePanel(
          [
            |||
              topk($limit,
                max by (namespace, user) (mimir_user:query_wall_time_seconds_p99:rate5m{%(matcher)s})
              )
            ||| % {
              matcher: $.namespaceMatcher(),
            },
          ],
          { Value: { alias: 'seconds' } }
        )
      )
    )

    .addRow(
      ($.row('By wall clock time (total)') + { collapse: true })
      .addPanel(
        $.panel('Top $limit users by wall clock time for all queries (querier seconds per second)') +
        { sort: { col: 3, desc: true } } +
        $.tablePanel(
          [
            |||
              topk($limit,
                sum by (namespace, user) (mimir_user:query_wall_time_seconds:rate5m{%(matcher)s})
              )
            ||| % {
              matcher: $.namespaceMatcher(),
            },
          ],
          { Value: { alias: 'seconds/sec' } }
        )
      )
    )

    .addRow(
      ($.row('By wall clock time (total vs all users)') + { collapse: true })
      .addPanel(
        $.panel('Top $limit users by wall clock time over all queries (percentage vs total for all users)') +
        { sort: { col: 3, desc: true } } +
        $.tablePanel(
          [
            |||
              topk($limit,
                sum by (namespace, user) (mimir_user:query_wall_time_seconds:rate5m{%(matcher)s})
                / on(namespace) group_left
                sum by (namespace) (mimir_user:query_wall_time_seconds:rate5m{%(matcher)s})
              )
              * 100
            ||| % {
              matcher: $.namespaceMatcher(),
            },
          ],
          { Value: { alias: '%' } }
        )
      )
    )

    .addRow(
      ($.row('By fetched chunk bytes (p99)') + { collapse: true })
      .addPanel(
        $.panel('Top $limit users by fetched chunk bytes per query (p99)') +
        { sort: { col: 3, desc: true } } +
        $.tablePanel(
          [
            |||
              topk($limit,
                max by (namespace, user) (mimir_user:query_fetched_chunk_bytes_p99:rate5m{%(matcher)s})
              )
            ||| % {
              matcher: $.namespaceMatcher(),
            },
          ],
          { Value: { alias: 'bytes' } }
        )
      )
    )

    .addRow(
      ($.row('By fetched chunk bytes (total)') + { collapse: true })
      .addPanel(
        $.panel('Top $limit users by fetched chunk bytes over all queries (bytes per second)') +
        { sort: { col: 3, desc: true } } +
        $.tablePanel(
          [
            |||
              topk($limit,
                sum by (namespace, user) (mimir_user:query_fetched_chunk_bytes:rate5m{%(matcher)s})
              )
            ||| % {
              matcher: $.namespaceMatcher(),
            },
          ],
          { Value: { alias: 'bytes/sec' } }
        )
      )
    )

    .addRow(
      ($.row('By fetched chunk bytes (total vs all users)') + { collapse: true })
      .addPanel(
        $.panel('Top $limit users by fetched chunk bytes over all queries (percentage vs total for all users)') +
        { sort: { col: 3, desc: true } } +
        $.tablePanel(
          [
            |||
              topk($limit,
                sum by (namespace, user) (mimir_user:query_fetched_chunk_bytes:rate5m{%(matcher)s})
                / on(namespace) group_left
                sum by (namespace) (mimir_user:query_fetched_chunk_bytes:rate5m{%(matcher)s})
              )
              * 100
            ||| % {
              matcher: $.namespaceMatcher(),
            },
          ],
          { Value: { alias: '%' } }
        )
      )
    )

    .addRow(
      ($.row('By fetched index bytes (p99)') + { collapse: true })
      .addPanel(
        $.panel('Top $limit users by fetched index bytes per query (p99)') +
        { sort: { col: 3, desc: true } } +
        $.tablePanel(
          [
            |||
              topk($limit,
                max by (namespace, user) (mimir_user:query_fetched_index_bytes_p99:rate5m{%(matcher)s})
              )
            ||| % {
              matcher: $.namespaceMatcher(),
            },
          ],
          { Value: { alias: 'bytes' } }
        )
      )
    )

    .addRow(
      ($.row('By fetched index bytes (total)') + { collapse: true })
      .addPanel(
        $.panel('Top $limit users by fetched index bytes over all queries (bytes per second)') +
        { sort: { col: 3, desc: true } } +
        $.tablePanel(
          [
            |||
              topk($limit,
                sum by (namespace, user) (mimir_user:query_fetched_index_bytes:rate5m{%(matcher)s})
              )
            ||| % {
              matcher: $.namespaceMatcher(),
            },
          ],
          { Value: { alias: 'bytes/sec' } }
        )
      )
    )

    .addRow(
      ($.row('By fetched index bytes (total vs all users)') + { collapse: true })
      .addPanel(
        $.panel('Top $limit users by fetched index bytes over all queries (percentage vs total for all users)') +
        { sort: { col: 3, desc: true } } +
        $.tablePanel(
          [
            |||
              topk($limit,
                sum by (namespace, user) (mimir_user:query_fetched_index_bytes:rate5m{%(matcher)s})
                / on(namespace) group_left
                sum by (namespace) (mimir_user:query_fetched_index_bytes:rate5m{%(matcher)s})
              )
              * 100
            ||| % {
              matcher: $.namespaceMatcher(),
            },
          ],
          { Value: { alias: '%' } }
        )
      )
    ),
}
