local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-ruler.json';

(import 'dashboard-utils.libsonnet') +
(import 'dashboard-queries.libsonnet') {
  local ruler_config_api_routes_re = '(%s)|(%s)' % [
    // Prometheus API routes which are also exposed by Mimir.
    '(api_prom_api_v1|prometheus_api_v1)_(rules|alerts|status_buildinfo)',
    // Mimir-only API routes used for rule configuration.
    '(api_prom|api_v1|prometheus|prometheus_config_v1)_rules.*',
  ],

  [filename]:
    assert std.md5(filename) == '631e15d5d85afb2ca8e35d62984eeaa0' : 'UID of the dashboard has changed, please update references to dashboard.';
    ($.dashboard('Ruler') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates()
    .addShowNativeLatencyVariable($.latencyVariableDefault())
    .addRow(
      ($.row('Headlines') + {
         height: '100px',
         showTitle: false,
       })
      .addPanel(
        $.panel('Active configurations') +
        $.statPanel('sum(cortex_ruler_managers_total{%s})' % $.jobMatcher($._config.job_names.ruler), format='short')
      )
      .addPanel(
        $.panel('Total rules') +
        $.statPanel('sum(cortex_prometheus_rule_group_rules{%s})' % $.jobMatcher($._config.job_names.ruler), format='short')
      )
      .addPanel(
        $.panel('Reads from ingesters - RPS') +
        $.ncSumCountRateStatPanel(
          metric='cortex_ingester_client_request_duration_seconds',
          selectors=$.jobSelector($._config.job_names.ruler + $._config.job_names.ruler_querier) + [utils.selector.eq('operation', '/cortex.Ingester/QueryStream')],
        ) + $.panelDescription(
          'Reads from ingesters - RPS',
          |||
            Note: Even while operating in Remote ruler mode you will still see values for this panel.

            This is because the metrics are inclusive of intermediate services and are showing the requests that ultimately reach the ingesters.

            For a more detailed view of the read path when using remote ruler mode, see the Remote ruler reads dashboard.
          |||
        ),
      )
      .addPanel(
        $.panel(
          if $._config.show_ingest_storage_panels then
            'Writes to ingesters / ingest storage - RPS'
          else
            'Writes to ingesters - RPS'
        ) +
        $.newStatPanel(
          local selectors = $.jobSelector($._config.job_names.ruler) + [utils.selector.eq('operation', '/cortex.Ingester/Push')];
          local baseQueries = $.ncSumHistogramCountRate('cortex_ingester_client_request_duration_seconds', selectors);

          local queryTemplate(baseQuery) =
            local params = {
              job_matcher: $.jobMatcher($._config.job_names.ruler),
              base_query: baseQuery,
            };
            if $._config.show_ingest_storage_panels then
              |||
                # Classic architecture.
                ((%(base_query)s) or vector(0))
                +
                # Ingest storage architecture.
                (sum(
                    rate(cortex_ingest_storage_writer_produce_records_enqueued_total{%(job_matcher)s}[$__rate_interval])
                ) or vector(0))
              ||| % params
            else
              |||
                %(base_query)s
              ||| % params;

          local query = {
            classic: queryTemplate(baseQueries.classic),
            native: queryTemplate(baseQueries.native),
          };
          local queries = [
            utils.showClassicHistogramQuery(query),
            utils.showNativeHistogramQuery(query),
          ];

          queries, legends=['', ''], unit='reqps', instant=true
        ) + { options: { colorMode: 'none' } },
      )
    )
    .addRow(
      $.row('Rule evaluations global')
      .addPanel(
        $.timeseriesPanel('Evaluations per second') +
        $.successFailureCustomPanel(
          [
            $.queries.ruler.evaluations.successPerSecond,
            $.queries.ruler.evaluations.failurePerSecond,
            $.queries.ruler.evaluations.missedIterationsPerSecond,
          ],
          ['success', 'failed', 'missed'],
        ),
      )
      .addPanel(
        $.timeseriesPanel('Latency') +
        $.queryPanel(
          $.queries.ruler.evaluations.latency,
          'average'
        ) +
        { fieldConfig+: { defaults+: { unit: 's' } } },
      )
    )
    .addRow(
      $.row('Rule group sync')
      .addPanel(
        $.timeseriesPanel('Ruler ring members') +
        $.queryPanel(
          // Every replica reports its own view of its cell's ring.
          // Take the maximum within each cell, then sum across the selected cells.
          // Group by cluster and namespace, not job.
          // Zone-aware rulers share one ring across several jobs.
          'sum by (state) (max by (%s, state) (cortex_ring_members{name="ruler", %s}))' % [std.join(', ', $._config.cluster_labels), $.jobMatcher($._config.job_names.ruler)],
          '{{ state }}'
        ) +
        { fieldConfig+: { defaults+: { unit: 'short' } } } +
        $.panelDescription(
          'Ruler ring members',
          |||
            Ruler ring members by state. Every replica reports its own view of its cell's ring, so this takes the maximum within each ring and then sums across the selected cells.

            `ACTIVE` is the set rule groups are evaluated over. A replica's first sync after startup shards over `ACTIVE` and `JOINING`, which is why a rollout briefly raises the rule group total.

            Ring membership can change within a scrape interval, so this panel may miss `JOINING` and `LEAVING` states. It can also overcount by one while replicas disagree, because the maximum is taken for each state separately. The `ring-change` series in the Syncs panel is the reliable record that the ring changed.
          |||
        ),
      )
      .addPanel(
        $.timeseriesPanel('Syncs') +
        $.queryPanel(
          'sum by (reason) (rate(cortex_ruler_sync_rules_total{%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.ruler),
          '{{ reason }}'
        ) +
        $.panelDescription(
          'Syncs',
          |||
            Rate of rule group sync operations, split by what triggered them.

            - `periodic`: the `-ruler.poll-interval` timer, 10m by default.
            - `ring-change`: the ruler saw the ruler ring change and resynced. Watch this to spot rule group resharding.
            - `api-change`: a tenant changed its rule configuration.
            - `initial`: the sync at startup, before the ruler starts to evaluate rules.
          |||
        ),
      )
      .addPanel(
        $.timeseriesPanel('Sync duration') +
        $.ncLatencyPanel('cortex_ruler_sync_rules_duration_seconds', '%s' % $.jobMatcher($._config.job_names.ruler)) +
        $.panelDescription(
          'Sync duration',
          |||
            Time for a ruler replica to sync the rule groups it owns.

            This includes the time to list and load the rule groups from object storage, filter them, and apply them to the rule managers. For the object storage operations alone, see the ruler configuration object store row.

            The metric covers both full and partial syncs, and is not split by what triggered the sync.
          |||
        ),
      )
    )
    .addRow(
      $.row('Rule group ownership')
      .addPanel(
        $.timeseriesPanel('Rule groups per replica') +
        $.queryPanel(
          'count by (%s) (cortex_prometheus_rule_group_rules{%s})' % [$._config.per_instance_label, $.jobMatcher($._config.job_names.ruler)],
          '{{ %s }}' % $._config.per_instance_label
        ) +
        // Stack to read the total the selected cells evaluate.
        // Stacking also avoids misreading a per-replica drop caused by a ruler scale out.
        $.stack +
        { fieldConfig+: { defaults+: { unit: 'short', custom+: { fillOpacity: 20, lineWidth: 1 } } } } +
        $.panelDescription(
          'Rule groups per replica',
          |||
            Number of rule groups owned by each ruler replica, stacked.

            Each rule group is owned by one replica at a time, so in steady state the stack total is the number of rule groups the selected cells evaluate. A band shrinking while another grows is resharding.

            The total rises during a rollout. A replica loads its rule groups while joining the ring, before it starts evaluating them, so joining and leaving replicas are counted alongside the ones they replace.

            A lasting imbalance in band heights means the rule groups are not spread evenly over the ruler ring tokens.
          |||
        ),
      )
      .addPanel(
        $.timeseriesPanel('Tenants per replica') +
        $.queryPanel(
          'max by (%s) (cortex_ruler_managers_total{%s})' % [$._config.per_instance_label, $.jobMatcher($._config.job_names.ruler)],
          '{{ %s }}' % $._config.per_instance_label
        ) +
        // Stack to compare replicas.
        // Stacking also avoids misreading a per-replica drop caused by a ruler scale out.
        $.stack +
        { fieldConfig+: { defaults+: { unit: 'short', custom+: { fillOpacity: 20, lineWidth: 1 } } } } +
        $.panelDescription(
          'Tenants per replica',
          |||
            Number of tenants with a rule manager on each ruler replica, stacked.

            A replica keeps a rule manager for a tenant while it owns at least one of that tenant's rule groups. A tenant is therefore counted once per replica holding its rule groups, so the stack total is higher than the number of tenants in the cell.

            Many rule groups can move between replicas without this changing, so read it as ownership context rather than as a measure of resharding.
          |||
        ),
      )
      .addPanel(
        $.timeseriesPanel('Rule configuration updates') +
        $.queryPanel(
          'sum(rate(cortex_ruler_config_updates_total{%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.ruler),
          'updates'
        ) +
        $.panelDescription(
          'Rule configuration updates',
          |||
            Rate of tenants whose rule set on a replica changed during a sync.

            This counts both a tenant editing its rules and rule groups moving between replicas. Read it with the Syncs panel: a rise next to `ring-change` syncs is resharding, a rise next to `api-change` syncs is a tenant edit.
          |||
        ),
      )
      .addPanel(
        $.timeseriesPanel('Ring check errors') +
        $.failurePanel('sum(rate(cortex_ruler_ring_check_errors_total{%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.ruler), 'Errors / sec') +
        $.panelDescription(
          'Ring check errors',
          |||
            Rate of failures to read the ruler ring while working out which rule groups a replica owns.

            Rule groups can be missed while this is not zero.
          |||
        ),
      )
    )
    .addRowIf(
      $._config.gateway_enabled,
      $.row('Configuration API (gateway)')
      .addPanel(
        $.timeseriesPanel('QPS') +
        $.qpsPanelNativeHistogram($.queries.ruler.requestsPerSecondMetric, utils.toPrometheusSelectorNaked($.jobSelector($._config.job_names.gateway) + [utils.selector.re('route', ruler_config_api_routes_re)]))
      )
      .addPanel(
        $.timeseriesPanel('Latency') +
        $.latencyRecordingRulePanelNativeHistogram($.queries.gateway.requestsPerSecondMetric, $.jobSelector($._config.job_names.gateway) + [utils.selector.re('route', ruler_config_api_routes_re)])
      )
      .addPanel(
        local selectors = $.jobSelector($._config.job_names.gateway) + [utils.selector.re('route', ruler_config_api_routes_re)];
        local labels = std.join('_', [matcher.label for matcher in selectors]);
        local metricStr = '%(labels)s:%(metric)s' % { labels: labels, metric: $.queries.gateway.requestsPerSecondMetric };
        $.timeseriesPanel('Per route p99 latency') +
        $.perInstanceLatencyPanelNativeHistogram('0.99', metricStr, selectors, legends=['{{ route }}', '{{ route }}'], instanceLabel='route', from_recording=true) +
        { fieldConfig+: { defaults+: { unit: 's' } } },
      )
    )
    .addRow(
      $.row('Writes (ingesters)')
      .addPanel(
        $.timeseriesPanel('Requests / sec') +
        $.qpsPanelNativeHistogram('cortex_ingester_client_request_duration_seconds', '%s, operation="/cortex.Ingester/Push"' % $.jobMatcher($._config.job_names.ruler))
      )
      .addPanel(
        $.timeseriesPanel('Latency') +
        $.ncLatencyPanel('cortex_ingester_client_request_duration_seconds', '%s, operation="/cortex.Ingester/Push"' % $.jobMatcher($._config.job_names.ruler))
      )
    )
    .addRowIf(
      $._config.show_ingest_storage_panels,
      $.row('Writes (ingest storage)')
      .addPanel(
        $.ingestStorageKafkaProducedRecordsRatePanel($.jobMatcher($._config.job_names.ruler))
      )
      .addPanel(
        $.ingestStorageKafkaProducedRecordsLatencyPanel($.jobMatcher($._config.job_names.ruler))
      )
    )
    .addRow(
      $.row('Reads (ingesters)')
      .addPanel(
        $.timeseriesPanel('QPS') +
        $.qpsPanelNativeHistogram('cortex_ingester_client_request_duration_seconds', '%s, operation="/cortex.Ingester/QueryStream"' % $.jobMatcher($._config.job_names.ruler + $._config.job_names.ruler_querier))
      )
      .addPanel(
        $.timeseriesPanel('Latency') +
        $.ncLatencyPanel('cortex_ingester_client_request_duration_seconds', '%s, operation="/cortex.Ingester/QueryStream"' % $.jobMatcher($._config.job_names.ruler + $._config.job_names.ruler_querier))
      )
    )
    .addRow(
      $.row('Ruler resources')
      .addPanel(
        $.containerCPUUsagePanelByComponent('ruler'),
      )
      .addPanel(
        $.containerMemoryWorkingSetPanelByComponent('ruler'),
      )
      .addPanel(
        $.containerGoHeapInUsePanelByComponent('ruler'),
      )
      .addPanel(
        $.containerEphemeralStoragePanelByComponent('ruler'),
      )
    )
    .addRowIf(
      $._config.autoscaling.ruler.enabled,
      $.cpuAndMemoryBasedAutoScalingRow('Ruler'),
    )
    .addRowIf(
      $._config.autoscaling.ruler_query_frontend.enabled,
      $.cpuAndMemoryBasedAutoScalingRow('Ruler-query-frontend'),
    )
    .addRow(
      $.kvStoreRow('Ruler - key-value store for rulers ring', 'ruler', 'ruler')
    )
    .addRow(
      $.row('Ruler - blocks storage')
      .addPanel(
        $.timeseriesPanel('Number of store-gateways hit per query') +
        $.ncLatencyPanel('cortex_querier_storegateway_instances_hit_per_query', '%s' % $.jobMatcher($._config.job_names.ruler), multiplier=1) +
        { fieldConfig+: { defaults+: { unit: 'short' } } },
      )
      .addPanel(
        $.timeseriesPanel('Refetches of missing blocks per query') +
        $.ncLatencyPanel('cortex_querier_storegateway_refetches_per_query', '%s' % $.jobMatcher($._config.job_names.ruler), multiplier=1) +
        { fieldConfig+: { defaults+: { unit: 'short' } } },
      )
      .addPanel(
        $.timeseriesPanel('Consistency checks failed') +
        $.failurePanel('sum(rate(cortex_querier_blocks_consistency_checks_failed_total{%s}[$__rate_interval])) / sum(rate(cortex_querier_blocks_consistency_checks_total{%s}[$__rate_interval]))' % [$.jobMatcher($._config.job_names.ruler), $.jobMatcher($._config.job_names.ruler)], 'Failures / sec') +
        { fieldConfig+: { defaults+: { unit: 'percentunit', min: 0, max: 1 } } } +
        $.panelDescription(
          'Consistency checks failed',
          |||
            Rate of queries that had to run with consistency checks and those checks failed. A failed consistency check means that some of at least one block which had to be queried wasn't present in any of the store-gateways.
          |||
        ),
      )
    )
    .addRow(
      $.row('Notifications')
      .addPanel(
        $.timeseriesPanel('Notifications') +
        $.queryPanel([
          'sum(rate(cortex_prometheus_notifications_sent_total{%s}[$__rate_interval])) > 0' % $.jobMatcher($._config.job_names.ruler),
          'sum(rate(cortex_prometheus_notifications_errors_total{%s}[$__rate_interval])) > 0' % $.jobMatcher($._config.job_names.ruler),
          'sum(rate(cortex_prometheus_notifications_dropped_total{%s}[$__rate_interval])) > 0' % $.jobMatcher($._config.job_names.ruler),
        ], [
          'sent',
          'errors',
          'dropped',
        ]) +
        {
          fieldConfig+: {
            defaults+: {
              unit: 'reqps',
            },
          },
          options+: {
            tooltip: {
              mode: 'multi',
              sort: 'desc',
            },
          },
        } +
        $.panelDescription(
          'Notifications',
          |||
            Shows the absolute rate of notification outcomes:
            - Sent: Successfully delivered notifications
            - Errors: Notifications that encountered errors during delivery
            - Dropped: Notifications that were dropped from the sending queue because the queue is full
          |||
        )
      )
      .addPanel(
        $.timeseriesPanel('Undelivered notifications (per tenant)') +
        $.queryPanel([
          // Error notifications percentage
          |||
            sum by(user) (rate(cortex_prometheus_notifications_errors_total{%s}[$__rate_interval]))
            /
            (
              sum by(user) (rate(cortex_prometheus_notifications_sent_total{%s}[$__rate_interval]))
              +
              sum by(user) (rate(cortex_prometheus_notifications_dropped_total{%s}[$__rate_interval]))
            )
            > 0
          ||| % [
            $.jobMatcher($._config.job_names.ruler),
            $.jobMatcher($._config.job_names.ruler),
            $.jobMatcher($._config.job_names.ruler),
          ],
          // Dropped notifications percentage
          |||
            sum by(user) (rate(cortex_prometheus_notifications_dropped_total{%s}[$__rate_interval]))
            /
            (
              sum by(user) (rate(cortex_prometheus_notifications_sent_total{%s}[$__rate_interval]))
              +
              sum by(user) (rate(cortex_prometheus_notifications_dropped_total{%s}[$__rate_interval]))
            )
            > 0
          ||| % [
            $.jobMatcher($._config.job_names.ruler),
            $.jobMatcher($._config.job_names.ruler),
            $.jobMatcher($._config.job_names.ruler),
          ],
        ], [
          '{{user}} - errors',
          '{{user}} - dropped',
        ]) +
        { fieldConfig+: { defaults+: { unit: 'percentunit', min: 0, max: 1 } } } +
        $.panelDescription(
          'Undelivered notifications (per tenant)',
          |||
            Shows the percentage of notifications that resulted in errors or were dropped, per tenant:
            - Errors: Percentage of notifications that encountered errors during delivery
            - Dropped: Percentage of notifications that were dropped from the queue

            Both percentages are calculated as proportion of total notifications (sent + dropped).
          |||
        )
      )
      .addPanel(
        $.timeseriesPanel('Queue length') +
        $.queryPanel(|||
          sum by(user) (cortex_prometheus_notifications_queue_length{%s})
            /
          sum by(user) (cortex_prometheus_notifications_queue_capacity{%s}) > 0
        ||| % [$.jobMatcher($._config.job_names.ruler), $.jobMatcher($._config.job_names.ruler)], '{{ user }}') +
        { fieldConfig+: { defaults+: { unit: 'percentunit', min: 0, max: 1 } } }
      )
    )
    .addRow(
      ($.row('Group evaluations') + { collapse: true })
      .addPanel(
        $.timeseriesPanel('Missed iterations') +
        $.queryPanel('sum by(user) (rate(cortex_prometheus_rule_group_iterations_missed_total{%s}[$__rate_interval])) > 0' % $.jobMatcher($._config.job_names.ruler), '{{ user }}'),
      )
      .addPanel(
        $.timeseriesPanel('Latency') +
        $.queryPanel(
          |||
            rate(cortex_prometheus_rule_group_duration_seconds_sum{%s}[$__rate_interval])
              /
            rate(cortex_prometheus_rule_group_duration_seconds_count{%s}[$__rate_interval])
          ||| % [$.jobMatcher($._config.job_names.ruler), $.jobMatcher($._config.job_names.ruler)],
          '{{ user }}'
        ) +
        { fieldConfig+: { defaults+: { unit: 's' } } },
      )
      .addPanel(
        $.timeseriesPanel('Failures') +
        $.queryPanel(
          'sum by(rule_group) (rate(cortex_prometheus_rule_evaluation_failures_total{%s}[$__rate_interval])) > 0' % [$.jobMatcher($._config.job_names.ruler)], '{{ rule_group }}'
        )
      )
    )
    .addRow(
      ($.row('Rule evaluation per user') + { collapse: true })
      .addPanel(
        $.timeseriesPanel('Latency') +
        $.queryPanel(
          |||
            sum by(user) (rate(cortex_prometheus_rule_evaluation_duration_seconds_sum{%s}[$__rate_interval]))
              /
            sum by(user) (rate(cortex_prometheus_rule_evaluation_duration_seconds_count{%s}[$__rate_interval]))
          ||| % [$.jobMatcher($._config.job_names.ruler), $.jobMatcher($._config.job_names.ruler)],
          '{{ user }}'
        ) +
        { fieldConfig+: { defaults+: { unit: 's' } } },
      )
    )
    .addRows(
      $.getObjectStoreRows('Ruler configuration object store (ruler accesses)', 'ruler-storage')
    ),
}
