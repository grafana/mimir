local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-compactor.json';

// This applies to the "longest time since successful run queries"
local fixTargetsForTransformations(panel, refIds) = panel {
  // Make some adjustments to the targets to make them compatible with the transformations required
  targets: [
    panel.targets[i] {
      refId: refIds[i],
      format: 'table',
      instant: true,
    }
    for i in std.range(0, std.length(refIds) - 1)
  ],
};

(import 'dashboard-utils.libsonnet') {

  local lastRunThresholds = {

    local secondsPerHour = 60 * 60,

    // In terms of hours
    local delayed = 2 * secondsPerHour,
    local late = 6 * secondsPerHour,
    local veryLate = 12 * secondsPerHour,

    // steps for thresholds
    steps: [
      { value: 0, color: 'green' },
      { value: delayed, color: 'yellow' },
      { value: late, color: 'orange' },
      { value: veryLate, color: 'red' },
    ],

    // status mappings: messages and colors
    mappings: [
      $.mappingRange('-Infinity', 0, { color: 'transparent', text: 'N/A' }),
      $.mappingRange(0, delayed, { color: 'green', text: 'Ok' }),
      $.mappingRange(delayed, late, { color: 'yellow', text: 'Delayed' }),
      $.mappingRange(late, veryLate, { color: 'orange', text: 'Late' }),
      $.mappingRange(veryLate, 'Infinity', { color: 'red', text: 'Very late' }),
      $.mappingSpecial('null+nan', { color: 'transparent', text: 'Unknown' }),
    ],

    descriptions: |||
      The value in the status column is based on how long it has been since the last successful compaction.

      - Okay: less than %(delayed)s hours
      - Delayed: more than %(delayed)s hours
      - Late: more than %(late)s hours
      - Very late: more than %(veryLate)s hours
    ||| % {
      delayed: delayed / secondsPerHour,
      late: late / secondsPerHour,
      veryLate: veryLate / secondsPerHour,
    },
  },

  local lastRunQuery =
    |||
      max by(%(instance)s)
      (
        (time() * (max_over_time(cortex_compactor_last_successful_run_timestamp_seconds{%(job)s}[1h]) !=bool 0))
        -
        max_over_time(cortex_compactor_last_successful_run_timestamp_seconds{%(job)s}[1h])
      )
    ||| % {
      instance: $._config.per_instance_label,
      job: $.jobMatcher($._config.job_names.compactor),
    },

  local lastRunCommonTransformations = [
    $.transformation('organize', {
      renameByName: {
        Value: 'Last run',
        ['%s' % $._config.per_instance_label]: 'Compactor',
      },
    }),
    $.transformation('sortBy', {
      sort: [
        {
          desc: true,
          field: 'Last run',
        },
      ],
    }),
  ],

  [filename]:
    assert std.md5(filename) == '1b3443aea86db629e6efdb7d05c53823' : 'UID of the dashboard has changed, please update references to dashboard.';
    ($.dashboard('Compactor') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates()
    .addRow(
      $.row('Summary')
      .addPanel(
        $.startedCompletedFailedPanel(
          'Per-instance runs / sec',
          'sum(rate(cortex_compactor_runs_started_total{%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.compactor),
          'sum(rate(cortex_compactor_runs_completed_total{%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.compactor),
          'sum(rate(cortex_compactor_runs_failed_total{%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.compactor)
        ) +
        $.bars +
        { fieldConfig+: { defaults+: { unit: 'ops' } } } +
        $.panelDescription(
          'Per-instance runs',
          |||
            Number of times a compactor instance triggers a compaction across all tenants that it manages.
          |||
        ),
      )
      .addPanel(
        $.timeseriesPanel('Tenants compaction progress') +
        $.queryPanel(
          |||
            (
              cortex_compactor_tenants_processing_succeeded{%(job)s} +
              cortex_compactor_tenants_processing_failed{%(job)s} +
              cortex_compactor_tenants_skipped{%(job)s}
            )
            /
            cortex_compactor_tenants_discovered{%(job)s} > 0
          ||| % {
            job: $.jobMatcher($._config.job_names.compactor),
          },
          '{{%s}}' % $._config.per_instance_label
        ) +
        { fieldConfig: { defaults: { unit: 'percentunit', max: 1, noValue: 1 } } } +
        $.panelDescription(
          'Tenants compaction progress',
          |||
            In a multi-tenant cluster, display the progress of tenants that are compacted while compaction is running.
          |||
        ),
      )
      .addPanel(
        $.panel('Longest time since last successful run') +
        $.panelDescription(
          'Longest time since last successful run',
          |||
            Displays the amount of time since the most recent successful execution
            of the compactor.
            The value shown will be for the compactor replica that has the longest time since its
            last successful run.
            The table to the right shows a summary for all compactor replicas.

            If there is no time value, one of the following messages might appear:

            - If you see "No compactor data" in this panel, that means that no compactors are active yet.

            - If you see "No successful runs" in this panel, that means that compactors are active, but none
              of them were successfully executed yet.

            These might be expected - for example, if you just recently restarted your compactors,
            they might not have had a chance to complete their first compaction run.
            However, if these messages persist, you should check the health of your compactors.
          |||
        ) +
        $.newStatPanel(lastRunQuery, unit='s') + {
          options: {
            reduceOptions: {
              values: false,
              calcs: ['first'],
              fields: '/^Last run$/',
            },
            textMode: 'value',
          },
          targets: [target { format: 'table', instant: true } for target in super.targets],
          transformations: lastRunCommonTransformations,
          fieldConfig: super.fieldConfig + {
            defaults: super.defaults {
              noValue: 'No compactor data',
            },
            overrides: [
              $.overrideFieldByName('Last run', [
                $.overrideProperty('custom.width', 74),
                $.overrideProperty('mappings', [
                  $.mappingRange('-Infinity', 0, { color: 'text', text: 'No successful runs since startup yet' }),
                ]),
                $.overrideProperty('color', { mode: 'thresholds' }),
                $.overrideProperty('thresholds', { mode: 'absolute', steps: lastRunThresholds.steps }),
              ]),
            ],
          },
        },
      )
      .addPanel(
        $.timeseriesPanel('Last successful run per-compactor replica') +
        $.panelDescription(
          'Last successful run per-compactor replica',
          |||
            Displays the compactor replicas, and for each, shows how long it has been since
            its last successful compaction run.

            %(thresholdDescriptions)s
            If the status of any compactor replicas are *Late* or *Very late*, check their health.
          ||| % {
            thresholdDescriptions: lastRunThresholds.descriptions,
          },
        ) +
        $.queryPanel(lastRunQuery, 'Last run') {
          type: 'table',
          targets: [target { format: 'table', instant: true } for target in super.targets],
          transformations:
            lastRunCommonTransformations +
            [
              // Grafana 8.5+ does not support constant numbers (e.g., 1), so we make a "One" field
              $.transformationCalculateField('One', 'Last run', '/', 'Last run'),
              // Duplicate field of "Last run" to provide "Status" text based on lastRunThresholds.mappings
              $.transformationCalculateField('Status', 'Last run', '*', 'One'),
              $.transformation('filterFieldsByName', {
                include: {  // Only include these fields in the display
                  names: ['Compactor', 'Last run', 'Status'],
                },
              }),
            ],
          fieldConfig: {
            overrides: [
              $.overrideFieldByName('Status', [
                $.overrideProperty('custom.displayMode', 'color-background'),
                $.overrideProperty('mappings', lastRunThresholds.mappings),
                $.overrideProperty('custom.width', 86),
                $.overrideProperty('custom.align', 'center'),
              ]),
              $.overrideFieldByName('Last run', [
                $.overrideProperty('unit', 's'),
                $.overrideProperty('custom.width', 74),
                $.overrideProperty('mappings', [
                  $.mappingRange('-Infinity', 0, { text: 'Never' }),
                ]),
              ]),
            ],
          },
        },
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.timeseriesPanel('Estimated Compaction Jobs') +
        $.queryPanel('sum(cortex_bucket_index_estimated_compaction_jobs{%s}) and (sum(rate(cortex_bucket_index_estimated_compaction_jobs_errors_total{%s}[$__rate_interval])) == 0)' %
                     [$.jobMatcher($._config.job_names.compactor), $.jobMatcher($._config.job_names.compactor)], 'Jobs') +
        $.panelDescription(
          'Estimated Compaction Jobs',
          |||
            Estimated number of compaction jobs based on latest version of bucket index. Ingesters upload new blocks every 2 hours (shortly after 01:00 UTC, 03:00 UTC, 05:00 UTC, etc.),
            and compactors should process all of them within 2h interval. If this graph regularly goes to zero (or close to zero) in 2 hour intervals, then compaction works as designed.

            Metric with number of compaction jobs is computed from blocks in bucket index, which is updated regularly. Metric doesn't change between bucket index updates, even if
            there were compaction jobs finished in this time. When computing compaction jobs, only jobs that can be executed at given moment are counted. There can be more
            jobs, but if they are blocked, they are not counted in the metric. For example if there is a split compaction job pending for some time range, no merge job
            covering the same time range can run. In this case only split compaction job is counted toward the metric, but merge job isn't.

            In other words, computed number of compaction jobs is the minimum number of compaction jobs based on latest version of bucket index.
          |||
        ),
      )
      .addPanel(
        $.timeseriesPanel('TSDB compactions / sec') +
        $.queryPanel('sum(rate(prometheus_tsdb_compactions_total{%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.compactor), 'compactions') +
        { fieldConfig+: { defaults+: { unit: 'ops' } } } +
        $.panelDescription(
          'TSDB compactions / sec',
          |||
            Rate of TSDB compactions. Single TSDB compaction takes one or more input blocks and produces one or more (during "split" phase) output blocks.
          |||
        ),
      )
      .addPanel(
        $.timeseriesPanel('TSDB compaction duration') +
        $.latencyPanel('prometheus_tsdb_compaction_duration_seconds', '{%s}' % $.jobMatcher($._config.job_names.compactor)) +
        $.panelDescription(
          'TSDB compaction duration',
          |||
            Display the amount of time that it has taken to run a single TSDB compaction.
          |||
        ),
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.timeseriesPanel('Average blocks / tenant') +
        $.queryPanel('avg(max by(user) (cortex_bucket_blocks_count{%s}))' % $.jobMatcher($._config.job_names.compactor), 'avg'),
      )
      .addPanel(
        $.timeseriesPanel('Tenants with largest number of blocks') +
        $.queryPanel('topk(10, max by(user) (cortex_bucket_blocks_count{%s}))' % $.jobMatcher($._config.job_names.compactor), '{{user}}') +
        $.panelDescription(
          'Tenants with largest number of blocks',
          |||
            The 10 tenants with the largest number of blocks.
          |||
        ),
      )
    )
    .addRow(
      $.row('Garbage collector')
      .addPanel(
        $.timeseriesPanel('Blocks marked for deletion / sec') +
        $.queryPanel(
          |||
            sum(rate(cortex_compactor_blocks_marked_for_deletion_total{%(job)s}[$__rate_interval]))
          ||| % {
            job: $.jobMatcher($._config.job_names.compactor),
          },
          'blocks'
        ) +
        { fieldConfig+: { defaults+: { unit: 'ops' } } }
      )
      .addPanel(
        $.timeseriesPanel('Blocks deletions / sec') +
        $.successFailurePanel(
          // The cortex_compactor_blocks_cleaned_total tracks the number of successfully
          // deleted blocks.
          |||
            sum(rate(cortex_compactor_blocks_cleaned_total{%(job)s}[$__rate_interval]))
          ||| % {
            job: $.jobMatcher($._config.job_names.compactor),
          },
          |||
            sum(rate(cortex_compactor_block_cleanup_failures_total{%(job)s}[$__rate_interval]))
          ||| % {
            job: $.jobMatcher($._config.job_names.compactor),
          },
        ) +
        $.stack +
        { fieldConfig+: { defaults+: { unit: 'ops' } } }
      )
    )
    .addRow(
      $.row('Metadata sync')
      .addPanel(
        $.timeseriesPanel('Metadata syncs / sec') +
        $.successFailurePanel(
          // The cortex_compactor_meta_syncs_total metric is incremented each time a per-tenant
          // metadata sync is triggered.
          |||
            sum(rate(cortex_compactor_meta_syncs_total{%(job)s}[$__rate_interval]))
            -
            sum(rate(cortex_compactor_meta_sync_failures_total{%(job)s}[$__rate_interval]))
          ||| % {
            job: $.jobMatcher($._config.job_names.compactor),
          },
          |||
            sum(rate(cortex_compactor_meta_sync_failures_total{%(job)s}[$__rate_interval]))
          ||| % {
            job: $.jobMatcher($._config.job_names.compactor),
          },
        ) +
        $.stack +
        { fieldConfig+: { defaults+: { unit: 'ops' } } }
      )
      .addPanel(
        $.timeseriesPanel('Metadata sync duration') +
        // This metric tracks the duration of a per-tenant metadata sync.
        $.latencyPanel('cortex_compactor_meta_sync_duration_seconds', '{%s}' % $.jobMatcher($._config.job_names.compactor)),
      )
    )
    .addRows($.getObjectStoreRows('Object Store', 'compactor'))
    .addRow(
      $.kvStoreRow('Key-value store for compactors ring', 'compactor', '.+')
    )
    .addRowIf(
      $._config.autoscaling.compactor.enabled,
      $.cpuBasedAutoScalingRow('Compactor'),
    ),
}
