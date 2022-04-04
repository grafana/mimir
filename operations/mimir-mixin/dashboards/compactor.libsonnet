local utils = import 'mixin-utils/utils.libsonnet';

// Panel query override functions
local overrideFieldByName(fieldName, overrideProperties) = {
  matcher: {
    id: 'byName',
    options: fieldName,
  },
  properties: overrideProperties,
};

local overrideProperty(id, value) = { id: id, value: value };

// Panel query value mapping functions
local mappingRange(from, to, result) = {
  type: 'range',
  options: {
    from: from,
    to: to,
    result: result,
  },
};

local mappingSpecial(match, result) = {
  type: 'special',
  options: {
    match: match,
    result: result,
  },
};

// Panel query transformation functions

local transformation(id, options={}) = { id: id, options: options };

local transformationCalculateField(alias, left, operator, right, replaceFields=false) =
  transformation('calculateField', {
    alias: alias,
    binary: {
      left: left,
      operator: operator,
      right: right,
    },
    mode: 'binary',
    replaceFields: replaceFields,
  });


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

  local vars = {
    instance: $._config.per_instance_label,
  },

  local lastRunQueries = {
    queries: [
      // Last run - (will be exactly zero if no successful run)
      |||
        max by(%(instance)s)
        (
          (time() * (cortex_compactor_last_successful_run_timestamp_seconds !=bool 0))
          - 
          cortex_compactor_last_successful_run_timestamp_seconds
        )
      ||| % (vars),
      // Interval
      |||
        max by (%(instance)s) 
        (
          cortex_compactor_compaction_interval_seconds
        )
      ||| % (vars),
    ],
    refIds: [
      'Last run',
      'Interval',
    ],
  },

  local lastRunCommonTransformations = [
    transformation('organize', {
      renameByName: {
        'Value #Interval': 'Interval',
        'Value #Last run': 'Last run',
        ['%s' % vars.instance]: 'Compactor',
      },
    }),
    transformationCalculateField('Status', 'Last run', '/', 'Interval'),
    transformation('sortBy', {
      sort: [
        {
          desc: true,
          field: 'Status',
        },
      ],
    }),
  ],

  lastRunStatPanel():: (
    local panel =
      $.newStatPanel(lastRunQueries.queries, lastRunQueries.refIds, unit='s') + {
        options: {
          reduceOptions: {
            values: false,
            calcs: ['first'],
            fields: '/^Last run$/',
          },
          textMode: 'value',
        },
        transformations:
          [transformation('merge')] +
          lastRunCommonTransformations +
          [  
            // Not a visible field, but used as a max to determine what the "Red" value is in GrYlRd
            // This corresponds to the "Very late" mapping range in `lastRunTablePanel` 
            transformationCalculateField('Maximum of threshold', 'Interval', '*', '9'),
          ],
        fieldConfig: super.fieldConfig + {
          defaults: super.defaults {
            noValue: 'No compactor data',
          },
          overrides: [
            overrideFieldByName('Last run', [
              overrideProperty('custom.width', 74),
              overrideProperty('mappings', [
                mappingRange('-Infinity', 0, { color: 'text', text: 'No successful runs' }),
              ]),
              overrideProperty('color', { mode: 'continuous-GrYlRd' }), // Green is zero, red is `Maximum of threshold`
            ]),
          ],
        },
      };
    fixTargetsForTransformations(panel, lastRunQueries.refIds)
  ),

  lastRunTablePanel():: (
    local panel =
      $.queryPanel(lastRunQueries.queries, lastRunQueries.refIds) + {
        type: 'table',
        transformations:
          [{ id: 'seriesToColumns', options: { byField: 'instance' } }] +
          lastRunCommonTransformations +
          [
            transformation('filterFieldsByName', {
              include: {  // Only include these fields in the display
                names: ['Compactor', 'Last run', 'Status'],
              },
            }),
            transformation('filterByValue', {
              filters: [{ fieldName: 'Last run', config: { id: 'isNull' } }],
              type: 'exclude',
              match: 'any',
            }),
          ],
        fieldConfig: {
          overrides: [
            overrideFieldByName('Status', [
              overrideProperty('custom.displayMode', 'color-background'),
              overrideProperty('mappings', [
                mappingRange('-Infinity', 0, { color: 'transparent', text: 'N/A' }),
                // These numbers are multiples of the compaction interval
                // Default interval 1hr, so 0hrs to 3hrs is okay, 3hrs to 6hrs is Delayed, etc.
                mappingRange(0, 3, { color: 'green', text: 'Ok' }),
                mappingRange(3, 6, { color: 'yellow', text: 'Delayed' }),
                mappingRange(6, 9, { color: 'orange', text: 'Late' }),
                mappingRange(9, 'Infinity', { color: 'red', text: 'Very late' }),
                mappingSpecial('null+nan', { color: 'transparent', text: 'Unknown' }),
              ]),
              overrideProperty('custom.width', 86),
              overrideProperty('custom.align', 'center'),
            ]),
            overrideFieldByName('Last run', [
              overrideProperty('unit', 's'),
              overrideProperty('custom.width', 74),
              overrideProperty('mappings', [
                mappingRange('-Infinity', 0, { text: 'Never' }),
              ]),
            ]),
          ],
        },
      };
    fixTargetsForTransformations(panel, lastRunQueries.refIds)
  ),
  'mimir-compactor.json':
    ($.dashboard('Compactor') + { uid: '9c408e1d55681ecb8a22c9fab46875cc' })
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
        { yaxes: $.yaxes('ops') } +
        $.panelDescription(
          'Per-instance runs',
          |||
            Number of times a compactor instance triggers a compaction across all tenants that it manages.
          |||
        ),
      )
      .addPanel(
        $.panel('Tenants compaction progress') +
        $.queryPanel(|||
          (
            cortex_compactor_tenants_processing_succeeded{%s} +
            cortex_compactor_tenants_processing_failed{%s} +
            cortex_compactor_tenants_skipped{%s}
          ) / cortex_compactor_tenants_discovered{%s}
        ||| % [$.jobMatcher($._config.job_names.compactor), $.jobMatcher($._config.job_names.compactor), $.jobMatcher($._config.job_names.compactor), $.jobMatcher($._config.job_names.compactor)], '{{%s}}' % $._config.per_instance_label) +
        { yaxes: $.yaxes({ format: 'percentunit', max: 1 }) } +
        $.panelDescription(
          'Tenants compaction progress',
          |||
            In a multi-tenant cluster, display the progress of tenants that are compacted while compaction is running.
            Reset to <tt>0</tt> after the compaction run is completed for all tenants in the shard.
          |||
        ),
      )
      .addPanel(
        $.panel('Longest time since last successful run') +
        $.panelDescription(
          'Longest time since last successful run',
          |||
            Displays the amount of time since the most recent successful execution
            of the compactor replica with the longest delay (per compaction interval).
          |||
        ) +
        $.lastRunStatPanel()
      )
      .addPanel(
        $.panel('Last successful run per-compactor replica') +
        $.panelDescription(
          'Last successful run per-compactor replica',
          |||
            Displays the compactor replicas, and for each, indicate the status of their
            most recent compaction in terms of how long it has been since that compactor
            was successfully executed, along with its compaction interval.
          |||
        ) +
        $.lastRunTablePanel()
      )
    )
    .addRow(
      $.row('')
      .addPanel(
        $.panel('TSDB compactions / sec') +
        $.queryPanel('sum(rate(prometheus_tsdb_compactions_total{%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.compactor), 'compactions') +
        { yaxes: $.yaxes('ops') } +
        $.panelDescription(
          'TSDB compactions / sec',
          |||
            Rate of TSDB compactions. Single TSDB compaction takes one or more input blocks and produces one or more (during "split" phase) output blocks.
          |||
        ),
      )
      .addPanel(
        $.panel('TSDB compaction duration') +
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
        $.panel('Average blocks / tenant') +
        $.queryPanel('avg(max by(user) (cortex_bucket_blocks_count{%s}))' % $.jobMatcher($._config.job_names.compactor), 'avg'),
      )
      .addPanel(
        $.panel('Tenants with largest number of blocks') +
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
        $.panel('Blocks marked for deletion / sec') +
        $.queryPanel('sum(rate(cortex_compactor_blocks_marked_for_deletion_total{%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.compactor), 'blocks') +
        { yaxes: $.yaxes('ops') },
      )
      .addPanel(
        $.successFailurePanel(
          'Blocks deletions / sec',
          // The cortex_compactor_blocks_cleaned_total tracks the number of successfully
          // deleted blocks.
          'sum(rate(cortex_compactor_blocks_cleaned_total{%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.compactor),
          'sum(rate(cortex_compactor_block_cleanup_failures_total{%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.compactor),
        ) + { yaxes: $.yaxes('ops') }
      )
    )
    .addRow(
      $.row('Metadata sync')
      .addPanel(
        $.successFailurePanel(
          'Metadata syncs / sec',
          // The cortex_compactor_meta_syncs_total metric is incremented each time a per-tenant
          // metadata sync is triggered.
          'sum(rate(cortex_compactor_meta_syncs_total{%s}[$__rate_interval])) - sum(rate(cortex_compactor_meta_sync_failures_total{%s}[$__rate_interval]))' % [$.jobMatcher($._config.job_names.compactor), $.jobMatcher($._config.job_names.compactor)],
          'sum(rate(cortex_compactor_meta_sync_failures_total{%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.compactor),
        ) + { yaxes: $.yaxes('ops') }
      )
      .addPanel(
        $.panel('Metadata sync duration') +
        // This metric tracks the duration of a per-tenant metadata sync.
        $.latencyPanel('cortex_compactor_meta_sync_duration_seconds', '{%s}' % $.jobMatcher($._config.job_names.compactor)),
      )
    )
    .addRows($.getObjectStoreRows('Object Store', 'compactor'))
    .addRow(
      $.kvStoreRow('Key-value store for compactors ring', 'compactor', '.+')
    ),
}
