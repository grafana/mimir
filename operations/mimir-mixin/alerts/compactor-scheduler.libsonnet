(import 'alerts-utils.libsonnet') {
  local alertGroups = [
    {
      name: 'mimir_compactor_scheduler_alerts',
      rules: [
        {
          // Alert if a scheduler-mode worker has not contacted the scheduler recently.
          alert: $.alertName('CompactorSchedulerUnreachable'),
          'for': '5m',
          expr: |||
            (time() - max by(%(alert_aggregation_labels)s, %(per_instance_label)s) (cortex_compactor_last_scheduler_contact_timestamp_seconds) > 15 * 60)
            and
            (max by(%(alert_aggregation_labels)s, %(per_instance_label)s) (cortex_compactor_last_scheduler_contact_timestamp_seconds) > 0)
          ||| % $._config,
          labels: {
            severity: 'critical',
            reason: 'not-contacted-recently',
          },
          annotations: {
            message: '%(product)s Compactor %(alert_instance_variable)s in %(alert_aggregation_variables)s has not contacted the scheduler in the last 15 minutes.' % $._config,
          },
        },
        {
          // Alert if a scheduler-mode worker has never contacted the scheduler since startup.
          alert: $.alertName('CompactorSchedulerUnreachable'),
          'for': '15m',
          expr: |||
            (max by(%(alert_aggregation_labels)s, %(per_instance_label)s) (cortex_compactor_last_scheduler_contact_timestamp_seconds) == 0)
          ||| % $._config,
          labels: {
            severity: 'critical',
            reason: 'since-startup',
          },
          annotations: {
            message: '%(product)s Compactor %(alert_instance_variable)s in %(alert_aggregation_variables)s has not contacted the scheduler since startup.' % $._config,
          },
        },
        {
          // Alert if the scheduler has recorded repeated job failures.
          // A scheduler is deployed per read compartment, so keep the compartment to know which one to look at.
          alert: $.alertName('CompactorSchedulerRepeatedJobFailure'),
          expr: |||
            sum by(%(alert_aggregation_labels)s, read_compartment) (
              %(repeated_failures)s
            ) > 0
          ||| % $._config {
            repeated_failures: $.withReadCompartmentLabel('increase(cortex_compactor_scheduler_repeated_job_failures_total[%s])' % $.rateInterval('5m')),
          },
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: '%(product)s Compactor scheduler in %(alert_aggregation_variables)s{{ if $labels.read_compartment }} read compartment {{ $labels.read_compartment }}{{ end }} has jobs failing repeatedly.' % $._config,
          },
        },
        {
          // Alert if the scheduler is not completing any jobs.
          // A scheduler is deployed per read compartment, so sum per compartment: otherwise a scheduler that
          // completes jobs hides a stalled one.
          alert: $.alertName('CompactorSchedulerNotCompletingJobs'),
          'for': '30m',
          expr: |||
            sum by(%(alert_aggregation_labels)s, read_compartment) (
              %(jobs_completed)s
            ) == 0
          ||| % $._config {
            jobs_completed: $.withReadCompartmentLabel('increase(cortex_compactor_scheduler_jobs_completed_total[%s])' % $.rateInterval('6h')),
          },
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: '%(product)s Compactor scheduler in %(alert_aggregation_variables)s{{ if $labels.read_compartment }} read compartment {{ $labels.read_compartment }}{{ end }} is not completing any jobs.' % $._config,
          },
        },
      ],
    },
  ],

  groups+:
    $.withRunbookURL(
      'https://grafana.com/docs/mimir/latest/operators-guide/mimir-runbooks/#%s',
      $.withDashboardURL(
        'mimir-compactor.json',
        $.withExtraLabelsAnnotations(alertGroups)
      )
    ),
}
