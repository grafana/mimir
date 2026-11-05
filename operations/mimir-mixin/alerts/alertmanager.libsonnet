(import 'alerts-utils.libsonnet') {
  local alertGroups = [
    {
      name: 'alertmanager_alerts',
      rules: [
        {
          alert: $.alertName('AlertmanagerSyncConfigsFailing'),
          expr: |||
            rate(cortex_alertmanager_sync_configs_failed_total[%s]) > 0
          ||| % $.rateInterval('5m'),
          'for': '30m',
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: |||
              %(product)s Alertmanager {{ $labels.%(per_job_label)s }}/%(alert_instance_variable)s is failing to read tenant configurations from storage.
            ||| % $._config,
          },
        },
        {
          alert: $.alertName('AlertmanagerRingCheckFailing'),
          expr: |||
            rate(cortex_alertmanager_ring_check_errors_total[%s]) > 0
          ||| % $.rateInterval('2m'),
          'for': '10m',
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: |||
              %(product)s Alertmanager {{ $labels.%(per_job_label)s }}/%(alert_instance_variable)s is unable to check tenants ownership via the ring.
            ||| % $._config,
          },
        },
        {
          alert: $.alertName('AlertmanagerStateMergeFailing'),
          expr: |||
            rate(cortex_alertmanager_partial_state_merges_failed_total[%s]) > 0
          ||| % $.rateInterval('2m'),
          'for': '10m',
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: |||
              %(product)s Alertmanager {{ $labels.%(per_job_label)s }}/%(alert_instance_variable)s is failing to merge partial state changes received from a replica.
            ||| % $._config,
          },
        },
        {
          alert: $.alertName('AlertmanagerReplicationFailing'),
          expr: |||
            rate(cortex_alertmanager_state_replication_failed_total[%s]) > 0
          ||| % $.rateInterval('2m'),
          'for': '10m',
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: |||
              %(product)s Alertmanager {{ $labels.%(per_job_label)s }}/%(alert_instance_variable)s is failing to replicating partial state to its replicas.
            ||| % $._config,
          },
        },
        // Each tenant persists its state once per -alertmanager.persist-interval (15m by default),
        // and only on the replica at ring position 0. The counters are summed over the tenants
        // of a pod, so both alerts are per pod.
        {
          // The last 3 or 4 persist attempts of each tenant on the pod failed. The expression holds
          // while the last 20m (one persist interval plus slack) had failures and no success, and
          // `for` requires that for 35m (3rd attempt after the first observed failure, plus slack). A
          // range window alone would fire on the first failed cycle of a new series (e.g. a new pod,
          // or one that just started persisting), because increase() only sees samples that exist.
          // When the failed counter was 0 before the outage, increase() doesn't extrapolate it below
          // zero while it does extrapolate the total, so the success term stays above 0.5 until the
          // last pre-outage sample leaves the window: the alert then fires after the 4th attempt.
          // Resolves on the first successful persist.
          alert: $.alertName('AlertmanagerPersistStateFailing'),
          expr: |||
            increase(cortex_alertmanager_state_persist_failed_total[%(window)s]) > 0
            unless
            (increase(cortex_alertmanager_state_persist_total[%(window)s]) - increase(cortex_alertmanager_state_persist_failed_total[%(window)s])) > 0.5
          ||| % { window: $.rateInterval('20m') },
          'for': '35m',
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: |||
              %(product)s Alertmanager {{ $labels.%(per_job_label)s }}/%(alert_instance_variable)s has failed every attempt to persist full state snapshots to remote storage for at least 3 persist intervals.
            ||| % $._config,
          },
        },
        {
          // Persists keep failing intermittently: more than 5% of attempts failed over the last 6h,
          // with failures in at least 4 of the last 6 hours, so that a single outage (covered by the
          // critical alert) does not also raise this one for the next 6h.
          alert: $.alertName('AlertmanagerPersistStateFailing'),
          expr: |||
            (
              increase(cortex_alertmanager_state_persist_failed_total[%(long_window)s])
              /
              increase(cortex_alertmanager_state_persist_total[%(long_window)s])
            ) > 0.05
            and
            count_over_time((increase(cortex_alertmanager_state_persist_failed_total[%(short_window)s]) > 0)[%(long_window)s:%(step)s]) >= 4
          ||| % {
            long_window: $.rateInterval('6h'),
            short_window: $.rateInterval('1h'),
            step: $.stepInterval('1h'),
          },
          'for': '1h',
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: |||
              %(product)s Alertmanager {{ $labels.%(per_job_label)s }}/%(alert_instance_variable)s failed {{ $value | humanizePercentage }} of its attempts to persist full state snapshots to remote storage over the last 6h.
            ||| % $._config,
          },
        },
        {
          alert: $.alertName('AlertmanagerInitialSyncFailed'),
          expr: |||
            increase(cortex_alertmanager_state_initial_sync_completed_total{outcome="failed"}[%s]) > 0
          ||| % $.rateInterval('1m'),
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: |||
              %(product)s Alertmanager {{ $labels.%(per_job_label)s }}/%(alert_instance_variable)s was unable to obtain some initial state when starting up.
            ||| % $._config,
          },
        },
        {
          alert: $.alertName('AlertmanagerAllocatingTooMuchMemory'),
          expr: $._config.alertmanager_alerts[$._config.deployment_type].memory_allocation % $._config { threshold: '0.80' },
          'for': '15m',
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: |||
              Alertmanager %(alert_instance_variable)s in %(alert_aggregation_variables)s is using too much memory.
            ||| % $._config,
          } + $.dashboardURLAnnotation('mimir-scaling.json'),
        },
        {
          alert: $.alertName('AlertmanagerAllocatingTooMuchMemory'),
          expr: $._config.alertmanager_alerts[$._config.deployment_type].memory_allocation % $._config { threshold: '0.90' },
          'for': '15m',
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: |||
              Alertmanager %(alert_instance_variable)s in %(alert_aggregation_variables)s is using too much memory.
            ||| % $._config,
          } + $.dashboardURLAnnotation('mimir-scaling.json'),
        },
        {
          alert: $.alertName('AlertmanagerInstanceHasNoTenants'),
          expr: |||
            # Alert on alertmanager instances in microservices mode that own no tenants,
            min by(%(alert_aggregation_labels)s, %(per_instance_label)s) (cortex_alertmanager_tenants_owned{%(per_instance_label)s=~"%(alertmanagerInstanceName)s"}) == 0
            # but only if other instances of the same cell do have tenants assigned.
            and on (%(alert_aggregation_labels)s)
            max by(%(alert_aggregation_labels)s) (cortex_alertmanager_tenants_owned) > 0
          ||| % {
            alert_aggregation_labels: $._config.alert_aggregation_labels,
            per_instance_label: $._config.per_instance_label,
            alertmanagerInstanceName: $._config.instance_names.alertmanager,
          },
          'for': '1h',
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: '%(product)s alertmanager %(alert_instance_variable)s in %(alert_aggregation_variables)s owns no tenants.' % $._config,
          },
        },
      ],
    },
  ],

  groups+:
    $.withRunbookURL(
      'https://grafana.com/docs/mimir/latest/manage/mimir-runbooks/#%s',
      $.withDashboardURL(
        'mimir-alertmanager.json',
        $.withExtraLabelsAnnotations(alertGroups)
      )
    ),
}
