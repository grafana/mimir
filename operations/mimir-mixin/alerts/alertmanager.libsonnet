(import 'alerts-utils.libsonnet') {
  local alertGroups = [
    {
      name: 'alertmanager_alerts',
      rules: [
        {
          alert: $.alertName('AlertmanagerSyncConfigsFailing'),
          expr: |||
            rate(cortex_alertmanager_sync_configs_failed_total[5m]) > 0
          |||,
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
            rate(cortex_alertmanager_ring_check_errors_total[2m]) > 0
          |||,
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
          alert: $.alertName('AlertmanagerPartialStateMergeFailing'),
          expr: |||
            rate(cortex_alertmanager_partial_state_merges_failed_total[2m]) > 0
          |||,
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
            rate(cortex_alertmanager_state_replication_failed_total[2m]) > 0
          |||,
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
        {
          alert: $.alertName('AlertmanagerPersistStateFailing'),
          expr: |||
            rate(cortex_alertmanager_state_persist_failed_total[15m]) > 0
          |||,
          'for': '1h',
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: |||
              %(product)s Alertmanager {{ $labels.%(per_job_label)s }}/%(alert_instance_variable)s is unable to persist full state snaphots to remote storage.
            ||| % $._config,
          },
        },
        {
          alert: $.alertName('AlertmanagerInitialSyncFailed'),
          expr: |||
            increase(cortex_alertmanager_state_initial_sync_completed_total{outcome="failed"}[1m]) > 0
          |||,
          labels: {
            severity: 'critical',
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
          },
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
          },
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

  groups+: $.withRunbookURL('https://grafana.com/docs/mimir/latest/operators-guide/mimir-runbooks/#%s', alertGroups),
}
