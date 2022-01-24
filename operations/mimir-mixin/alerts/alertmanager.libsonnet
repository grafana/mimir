{
  groups+: [
    {
      name: 'alertmanager_alerts',
      rules: [
        {
          alert: 'CortexAlertmanagerSyncConfigsFailing',
          expr: |||
            rate(cortex_alertmanager_sync_configs_failed_total[5m]) > 0
          |||,
          'for': '30m',
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: |||
              %(product)s Alertmanager {{ $labels.job }}/{{ $labels.instance }} is failing to read tenant configurations from storage.
            ||| % $._config,
          },
        },
        {
          alert: 'CortexAlertmanagerRingCheckFailing',
          expr: |||
            rate(cortex_alertmanager_ring_check_errors_total[2m]) > 0
          |||,
          'for': '10m',
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: |||
              %(product)s Alertmanager {{ $labels.job }}/{{ $labels.instance }} is unable to check tenants ownership via the ring.
            ||| % $._config,
          },
        },
        {
          alert: 'CortexAlertmanagerPartialStateMergeFailing',
          expr: |||
            rate(cortex_alertmanager_partial_state_merges_failed_total[2m]) > 0
          |||,
          'for': '10m',
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: |||
              %(product)s Alertmanager {{ $labels.job }}/{{ $labels.instance }} is failing to merge partial state changes received from a replica.
            ||| % $._config,
          },
        },
        {
          alert: 'CortexAlertmanagerReplicationFailing',
          expr: |||
            rate(cortex_alertmanager_state_replication_failed_total[2m]) > 0
          |||,
          'for': '10m',
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: |||
              %(product)s Alertmanager {{ $labels.job }}/{{ $labels.instance }} is failing to replicating partial state to its replicas.
            ||| % $._config,
          },
        },
        {
          alert: 'CortexAlertmanagerPersistStateFailing',
          expr: |||
            rate(cortex_alertmanager_state_persist_failed_total[15m]) > 0
          |||,
          'for': '1h',
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: |||
              %(product)s Alertmanager {{ $labels.job }}/{{ $labels.instance }} is unable to persist full state snaphots to remote storage.
            ||| % $._config,
          },
        },
        {
          alert: 'CortexAlertmanagerInitialSyncFailed',
          expr: |||
            increase(cortex_alertmanager_state_initial_sync_completed_total{outcome="failed"}[1m]) > 0
          |||,
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: |||
              %(product)s Alertmanager {{ $labels.job }}/{{ $labels.instance }} was unable to obtain some initial state when starting up.
            ||| % $._config,
          },
        },
      ],
    },
  ],
}
