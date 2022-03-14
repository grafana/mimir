(import 'alerts-utils.libsonnet') {
  groups+: [
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
              %(product)s Alertmanager {{ $labels.job }}/{{ $labels.instance }} is failing to read tenant configurations from storage.
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
              %(product)s Alertmanager {{ $labels.job }}/{{ $labels.instance }} is unable to check tenants ownership via the ring.
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
              %(product)s Alertmanager {{ $labels.job }}/{{ $labels.instance }} is failing to merge partial state changes received from a replica.
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
              %(product)s Alertmanager {{ $labels.job }}/{{ $labels.instance }} is failing to replicating partial state to its replicas.
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
              %(product)s Alertmanager {{ $labels.job }}/{{ $labels.instance }} is unable to persist full state snaphots to remote storage.
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
              %(product)s Alertmanager {{ $labels.job }}/{{ $labels.instance }} was unable to obtain some initial state when starting up.
            ||| % $._config,
          },
        },
        {
          alert: $.alertName('AlertmanagerAllocatingTooMuchMemory'),
          expr: |||
            (container_memory_working_set_bytes{container="alertmanager"} / container_spec_memory_limit_bytes{container="alertmanager"}) > 0.80
            and
            (container_spec_memory_limit_bytes{container="alertmanager"} > 0)
          |||,
          'for': '15m',
          labels: {
            severity: 'warning',
          },
          annotations: {
            message: |||
              Alertmanager {{ $labels.pod }} in %(alert_aggregation_variables)s is using too much memory.
            ||| % $._config,
          },
        },
        {
          alert: $.alertName('AlertmanagerAllocatingTooMuchMemory'),
          expr: |||
            (container_memory_working_set_bytes{container="alertmanager"} / container_spec_memory_limit_bytes{container="alertmanager"}) > 0.90
            and
            (container_spec_memory_limit_bytes{container="alertmanager"} > 0)
          |||,
          'for': '15m',
          labels: {
            severity: 'critical',
          },
          annotations: {
            message: |||
              Alertmanager {{ $labels.pod }} in %(alert_aggregation_variables)s is using too much memory.
            ||| % $._config,
          },
        },
      ],
    },
  ],
}
