(import 'alerts-utils.libsonnet') {
  local alertGroups = if !$._config.gem_enabled then [] else [
    {
      name: 'gem_alerts',
      rules: [
        {
          alert: $.alertName('FederationFrontendRemoteClusterErrors'),
          expr: |||
            100 * (
              sum(rate(cortex_federation_frontend_cluster_remote_latency_seconds_count{status="server_error"}[%(rate_interval)s]))
              /
              sum(rate(cortex_federation_frontend_cluster_remote_latency_seconds_count[%(rate_interval)s]))
            ) > 1
          ||| % { rate_interval: $.alertRangeInterval(1) },
          'for': '15m',
          labels: {
            severity: 'critical',
            service: 'federation-frontend',
          },
          annotations: {
            message: 'Upstream clusters are returning more than 1%% errors over 15 minutes.',
          },
        },
      ],
    },
  ],

  groups+: $.withRunbookURL('https://grafana.com/docs/mimir/latest/operators-guide/mimir-runbooks/#%s', $.withExtraLabelsAnnotations(alertGroups)),
}
