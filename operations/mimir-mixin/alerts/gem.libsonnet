(import 'alerts-utils.libsonnet') {
  local alertGroups = [
    {
      name: 'gem_alerts',
      rules: [
        {
          alert: 'GEMFederationFrontendRemoteClusterErrors',  // We do not use the alertName function here because this alert only makes sense in the context of GEM.
          expr: |||
            100 * (
              sum by (remote_cluster) (rate(cortex_federation_frontend_cluster_remote_latency_seconds_count{status="server_error"}[%(range_interval)s]))
              /
              sum by (remote_cluster) (rate(cortex_federation_frontend_cluster_remote_latency_seconds_count[%(range_interval)s]))
            ) > 1
          ||| % {
            range_interval: $.alertRangeInterval(1),
          },
          'for': '15m',
          labels: {
            severity: 'critical',
            service: 'federation-frontend',
          },
          annotations: {
            summary: 'An upstream cluster is returning more than 1%% server-side errors over the last 15 minutes.',
            message: |||
              The federation-frontend has been receiving {{ $value | humanizePercentage }} errors from cluster {{ $labels.remote_cluster }} over the last 15 minutes.
              If partial responses are disabled (default), then clients of the federation-frontend are receiving errors.
              If partial responses are enabled, then responses are now less complete.
            ||| % $._config,
          },
        },
      ],
    },
  ],

  groups+: $.withRunbookURL('https://grafana.com/docs/enterprise-metrics/latest/operations/runbooks/#%s', $.withExtraLabelsAnnotations(alertGroups)),
}
