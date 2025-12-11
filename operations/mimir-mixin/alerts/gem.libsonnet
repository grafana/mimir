local utils = import 'mixin-utils/utils.libsonnet';

(import 'alerts-utils.libsonnet') {
  local GEMFederationFrontendRemoteClusterErrors(histogram_type) = {
    alert: 'GEMFederationFrontendRemoteClusterErrors',  // We do not use the alertName function here because this alert only makes sense in the context of GEM.
    local sum_by = ["remote_cluster"],
    local range_interval = $.alertRangeInterval(1),
    local numerator = utils.ncHistogramSumBy(utils.ncHistogramCountRate('cortex_federation_frontend_cluster_remote_latency_seconds', 'status="server_error"', rate_interval=range_interval, from_recording=false), sum_by),
    local denominator = utils.ncHistogramSumBy(utils.ncHistogramCountRate('cortex_federation_frontend_cluster_remote_latency_seconds', '', rate_interval=range_interval, from_recording=false), sum_by),
    expr: |||
      100 * (
        %(numerator)s
        /
        %(denominator)s
      ) > 1
    ||| % {
      numerator: numerator[histogram_type],
      denominator: denominator[histogram_type],
    },
    'for': '15m',
    labels: $.histogramLabels({
      severity: 'critical',
      service: 'federation-frontend',
    }, histogram_type, nhcb=false),
    annotations: {
      summary: 'An upstream cluster is returning more than 1%% server-side errors over the last 15 minutes.',
      message: |||
        The federation-frontend has been receiving {{ $value | humanizePercentage }} errors from cluster {{ $labels.remote_cluster }} over the last 15 minutes.
        If partial responses are disabled (default), then clients of the federation-frontend are receiving errors.
        If partial responses are enabled, then responses are now less complete.
      ||| % $._config,
    },
  },

  local alertGroups = [
    {
      name: 'gem_alerts',
      rules: [
        GEMFederationFrontendRemoteClusterErrors('classic'),
        GEMFederationFrontendRemoteClusterErrors('native'),
      ],
    },
  ],

  groups+: $.withRunbookURL('https://grafana.com/docs/enterprise-metrics/latest/operations/runbooks/#%s', $.withExtraLabelsAnnotations(alertGroups)),
}
