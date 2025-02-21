(import 'mixin.libsonnet') + {
  // This config enables features that are relevant to most GEM deployments.
  _config+:: {
    gem_enabled: true,

    gateway_enabled: true,

    autoscaling+: {
      querier+: {
        enabled: true,
      },
      ruler_querier+: {
        enabled: true,
      },
      distributor+: {
        enabled: true,
      },
      query_frontend+: {
        enabled: true,
      },
      ruler_query_frontend+: {
        enabled: true,
      },
    },
  },
}
