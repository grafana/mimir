(import 'mixin.libsonnet') + {
  // Config overrides used when building the compiled version of the mimir-mixin.
  // This includes all features, since the compiled version can't be customized.
  _config+:: {
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
      ruler+: {
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
