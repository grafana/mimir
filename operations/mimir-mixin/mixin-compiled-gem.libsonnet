(import 'mixin.libsonnet') + {
  // This config enables features that are relevant to most GEM deployments.
  _config+:: {
    product: 'GEM',
    alert_product: 'Mimir',
    tags+: ['gem'],

    // This is the md5 of the gem-rollout-operator dashboard name.
    // This is set such that if the name / uid was to change an error will be raised in dashboard generation.
    // This ensures that the uid is consistent and can be reliably linked to.
    rollout_operator_dashboard_uid: 'f40e8042a6be71a98444a29b2c4e9421',

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
      ruler+: {
        enabled: true,
      },
    },
  },
}
