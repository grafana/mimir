// Based on test-all-components.jsonnet.
(import 'test-all-components.jsonnet') {
  _config+:: {
    overrides+: {
      'tenant-1': {
        max_global_series_per_user: 100000,
        max_global_series_per_metric: 1000,
      },
      'tenant-2': {
        max_global_series_per_user: 200000,
        max_global_series_per_metric: 2000,
      },
      'tenant-3': {
        max_global_series_per_user: 300000,
        max_global_series_per_metric: 3000,
      },
      'tenant-4': {
        max_global_series_per_user: 400000,
        max_global_series_per_metric: 5000,
      },
      'tenant-5': {
        max_global_series_per_user: 500000,
        max_global_series_per_metric: 5000,
      },
    },
  },
} + {
  _config+:: {
    overrides+: {
      // Remove tenant-2 from limits in the runtime config.
      'tenant-2': null,

      // Intentionally override tenant-3 with an empy object which is expected to be preserved.
      'tenant-3': {},

      // Override a single value with 0.
      'tenant-4'+: {
        max_global_series_per_metric: 0,
      },

      // Remove a single value.
      'tenant-5'+: {
        max_global_series_per_metric: null,
      },
    },
  },
}
