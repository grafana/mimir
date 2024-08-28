// Based on test-all-components.jsonnet.
(import 'test-all-components.jsonnet') {
  _config+:: {
    overrides+: {
      'tenant-1': {
        max_global_series_per_user: 100000,
      },
      'tenant-2': {
        max_global_series_per_user: 200000,
      },
      'tenant-3': {
        max_global_series_per_user: 300000,
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
    },
  },
}
