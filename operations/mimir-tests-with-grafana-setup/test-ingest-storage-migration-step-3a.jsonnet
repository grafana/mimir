// Migration step 3a:
// - Switch limits-operator to query for number of active partitions.
(import 'test-ingest-storage-migration-step-3.jsonnet') {
  _config+:: {
    limits_operator+:: {
      use_partitions: true,
    },
  },
}
