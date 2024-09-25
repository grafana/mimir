// Migration step 4:
// - Switch read path to partition ingesters.
(import 'test-ingest-storage-migration-step-2.jsonnet') {
  _config+:: {
    // This builds on previous step.
    ingest_storage_migration_querier_enabled: true,
  },
}
