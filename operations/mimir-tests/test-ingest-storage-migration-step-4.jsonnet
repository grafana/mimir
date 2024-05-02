// Migration step 4:
// - Stop writing to classic ingesters.
(import 'test-ingest-storage-migration-step-3.jsonnet') {
  _config+:: {
    // This builds on previous step.
    ingest_storage_migration_write_to_classic_ingesters_enabled: false,
  },
}
