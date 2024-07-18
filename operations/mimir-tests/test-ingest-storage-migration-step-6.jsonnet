// Migration step 6:
// - Scale down classic ingesters to 0 replicas.
(import 'test-ingest-storage-migration-step-5.jsonnet') {
  _config+:: {
    // This builds on previous step.
    ingest_storage_migration_classic_ingesters_scale_down: true,
  },
}
