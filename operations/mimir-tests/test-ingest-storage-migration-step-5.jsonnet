// Migration step 5:
// - Scale down classic ingesters to 0 replicas.
(import 'test-ingest-storage-migration-step-4.jsonnet') {
  _config+:: {
    // This builds on previous step.
    ingest_storage_migration_classic_ingesters_scale_down: true,
  },
}
