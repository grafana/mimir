// Migration step 5a:
// - Prepare classic ingesters to be scaled down to 0 replicas.
(import 'test-ingest-storage-migration-step-4.jsonnet') {
  _config+:: {
    // This builds on previous step.
    ingest_storage_migration_classic_ingesters_no_scale_down_delay: true,
  },
}
