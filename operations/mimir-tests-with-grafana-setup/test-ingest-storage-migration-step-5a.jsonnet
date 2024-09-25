// Migration step 5a:
// - Prepare classic ingesters to be scaled down to 0 replicas.
(import 'test-ingest-storage-migration-step-4.jsonnet') {
  _config+:: {
    ingest_storage_ingester_migration_classic_ingesters_remove_downscaling_annotations: true,
  },
}
