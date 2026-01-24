// Migration step 12:
// - Disable ingester ring tokens (not used).
(import 'test-ingest-storage-migration-step-11.jsonnet') {
  _config+:: {
    ingest_storage_ingester_ring_tokens_enabled: false,
  },
}
