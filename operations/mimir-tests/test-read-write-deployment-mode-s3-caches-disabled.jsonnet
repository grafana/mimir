// Based on test-read-write-deployment-mode-s3.jsonnet.
(import 'test-read-write-deployment-mode-s3.jsonnet') {
  _config+:: {
    cache_frontend_enabled: false,
    cache_index_queries_enabled: false,
    cache_chunks_enabled: false,
    cache_metadata_enabled: false,
  },
}
