(import 'test-multi-az-read-path.jsonnet') {
  _config+:: {
    query_engine_range_vector_splitting_enabled: true,
  },
}
