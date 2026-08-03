(import 'test-range-vector-splitting.jsonnet') {
  _config+:: {
    ruler_remote_evaluation_enabled: true,
    query_engine_range_vector_splitting_regular_path_enabled: false,
  },
}
