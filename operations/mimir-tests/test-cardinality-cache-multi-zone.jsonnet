(import 'test-multi-az-read-path.jsonnet') {
  _config+:: {
    cache_cardinality_enabled: true,
  },
}
