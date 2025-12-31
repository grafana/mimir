// Based on test-shuffle-sharding.jsonnet.
(import 'test-shuffle-sharding.jsonnet') {
  _config+:: {
    shuffle_sharding+:: {
      ingester_read_path_enabled: false,
    },
  },
}
