// Based on test-shuffle-sharding.jsonnet.
(import 'test-shuffle-sharding.jsonnet') {
  _config+:: {
    shuffle_sharding+:: {
      ingest_storage_partitions_enabled: true,
    },
  },
}
