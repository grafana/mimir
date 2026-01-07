// Based on test-shuffle-sharding.jsonnet.
(import 'test-shuffle-sharding.jsonnet') {
  _config+:: {
    shuffle_sharding+:: {
      store_gateway_shard_size_per_zone_enabled: true,
    },
  },
}
