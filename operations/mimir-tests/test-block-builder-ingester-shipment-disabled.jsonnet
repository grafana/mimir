// Based on test-block-builder.jsonnet. Tests the "full migration" mode where
// block-builder is the sole L0 block producer and ingesters stop shipping blocks.
(import 'test-block-builder.jsonnet') {
  _config+:: {
    block_builder+: {
      ingester_tsdb_ship_blocks_enabled: false,
    },
  },
}
