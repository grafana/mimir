// Based on test-all-components.jsonnet.
(import 'test-all-components.jsonnet') {
  _config+:: {
    ingester_tsdb_head_early_compaction_enabled: true,
  },
}
