// Based on test-all-components.jsonnet.
(import 'test-all-components.jsonnet') {
  _config+:: {
    ingester_stream_chunks_when_using_blocks: false,
  },
}
