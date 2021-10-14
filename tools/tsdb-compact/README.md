tsdb-compact runs Prometheus compaction code, but without the planning -- instead it compacts (merges) blocks
passed as command line arguments into single (or multiple when splitting is enabled) blocks.
