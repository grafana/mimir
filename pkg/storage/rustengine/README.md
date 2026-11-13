# Experimental Rust tenant engine

The ingester stays in Go: requests, Kafka, offsets, the ring, limits and scheduling. `pkg/ingester/engine_rust*.go`
implements the ingester's `tenantEngine` with a tenant storage engine written in Rust, which this package calls through
CGO. Rust owns the tenant's series, their samples, the label index, the files old chunks move to, and the active
series.

## Build and run

From the repository root, with Rust and a C toolchain installed:

```sh
cargo build --locked --release --manifest-path pkg/storage/rustengine/native/Cargo.toml
rust_engine_build_id=$(shasum -a 256 pkg/storage/rustengine/native/target/release/libmimir_engine.a | awk '{print $1}')
CGO_ENABLED=1 CGO_CFLAGS="${CGO_CFLAGS:--O2 -g} -DMIMIR_RUST_${rust_engine_build_id}=1" \
  go build -tags rustengine,netgo,stringlabels -o /tmp/mimir-rust ./cmd/mimir
```

Go's build cache doesn't track the static library: the archive's hash in `CGO_CFLAGS` makes a rebuilt library rebuild
the Go packages that link it. Select the engine with `-blocks-storage.tsdb.engine=rust` and
`-blocks-storage.tsdb.ship-interval=0`, in a directory without Prometheus TSDB data.

## Recovery

There is no WAL, as for the Go seriesstore engine: Kafka is the log. A clean close writes the head, and the next open
restores it. Without a clean head, the engine starts empty, removes what it had on disk, and reports that it needs Kafka
replayed, which makes the ingester replay the partition from its retention. Ingest storage is required to keep what was
ingested across a crash.

## Boundary

Each call passes an operation number and a little-endian frame, and the record content borrowed for the call when
there is one. Rust keeps no Go memory. Responses are copied into Go memory, except on the streaming path, where pages
are read in Rust's memory and given back once sent, chunks are given as pointers into pages and files the query's
cursor keeps until it is closed, and labels as pointers into the engine's symbols while the query pins the engine.

Engines lock themselves: ingestion and upkeep take the state's write lock, queries its read lock, so queries run while
the tenant ingests. A tenant's records are pushed one at a time, as the Kafka consumer does.

## Ingestion

`ReadRecord` parses a remote write 2.0 record in place, in Rust. `Ingest` then appends, in one call per record, the
floats of the series the head has: their label references are mapped to the engine's symbols once per record, series
are found by two 64-bit hashes kept in the lookup table, and their memory is prefetched before appending to them. What
a record leaves (series the head doesn't have, histograms, exemplars, created timestamps) goes through the ingester's
shared validation with an appender that stages samples in Rust and commits them at once.

Appends follow Prometheus: a request's samples are checked against the series as they were when it started, with the
head's bounds of that time, and checked again when applied, where what conflicts with the request's own samples is
dropped.

## Storage

- Label names and values are interned. Series refer to their labels by symbol, and postings are sorted lists of slots;
  a slot is never reused while the engine runs, so postings stay sorted by appending. Symbols have ranks in the order
  of their strings, so selections sort by labels comparing numbers.
- In-order floats are XOR chunks, cut at 120 samples, about 1 KiB, or the block's end. A series' open chunk keeps its
  buffer from one chunk to the next, and sealed chunks of a block share 64 KiB pages, which free together when the
  block leaves memory.
- Late floats, histograms and exemplars are kept apart, for the few series that have them.
- Compaction follows the Prometheus engine's block eligibility: it writes the blocks' chunks to immutable files, one per
  block per compaction, which queries map. Late floats get chunks of their own in them; ordered queries merge
  overlapping chunks, and the ingester streams unordered ones when out-of-order ingestion is on.
- Series idle past the active series' timeout leave memory, their data staying queryable. Label queries still see them
  until the head would have compacted their data, as Prometheus' head would have kept them.
- The engine keeps when each series was last ingested, its custom trackers and its last bucket count, so the ingester
  doesn't feed its active series tracker for every series of every push.

## Verify

```sh
cargo test --locked --release --manifest-path pkg/storage/rustengine/native/Cargo.toml
CGO_ENABLED=1 CGO_CFLAGS="-O2 -g -DMIMIR_RUST_${rust_engine_build_id}=1" \
  go test -tags rustengine,stringlabels ./pkg/ingester -run '^TestRust'
```

`TestRustMatchesPrometheus` pushes the same records to an ingester with each engine and compares what queries read,
streamed or not, label queries, discarded samples and active series, with and without out-of-order ingestion, across a
restart.

`BenchmarkEngineCompare` (build tag `enginecompare`) drives an ingester through the Kafka record path and queries, and
runs the same against the Go engine on its branch, to compare them.

## Limits

This is a prototype. It writes no Prometheus blocks, so block shipping is rejected. Its disk format has no
compatibility promise. Histograms are kept as their protobuf rather than compressed. Symbols aren't collected, and
series slots grow with churn until the engine restarts.
