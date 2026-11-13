# Experimental Rust tenant engine

The Go ingester retains request handling, Kafka consumption and offset commits,
ring membership, limits, active-series tracking, ownership, and scheduling.
`pkg/ingester/engine_rust*.go` implements `tenantEngine` and `engineHead` from
`julienduchesne/engine-interface`. Rust owns sample storage, transactions, label
postings, the WAL, cold files, and retention.

## Build and run

From the repository root, with Rust and a C toolchain installed:

```sh
cargo build --locked --release --manifest-path pkg/storage/rustengine/native/Cargo.toml
rust_engine_build_id=$(shasum -a 256 pkg/storage/rustengine/native/target/release/libmimir_engine.a | awk '{print $1}')
CGO_ENABLED=1 CGO_CFLAGS="${CGO_CFLAGS:--O2 -g} -DMIMIR_RUST_${rust_engine_build_id}=1" \
  go build -tags rustengine,netgo,stringlabels -o /tmp/mimir-rust ./cmd/mimir
```

Enable with `-blocks-storage.tsdb.engine=rust`,
`-blocks-storage.tsdb.ship-interval=0`, and a fresh
`-blocks-storage.tsdb.dir`. A normal build continues using Prometheus; selecting
Rust in a binary without the Rust build tag or CGO returns an error. Rust refuses
a directory containing Prometheus storage. The Rust directory is locked against
concurrent opens.

The `rustengine` build tag links the static library. Build the library before Go
builds/tests, and rebuild it whenever Rust sources change. Go's build cache does
not track foreign static archives: use the archive hash in `CGO_CFLAGS` for both
builds and tests so a rebuilt Rust library cannot leave a cached Go binary.
Cross-compilation
requires a Rust library and a C toolchain for the same target as Go.

Linux and macOS builds use a prefixed jemalloc allocator for Rust allocations;
Go and other CGO libraries keep their allocators. Build with
`--no-default-features` for a system-allocator comparison.

## Boundary

CGO keeps tenant engines in the Go process. The versioned, little-endian binary
ABI copies owned data across the boundary. Float frames borrow decoded Go
request slices during the call, and Rust copies retained data. Known float
series stream in frames
of at most 256 series, 8192 samples, and 1 MiB, within one request transaction.
Oversized individual series use the transactional appender. Hard errors or
cancellation roll back every frame. Fully accepted float-only requests begin,
append, and commit in one native call; requests needing Go validation keep the
transaction open until the callbacks finish. Immutable series label dictionaries
are shared across native transactions and query snapshots. Series creation, histograms, created timestamps, and
exemplars use the shared Go ingestion validation with a transactional Rust
appender. Native calls retain no Go pointers: request bytes are borrowed during a
call, Rust owns retained labels/samples, and Rust response buffers are freed
before returning to Go. Calls are serialized per tenant, and Rust panics poison
the engine and become errors at the C boundary. Native aborts and memory faults
still terminate the process.

Queries retain an immutable snapshot of selected compressed series.
Queries without matchers or sharding select all native references without
copying the postings through Go. Unsharded, nonempty equality matchers resolve
postings in Rust and keep the selected references there. Go evaluates regexes
against distinct label values and sends value filters for other unsharded
selectors; Rust combines them with native postings. Sharding uses Go's
`labels.StableHash`. Head
snapshots copy uncompressed samples only within the query range. Series
labels and first chunks arrive in pages of at most 16 series. The Go Rust
querier builds streamed responses directly from those pages and pulls at most
16 remaining compressed chunks per call. Float chunks use the Prometheus XOR
format without per-sample Go conversion or iterator wrappers. XOR2 output currently
requires Go transcoding. Ordered reads normalize overlapping OOO chunks per
series; unordered streaming reads preserve the compressed chunks for merging
in the querier.
Cold files encode even short float tails as XOR chunks when written, so reads
reuse their encoded bytes.
Histogram chunks are encoded in Go from protobuf payloads. Querier close and
context cancellation release native cursors. Go owns returned bytes, so chunks
remain safe after the native response buffer is freed.

The `series` selection hint uses a labels-only cursor. Head samples are checked
without cloning them. Cold range endpoints answer membership without chunk
I/O; ambiguous interior ranges read the relevant records to preserve gaps.
Labels cross the boundary in pages of 16 series, with no sample chunks.

Two processes would provide crash isolation, but require IPC, streaming and
backpressure, process supervision, and coordinated shutdown/recovery. The current
engine interface already supplies an in-process boundary. This choice has not
been validated against production performance or availability requirements.

## Storage and upstream improvements

The XOR codec comes from `julienduchesne/new-ingester-rs` at `389d35c634`.
Storage patterns were checked against `julienduchesne/new-ingester` through
`efd29f5848`, including its later changes:

- Known-series float ingestion batches work across the boundary and reuses each
  resolved series identity (`9b531e9956`).
- XOR writers grow through tighter allocation sizes; completed buffers discard
  spare capacity. Open float chunks are bounded by sample count and byte size.
- Label name/value dictionaries and postings survive head eviction. Regex
  matchers run once per distinct value; label results intersect postings instead
  of resolving every matching series (`d4c5bf2cb0`, `4b5ffb31e4`, `efd29f5848`).
- Cold files and encoded chunks outside the requested time range are skipped
  (`7e7fd9bfb8`, `b2272b4251`).
- Cold retention uses file creation time and checkpoints the append cutoff so
  restart cannot resurrect expired data (`5552f2c403`, `1c9a689698`).

Kafka-only ingestion uses Kafka for recovery, like `new-ingester`, when gRPC Push
is disabled, file-stored consumer offsets are enforced, and the offset catalogue
is disabled. Commits publish without a WAL and `Head().Sync()` is a no-op. Clean
shutdown writes and fsyncs the complete head before marking it clean. Opening
removes that marker durably before accepting writes. If any tenant was not
restored cleanly, Go removes the partition's committed offset files before its
Kafka reader starts, forcing replay from retention. A Kafka recovery directory
cannot be reopened with a configuration that requires a WAL.

Other configurations append request deltas to a WAL before publishing them.
`Head().Sync()` flushes and fsyncs it before Go commits Kafka offsets. The fsync
owns a cloned file descriptor, allowing reads and ingestion to continue while
it waits; subsequent commits remain dirty for the next sync. Sync operations and
close are serialized, and a sync failure poisons the engine. Direct ingest
flushes the buffer at each commit. Flush and eviction write immutable
cold files, then atomically replace and fsync the head checkpoint, then truncate
the covered WAL. Replay skips checkpointed sequence numbers and truncates an
incomplete final WAL record. Flushed and evicted samples remain queryable.
Automatic compaction follows the Go engine's block eligibility rules, including
timely compaction, and coalesces eligible ranges into one checkpoint.
Idle series leave the head when their newest sample is older than the active
series timeout plus five minutes. Uncompacted out-of-order data protects a
series from idle eviction, including after WAL replay. Evicted samples remain
queryable in cold files. The binary WAL reader accepts the preceding version.

Label lookups expose the whole head index when its time range overlaps the
query. Cold files retain sorted series references for each aligned block range,
so labels remain visible throughout that range without including series from
empty gaps. Idle-evicted labels remain visible through the overlapping head
while their data reaches its inclusive logical cutoff. Broad timed label
lookups intersect dictionary postings; selective lookups visit the selected
series. Cold reads skip series and encoded chunks outside the query range
before allocating their decoded representation.
Nearby selected records share reads of up to 1 MiB. Larger individual records
use a dedicated read; every selected record retains its corruption checks.
Head and cold selector results have separate per-tenant caches, each capped at
128 entries and 64 KiB of accounted keys and postings. Least-recently-used
eviction preserves hot selectors when new ones fill the cache. Native index generations
invalidate results after series creation, eviction, compaction, or retention;
ordinary sample appends keep them reusable.
Streaming responses reuse decoded Go label adapters; Prometheus label sets are
built lazily for callers that need them. The response owns the backing bytes.
Postings sorting and query sharding fetch labels in pages of 64 references;
sharding continues to use Go's `labels.StableHash`.

## Current limits

This is a correctness and integration prototype, not a production replacement.
It does not produce Prometheus blocks, so block shipping is rejected. Its disk
format is experimental and has no compatibility promise. Histograms use opaque
protobuf payloads and are not compressed by a native histogram codec. Cold label
metadata/postings remain in Rust memory. JSON envelopes are retained only for
legacy cold files. New cold files use binary series records
and a persisted offset index, so a cursor reads only selected series. Cursor
snapshots share immutable compressed head chunks and copy cold data for selected
series. Response pages share the snapshot's compressed buffers until the wire
response is encoded. WAL records
are length-framed binary deltas, with labels recorded only on series creation.
JSON WAL directories require a fresh directory; this experimental disk format
has no migration guarantee. There are no checksums, cold-file merging, mmap indexes, native
allocator metrics, or per-shard concurrency yet. Compaction holds the tenant
lock. The configured OOO window remains open across flushes.

## Verify

```sh
cargo test --locked --manifest-path pkg/storage/rustengine/native/Cargo.toml
cargo build --locked --release --manifest-path pkg/storage/rustengine/native/Cargo.toml
rust_engine_build_id=$(shasum -a 256 pkg/storage/rustengine/native/target/release/libmimir_engine.a | awk '{print $1}')
CGO_ENABLED=1 CGO_CFLAGS="${CGO_CFLAGS:--O2 -g} -DMIMIR_RUST_${rust_engine_build_id}=1" \
  go test -race -tags rustengine,netgo,stringlabels ./pkg/storage/rustengine ./pkg/ingester -run '^TestRust'
```

Tests cover the C ABI through Go, Go-ingester writes, duplicate/OOO errors,
transaction rollback and limits, compressed chunks, histograms and typed stale
markers, exemplars, label matching after eviction, flush/restart, incomplete WAL
tails, directory locking, cold-file time filtering, retention, and tenant
isolation. These do not establish full Prometheus compatibility, production RSS,
or throughput parity with the Go engine.
