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
CGO_ENABLED=1 go build -tags rustengine,netgo,stringlabels -o /tmp/mimir-rust ./cmd/mimir
```

Enable with `-blocks-storage.tsdb.engine=rust`,
`-blocks-storage.tsdb.ship-interval=0`, and a fresh
`-blocks-storage.tsdb.dir`. A normal build continues using Prometheus; selecting
Rust in a binary without the Rust build tag or CGO returns an error. Rust refuses
a directory containing Prometheus storage. The Rust directory is locked against
concurrent opens.

The `rustengine` build tag links the static library. Build the library before Go
builds/tests, and rebuild it whenever Rust sources change. Cross-compilation
requires a Rust library and a C toolchain for the same target as Go.

## Boundary

CGO keeps tenant engines in the Go process. Known float series use one native
append call per request. Series creation, histograms, created timestamps, and
exemplars use the shared Go ingestion validation with a transactional Rust
appender. Native calls retain no Go pointers: request bytes are borrowed during a
call, Rust owns retained labels/samples, and Rust response buffers are freed
before returning to Go. Calls are serialized per tenant, and Rust panics poison
the engine and become errors at the C boundary. Native aborts and memory faults
still terminate the process.

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

Commits append request deltas to the WAL before publishing them. `Head().Sync()`
fsyncs the WAL before Go commits Kafka offsets. Flush and eviction write immutable
cold files, then atomically replace and fsync the head checkpoint, then truncate
the covered WAL. Replay skips checkpointed sequence numbers and truncates an
incomplete final WAL record. Flushed and evicted samples remain queryable.

## Current limits

This is a correctness and integration prototype, not a production replacement.
It does not produce Prometheus blocks, so block shipping is rejected. Its disk
format is experimental and has no compatibility promise. Histograms use opaque
protobuf payloads and are not compressed by a native histogram codec. Cold label
metadata/postings remain in Rust memory; cold sample files use JSON envelopes
around compressed float chunks, and matching files are read in full. WAL records
are JSON deltas. There are no checksums, cold-file merging, mmap indexes, native
allocator metrics, or per-shard concurrency yet. Compaction holds the tenant
lock. A forced flush establishes a lower append bound, including for OOO writes.

## Verify

```sh
cargo test --locked --manifest-path pkg/storage/rustengine/native/Cargo.toml
cargo build --locked --release --manifest-path pkg/storage/rustengine/native/Cargo.toml
CGO_ENABLED=1 go test -tags rustengine,netgo,stringlabels ./pkg/ingester -run '^TestRust'
```

Tests cover the C ABI through Go, Go-ingester writes, duplicate/OOO errors,
transaction rollback and limits, compressed chunks, histograms and typed stale
markers, exemplars, label matching after eviction, flush/restart, incomplete WAL
tails, directory locking, cold-file time filtering, retention, and tenant
isolation. These do not establish full Prometheus compatibility, production RSS,
or throughput parity with the Go engine.
