# Store-gateway — streaming label/value search RPCs

This document covers the store-gateway-side implementation of the streaming
label/value search RPCs (`BucketStore.SearchLabelNames`,
`BucketStore.SearchLabelValues`). For the overall feature architecture see
[`README.md`](./README.md).

## Scope

The store-gateway is a **leaf source** in the search fan-out but, unlike
the ingester, it owns **multiple blocks** for a single tenant. Each
matching block is a sub-source. The store-gateway therefore:

1. **Fans out** the search across all matching blocks (one goroutine per
   block via `errgroup`).
2. **Applies filter / ordering / limit per block** so each per-block
   `SearchResultSet` is independently correct.
3. **K-way merges** the per-block sets via `storage.MergeSearchResultSets`
   to produce one cross-block stream.
4. **Streams** the merged stream out to its gRPC client (the
   blocks-store-querier on the querier side) in 256-result batches,
   prefixed with a **header batch** carrying the queried-block set.

Single-set fast path: when only one block matches the request's time
range and block matchers, the merge wrapper is skipped because the
per-block search has already honoured the hints in full.

## Files

- `pkg/storegateway/bucket_search.go` — RPC handlers, the per-block
  fan-out, the per-block hints applier, the streaming-response writer,
  and the queried-blocks header builder.
- `pkg/storegateway/storepb/rpc.proto` — wire definitions for the RPCs,
  the `SearchResultBatch` carrier, and the `SearchResponseHints` header.

## Per-call flow

A single `SearchLabel{Names,Values}` RPC executes the following steps inside
the handler:

1. **Wire-to-internal conversion** — label matchers, filter, ordering, and
   limit are converted from their wire types into the internal types used
   throughout the bucket store. Optional `request_hints.block_matchers` are
   extracted and used later to scope the block fan-out.
2. **Limiter setup** — a per-call series limiter is constructed (label-names
   path only).
3. **Per-block fan-out** — `errgroup` spawns one goroutine per matching
   block. Each goroutine pins its block by holding an `indexReader` for the
   duration of the work, runs the per-block label-names / label-values
   search, applies filter+ordering+limit **per block** so each per-block
   result set is independently correct, and appends the result set plus the
   block's ULID to shared slices under a mutex.
4. **Synchronisation** — `g.Wait()` ensures every per-block goroutine has
   drained before merging; index readers are closed by their defers at that
   point.
5. **Cross-block k-way merge** — the per-block result sets are merged via
   the vendored Prometheus pairwise merge, which re-applies ordering and
   limit across blocks. The merge does **not** re-apply the filter — leaves
   already did that. If only one block matched, the merge wrapper is
   skipped (see [Single-set fast path](#single-set-fast-path)).
6. **Streaming response** — the merged (or single-set) result is drained
   into fixed-size batches and sent over the gRPC stream, preceded by a
   header-only batch carrying the set of queried block ULIDs.

## Where filter / order / limit are applied

There are **two** distinct application points inside the store-gateway:

| Layer            | Filter                                  | Order                        | Limit             |
| ---------------- | --------------------------------------- | ---------------------------- | ----------------- |
| **Per-block**    | ✓                                       | ✓                            | ✓ (per-block cap) |
| **Cross-block**  | (pass-through, leaves already filtered) | ✓ (re-applied across blocks) | ✓ (global cap)    |

### Cross-block merge

The vendored Prometheus pairwise k-way merge is called with the per-block
sets and the request's ordering and limit so it re-applies ordering and
limit across blocks. The merge does not re-apply the filter — leaves
already did.

### Single-set fast path

When only one block matches the time range and block matchers, the merge
wrapper is skipped and the single result set is streamed directly.
Correctness depends on the per-block stage having applied
filter+order+limit in full — which it does. The single-block test fixtures
(`TestBucketStoreSearchLabelNames`, `TestBucketStoreSearchLabelValues`)
exercise this path.

## Streaming and batch size

`streamBucketSearchResults` drains the merged (or single-set) result set
and emits batches over the gRPC stream. The first message on the wire is a
**header-only batch** carrying the `queried_blocks` response hint — this
tells the querier which blocks were actually consulted, which it needs for
its consistency check. Result batches follow, each capped at
`searchBatchSize` results (deliberately matched to the ingester's value).
A trailing batch carries any warnings.

### `request_hints.block_matchers`

Both request types carry an optional `request_hints.block_matchers`
list. When non-empty, those matchers are evaluated against each block's
labels (e.g. shard label, source label) to scope which blocks the
goroutine fan-out covers. Empty disables per-block filtering and all
blocks in the request's time range participate.

This is the channel by which the querier's per-replica block assignment
(from its consistency-check flow) reaches the store-gateway: the querier
embeds the block IDs / matchers in the request hint, and the
store-gateway only queries those.

## Concurrency

The per-block fan-out uses `errgroup` derived from the request context.
There is **no explicit cap** on per-block concurrency — the store-gateway
relies on the bucket store's own queue/limiter (configured at
construction) to bound the goroutine count.

Each per-block goroutine pins its block by holding an index reader for
the goroutine's lifetime (the reader is defer-closed on exit and is
ref-counted by the block), lazily ensures the index header is loaded,
runs the per-block label-names / label-values search, applies per-block
hints, and appends the resulting set to a shared collection under a
mutex.

`g.Wait()` is the synchronisation point. After it returns, no per-block
goroutine is reading from a block, so all index readers are safely
closed by their defers before the merge is constructed.

## Error handling

- **Invalid matchers / filter / ordering** surface as
  `codes.InvalidArgument` via `status.Error`.
- **Per-block errors** abort the errgroup; the first error is returned
  by `g.Wait()`. The handler wraps that error appropriately:
  - `context.Canceled` → `codes.Canceled`
  - anything else → `codes.Internal`
- **Send errors** during streaming are returned from the streaming-response
  writer and propagate up; the merger's terminal error (`rs.Err()`) is
  joined with the send error so traces preserve the upstream cause.

## What the store-gateway does NOT do

- **Metadata enrichment**: the proto's `Result.Metadata` field is never
  populated by the store-gateway. Metric metadata lives only at the
  ingester. See [`ingester.md`](./ingester.md) for the enrichment path.
- **Tenant federation**: per-tenant scoping happens upstream at the
  querier; the store-gateway sees one tenant per call.

## Benchmarks

Benchmarks live in `pkg/storegateway/bucket_search_test.go`:

- `BenchmarkBucketStoreSearchLabelValues` — cardinality sweep across the
  fixture's low- and high-cardinality labels, with filter (none /
  substring / fuzzy), ordering, and limit varied.
- `BenchmarkBucketStoreSearchLabelNames` — small matrix; label-name
  cardinality is constant.
- `BenchmarkBucketStoreSearchLabelValuesVsLabelValues` — parity
  benchmark, same store, no filter, alpha asc, large limit; sub-cases
  keyed on `/impl=legacy` / `/impl=new` for `benchstat -col '/impl'`.

All store-gateway benchmarks share `prepareBenchmarkSearchStore`, which
builds a fixture identical to `BenchmarkBucketStoreLabelValues` in
`bucket_stores_test.go` so cardinality buckets line up across the legacy
and search benchmarks. The high-cardinality bench is **slow** to set up —
gate `-short` if you only want the fast ones.
