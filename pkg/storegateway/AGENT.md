# Store-gateway — streaming label/value search RPCs

This document covers the store-gateway-side implementation of the streaming
label/value search RPCs (`BucketStore.SearchLabelNames`,
`BucketStore.SearchLabelValues`). For the overall feature architecture see
[`pkg/streaminglabelvalues/AGENT.md`](../streaminglabelvalues/AGENT.md).

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
  fan-out, `applyPerBlockSearchHints`, `streamBucketSearchResults`, and
  `buildSearchResponseHints`.
- `pkg/storegateway/storepb/rpc.proto` — wire definitions for the RPCs,
  the `SearchResultBatch` carrier, and the `SearchResponseHints` header.

## Per-call flow

```
                              gRPC server-streaming RPC
                              SearchLabel{Names,Values}(req) →
                                stream SearchResultBatch
                                              │
                                              ▼
   ┌──────────────────────────────────────────────────────────────────────┐
   │ BucketStore.SearchLabel{Names,Values}  (bucket_search.go)            │
   │                                                                      │
   │  1. MatchersToPromMatchers(req.Matchers...)   wire → *labels.Matcher │
   │  2. searchLabel{Names,Values}RequestBlockMatchers(req)               │
   │       (request_hints.block_matchers; scopes which blocks to query)   │
   │  3. storepbToParams(req.Filter)        wire → *Params                │
   │     storepbToOrdering(req.Ordering)    wire → storage.Ordering       │
   │     int(req.Limit)                                                   │
   │                                                                      │
   │  4. seriesLimiter := s.seriesLimiterFactory(...) [SearchLabelNames]  │
   │                                                                      │
   │  5. errgroup.WithContext(ctx) ─── per-block goroutine fan-out:       │
   │                                                                      │
   │     s.blockSet.filter(req.Start, req.End, reqBlockMatchers,          │
   │       func(b *bucketBlock) {                                         │
   │           indexr := b.indexReader(...)   // keeps block pinned       │
   │           g.Go(func() error {                                        │
   │               result, err := blockLabel{Names,Values}(gctx, indexr,  │
   │                                  matchers, …, stats)                 │
   │               set, err := applyPerBlockSearchHints(                  │
   │                                  result, params, order, limit)       │
   │               // set is filtered+ordered+limited PER BLOCK           │
   │               setsMtx.Lock()                                         │
   │               sets = append(sets, set)                               │
   │               queriedBlocks = append(queriedBlocks, b.meta.ULID)     │
   │               setsMtx.Unlock()                                       │
   │           })                                                         │
   │       })                                                             │
   │                                                                      │
   │     g.Wait()  ── all per-block goroutines drain                      │
   │                                                                      │
   │  6. Single-set fast path: if len(sets) == 1, skip merge wrapper.     │
   │     Otherwise:                                                       │
   │       merged := storage.MergeSearchResultSets(sets, &SearchHints{…}) │
   │       ── k-way pairwise merge over per-block sets                    │
   │       ── re-applies OrderBy and Limit across blocks                  │
   │                                                                      │
   │  7. streamBucketSearchResults(ctx, merged|sets[0],                   │
   │                               queriedBlocks, srv.Send)               │
   └──────────────────────────────────────┬───────────────────────────────┘
                                          │
                                          ▼
                            srv.Send(*SearchResultBatch) → over gRPC
```

## Where filter / order / limit are applied

There are **two** distinct application points inside the store-gateway:

| Layer                                      | Filter                                  | Order                        | Limit             |
| ------------------------------------------ | --------------------------------------- | ---------------------------- | ----------------- |
| **Per-block** (`applyPerBlockSearchHints`) | ✓                                       | ✓                            | ✓ (per-block cap) |
| **Cross-block** (`MergeSearchResultSets`)  | (pass-through, leaves already filtered) | ✓ (re-applied across blocks) | ✓ (global cap)    |

### Cross-block: `MergeSearchResultSets`

The vendored Prometheus pairwise k-way merge is called with the
per-block sets and a `*storage.SearchHints{OrderBy, Limit}` so it
re-applies ordering and limit across blocks. The merge does not re-apply
the filter — leaves already did.

### Single-set fast path

When `len(sets) == 1` after block fan-out (single block in time range),
the merge wrapper is skipped:

```go
if len(sets) == 1 {
    defer sets[0].Close()
    return streamBucketSearchResults(ctx, sets[0], queriedBlocks, srv.Send)
}
```

Correctness depends on `applyPerBlockSearchHints` having applied
filter+order+limit in full — which it does. Tests
(`TestBucketStoreSearchLabelNames`, `TestBucketStoreSearchLabelValues`)
exercise this path because the test fixture builds a single block.

## Streaming and batch size

`streamBucketSearchResults` (lines ~294–334 of `bucket_search.go`) drains
the merged (or single-set) `SearchResultSet` and emits batches:

```
   // First message: header-only batch carrying queried_blocks.
   send(&SearchResultBatch{
       ResponseHints: buildSearchResponseHints(queriedBlocks),
   })

   var batch *SearchResultBatch
   for rs.Next() {
       if batch == nil {
           batch = &SearchResultBatch{
               Results: make([]Result, 0, searchBatchSize),  // 256
           }
       }
       r := rs.At()
       batch.Results = append(batch.Results,
           SearchResultBatch_Result{Value: r.Value, Score: r.Score})
       if len(batch.Results) >= searchBatchSize {       // 256-result boundary
           if err := send(batch); err != nil { return ... }
           batch = nil                                  // fresh envelope next iter
       }
   }
   // tail: attach warnings (if any), send.
```

**Batch size: 256 results per `*storepb.SearchResultBatch`** (the
`searchBatchSize` constant in `bucket_search.go`, deliberately matched
to the ingester's value).

### `request_hints.block_matchers`

Both request types carry an optional `request_hints.block_matchers`
list. When non-empty, the store-gateway's `blockSet.filter` evaluates
those matchers against each block's labels (e.g. shard label, source
label) to scope which blocks the goroutine fan-out covers. Empty disables
per-block filtering and all blocks in the time range participate.

This is how the querier's per-replica block assignment from
`queryWithConsistencyCheck` flows down into the store-gateway: the
querier embeds the block IDs / matchers in the request hint, and the
store-gateway only queries those.

## Concurrency

The per-block fan-out uses `errgroup` with the request `ctx` derived as
`errgroup.WithContext(ctx)`. There is **no explicit cap** on per-block
concurrency — the store-gateway relies on the bucket store's own
queue/limiter (configured at construction) to bound the goroutine count.

Per-block goroutines:

- pin their block by holding an `indexReader` (`b.indexReader(...)`)
  for the duration of the goroutine, defer-closed on exit (see
  `pendingReaders` ref-counting in `bucketBlock`),
- ensure the index header is loaded (`ensureIndexHeaderLoaded`),
- execute their search (`blockLabelNames` / `blockLabelValues`),
- apply per-block hints,
- append to a shared `sets` slice under `setsMtx`.

`g.Wait()` is the synchronisation point. After it returns, no per-block
goroutine is reading from a block, so all index readers are safely
closed by their defer statements before the merge is constructed.

## Error handling

- **Invalid matchers / filter / ordering** surface as
  `codes.InvalidArgument` via `status.Error`.
- **Per-block errors** abort the errgroup; the first error is returned
  by `g.Wait()`. The handler wraps that error appropriately:
  - `context.Canceled` → `codes.Canceled`
  - anything else → `codes.Internal`
- **Send errors** during streaming (`streamBucketSearchResults`) are
  surfaced by `streamBucketSearchResults` itself and propagate up; the
  merger's terminal error (`rs.Err()`) is joined with the send error
  via `joinWithMergerErr` so traces preserve the upstream cause.

## What the store-gateway does NOT do

- **Metadata enrichment**: the proto's `Result.Metadata` field is never
  populated by the store-gateway. Metric metadata lives only at the
  ingester. See `pkg/ingester/AGENT.md` for the enrichment path.
- **Tenant federation**: per-tenant scoping happens upstream at the
  querier; the store-gateway sees one tenant per call.

## Benchmarks

Benchmarks live in `pkg/storegateway/bucket_search_test.go`:

- `BenchmarkBucketStoreSearchLabelValues` — cardinality sweep
  (`label_1`, `label_3`, `high_cardinality_label_1` 1M values),
  filter (none / substring / fuzzy), ordering, limit.
- `BenchmarkBucketStoreSearchLabelNames` — small matrix; label-name
  cardinality is constant.
- `BenchmarkBucketStoreSearchLabelValuesVsLabelValues` — parity
  benchmark, same store, no filter, alpha asc, large limit; sub-cases
  keyed on `/impl=legacy` / `/impl=new` for `benchstat -col '/impl'`.

All store-gateway benchmarks share `prepareBenchmarkSearchStore`, which
builds a fixture identical to `BenchmarkBucketStoreLabelValues` in
`bucket_stores_test.go` so cardinality buckets line up across the legacy
and search benchmarks. The high-cardinality bench is **slow** (~50s
fixture build per bench function) — gate `-short` if you only want the
fast ones.
