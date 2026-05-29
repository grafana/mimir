# Streaming label/value search

This document provides a Mimir implementation guide for the label/values API proposed and implemented by Prometheus.

See [https://github.com/prometheus/proposals/pull/74](https://github.com/prometheus/proposals/pull/74).

## What this feature is

Three experimental HTTP endpoints, gated by
`-querier.experimental-search-api-enabled` (default `false`), that let
clients perform substring / fuzzy filtering over label names and label
values without materialising the full result set:

| HTTP method  | Path                          | Returns                                |
| ------------ | ----------------------------- | -------------------------------------- |
| `GET`/`POST` | `/api/v1/search/metric_names` | Filtered, ordered list of metric names |
| `GET`/`POST` | `/api/v1/search/label_names`  | Filtered, ordered list of label names  |
| `GET`/`POST` | `/api/v1/search/label_values` | Filtered, ordered list of label values |

Responses are batched with **NDJSON** (`application/x-ndjson`): one JSON object per line, flushed
incrementally, terminated by a status trailer.

## Top-level data flow

```
              HTTP (NDJSON)
                 │
                 ▼
  ┌──────────────────────────────────────────────────────────────────────┐
  │ HTTP handler                                                         │
  │   parses URL params, builds search Params + hints,                   │
  │   opens a Querier, streams NDJSON to the client                      │
  └──────────────────────────────┬───────────────────────────────────────┘
                                 │ in-process
                                 ▼
  ┌──────────────────────────────────────────────────────────────────────┐
  │ Tenant federation                                                    │
  │   per-tenant fan-out; k-way merge across tenants                     │
  │   single-tenant fast paths skip the merge wrapper                    │
  └──────────────────────────────┬───────────────────────────────────────┘
                                 │ in-process (one per tenant)
                                 ▼
  ┌──────────────────────────────────────────────────────────────────────┐
  │ Multi-searcher                                                       │
  │   fans out to all child queriers, k-way merges their result sets     │
  └────────┬──────────────────────────────────────────────────┬──────────┘
           │ in-process                                       │ in-process
           ▼                                                  ▼
  ┌──────────────────────────┐                ┌─────────────────────────────────┐
  │ Distributor querier      │                │ Blocks-store querier            │
  │   (runs in querier proc) │                │   (runs in querier proc)        │
  │ fan out to quorum-many   │                │ fan out to store-gateways       │
  │ ingesters                │                │ k-way merge across SGs          │
  │ per-ingester prefetch    │                │                                 │
  │ k-way merge per-ingester │                │                                 │
  └────────┬─────────────────┘                └────────┬────────────────────────┘
           │ gRPC server-streaming                     │ gRPC server-streaming
           │                                           │
           │ NB: BOTH boxes above run INSIDE the       │
           │ querier process. "Distributor" here is    │
           │ the read-path client wrapper, not the     │
           │ write-path distributor service.           │
           ▼                                           ▼
  ┌──────────────────────────┐                ┌─────────────────────────────────┐
  │ Ingester                 │                │ Store-gateway                   │
  │ single TSDB head source  │                │ per-block fan-out (errgroup)    │
  │ streams fixed-size       │                │ k-way merge across blocks       │
  │ result batches           │                │ streams fixed-size batches      │
  │                          │                │ (header batch first)            │
  └──────────────────────────┘                └─────────────────────────────────┘
```

### Code locations

| Layer                       | Code                                               |
| --------------------------- | -------------------------------------------------- |
| HTTP handler                | `pkg/querier/search_handler.go`                    |
| Tenant federation           | `pkg/querier/tenantfederation/merge_queryable.go`  |
| Multi-searcher              | `pkg/querier/multi_searcher.go`                    |
| Distributor querier wrapper | `pkg/querier/distributor_queryable_search.go`      |
| In-process distributor call | `pkg/distributor/distributor_search.go`            |
| Blocks-store querier        | `pkg/querier/blocks_store_queryable_search.go`     |
| Ingester                    | `pkg/ingester/ingester_search.go`                  |
| Store-gateway               | `pkg/storegateway/bucket_search.go`                |
| Per-stream prefetcher       | `pkg/storage/concurrent_searchresult.go`           |
| Shared params/filters       | `pkg/streaminglabelvalues/`                        |
| Vendored k-way merge        | `vendor/github.com/prometheus/prometheus/storage/` |

## Where filtering, ordering, limit, and k-way merge happen

There are **two distinct layers** where these operations are applied:

1. **Leaf (per-source) layer** — at the ingester (single in-memory TSDB head)
   and at the store-gateway (per-block, fan-out within the store-gateway).
   Each leaf applies the full `Filter+OrderBy+Limit` to its own data.
2. **Merge (k-way) layer** — at the distributor (over quorum-many ingester
   streams), at the blocks-store querier (over per-store-gateway streams),
   at the multi-searcher (over distributor+blocks-store sources), and at
   the tenant-federation layer (over per-tenant sources).

| Layer                           | Filter? | Order? | Limit?                      | K-way merge?            |
| ------------------------------- | ------- | ------ | --------------------------- | ----------------------- |
| Ingester                        | ✓       | ✓      | ✓                           | n/a (one source)        |
| Store-gateway per-block         | ✓       | ✓      | ✓ (per-block cap)           | n/a (one block)         |
| Store-gateway (across blocks)   |         |        | enforced by merge           | `MergeSearchResultSets` |
| Distributor (over ingesters)    |         |        | enforced by merge           | `MergeSearchResultSets` |
| Blocks-store querier (over SGs) |         |        | enforced by merge           | `MergeSearchResultSets` |
| Multi-searcher (querier fan)    |         |        | enforced by merge           | `MergeSearchResultSets` |
| Tenant federation               |         |        | enforced by merge           | `MergeSearchResultSets` |
| HTTP handler                    |         |        | enforced by `limit+1` probe | n/a                     |

The filter / ordering parameters (`Params` from `pkg/streaminglabelvalues` and
`*storage.SearchHints` from Prometheus) propagate through every layer
unchanged — leaves apply them, merge layers only re-apply ordering and limit
to honour them across sources.

`storage.MergeSearchResultSets` is **vendored Prometheus** code
(`vendor/github.com/prometheus/prometheus/storage/...`). It is a pairwise
binary-tree k-way merge that:

- respects `hints.OrderBy` (alpha asc, alpha desc, score desc),
- enforces `hints.Limit` without materialising the full union,
- deduplicates identical `Value` across sources (last-write-wins on score),
- propagates leaf warnings (annotations) to the merged set.

## Wire protocols and batch granularity

| Boundary                        | Protocol                            | Batch granularity                                                                                         |
| ------------------------------- | ----------------------------------- | --------------------------------------------------------------------------------------------------------- |
| Client ↔ querier (HTTP handler) | NDJSON over HTTP, chunked + flushed | Caller-controlled via `batch_size` URL param, defaulted and capped by constants in the HTTP handler       |
| Querier ↔ distributor querier   | in-process                          | n/a                                                                                                       |
| Distributor querier ↔ ingester  | gRPC server-streaming               | Fixed (`searchBatchSize`) results per `SearchResultBatch` proto message                                   |
| Distributor's prefetch buffer   | Go channel                          | One wire batch in flight per ingester (`ingesterSearchPrefetchBuffer`)                                    |
| Querier ↔ store-gateway         | gRPC server-streaming               | Fixed (`searchBatchSize`) results per `SearchResultBatch` proto message, deliberately matched to ingester |

The HTTP handler writes one NDJSON line per batch and flushes immediately, so
the client sees results incrementally. The gRPC `searchBatchSize` constant is
intentionally shared between ingester and store-gateway — it's the
production-tuned trade-off between gRPC framing overhead and per-call memory
footprint, and changing it on one side without the other would create a
mismatch. The querier-side prefetcher wraps each gRPC stream with a single
buffered channel so the merge layer can read from one stream while another's
`Recv()` is in flight.

## Tenant federation

`pkg/querier/tenantfederation/merge_queryable.go` wraps the multi-searcher
with per-tenant fan-out. Two fast paths short-circuit the merge: a
configured "bypass on single tenant id" flag forwards directly to the
upstream single-tenant call without injecting the synthetic id label; and,
independently of that flag, any fan-out that reduces to a single job after
matcher filtering also bypasses the merge wrapper, since a k-way merge over
a single source is pass-through.

Multi-tenant fan-out uses `concurrency.ForEachJob` with a configured
concurrency cap. Result sets are collected into a pre-sized slice indexed
by job and merged via the vendored Prometheus k-way merge once all jobs
return.

## Metadata enrichment (`include_metadata`)

The `include_metadata=true` URL param on `/api/v1/search/metric_names` asks
for per-metric `Type/Help/Unit` metadata to be inlined on each result. This
enrichment is **applied only at the ingester**, the store-gateway has no metric metadata
to enrich with.

## Per-source notes

Both leaf sources apply the full filter / order / limit per call before
streaming results back to their gRPC client. They differ in what "their
data" means: the ingester sees a single in-memory TSDB head; the store-
gateway sees multiple blocks for a tenant and fans out internally.

### Ingester (single TSDB head, leaf)

The ingester exposes two gRPC server-streaming RPCs (`stream
SearchResultBatch`) over its existing gRPC server. Each RPC reads from the
per-tenant TSDB head — a **single in-memory source** — and streams batches
of results back to the gRPC client.

**Who is the gRPC client?** Despite the file location `pkg/distributor/`,
the search RPC's fan-out code is the **read-path client wrapper** that runs
inside the **querier** process (and any other component that embeds the
querier, e.g. ruler). The querier's distributor-querier wrapper holds a
`*Distributor` reference and calls into `Distributor.SearchLabel{Names,Values}`
in `pkg/distributor/distributor_search.go`, which is what opens the gRPC
stream to the ingester. The distributor _service_ (write path) is not on
the read-path call chain — the `Distributor` type is shared code, not a
network hop.

There is no k-way merge inside the ingester because there is only one
source. Filtering, ordering, and limit propagate from the wire hints
through to Prometheus's `storage.Searcher` interface on the head. Because
the ingester is a single-source leaf, the limit applied here is the
**per-source limit** — the distributor's merge layer re-applies the global
limit across all ingesters in the quorum.

**Files**: `pkg/ingester/ingester_search.go` (RPC handlers, streaming
loop, metadata decorator, wire/internal converters);
`pkg/ingester/client/ingester.proto` (RPC + batch wire definitions).

**Per-call flow:**

1. **Wire-to-internal conversion** — request hints (filter, ordering,
   limit, label matchers) are converted into the internal `Params` /
   compiled `Filter` / `[]*labels.Matcher` / clamped `int` limit (a `0`
   limit means "unlimited", per the Prometheus convention; the limit is
   clamped to fit a Go `int`).
2. **Tenant extraction and read-consistency enforcement** — fail fast on
   missing tenant or consistency violations.
3. **Open a time-bounded querier on the per-tenant TSDB head.**
4. **Type-assert to `storage.Searcher` and invoke the appropriate search
   method** — filter, ordering, and limit are all applied inside the
   searcher (per-candidate-value `Filter.Accept`; source-iterator
   ordering or sorting wrap; early-stop on limit).
5. **Stream the result set** as fixed-size batches over the gRPC
   server-streaming response, optionally decorating each batch with
   metric metadata (see [Metadata enrichment](#metadata-enrichment-include_metadata)).

**Streaming loop**: the loop drains the `storage.SearchResultSet` and
emits batches sized at `searchBatchSize`. The same envelope and the same
underlying slice are reused across all batches in one call — `stream.Send`
is synchronous (gogoproto marshals before returning) so resetting the
slice length to zero after each send is safe. Cancellation is checked
before iteration starts; the inner `storage.Searcher` honours `ctx` and a
cancelled client stops the loop at the next iteration boundary.

### Store-gateway (per-block fan-out, leaf)

The store-gateway is a leaf source in the search fan-out but, unlike the
ingester, it owns **multiple blocks** for a single tenant. Each matching
block is a sub-source. The store-gateway therefore:

1. **Fans out** the search across all matching blocks (one goroutine per
   block via `errgroup`).
2. **Applies filter / ordering / limit per block** so each per-block
   `SearchResultSet` is independently correct.
3. **K-way merges** the per-block sets via `storage.MergeSearchResultSets`
   to produce one cross-block stream.
4. **Streams** the merged stream out to its gRPC client (the blocks-store-
   querier on the querier side) in fixed-size batches, prefixed with a
   **header batch** carrying the queried-block set.

When only one block matches the request's time range and block matchers,
the merge wrapper is skipped — the per-block search has already honoured
the hints in full.

**Files**: `pkg/storegateway/bucket_search.go` (RPC handlers, per-block
fan-out, per-block hints applier, streaming-response writer, queried-
blocks header builder); `pkg/storegateway/storepb/rpc.proto` (RPC + batch
+ response-hints wire definitions).

**Per-call flow:**

1. **Wire-to-internal conversion** — label matchers, filter, ordering,
   and limit are converted from wire types into the internal types used
   throughout the bucket store. Optional `request_hints.block_matchers`
   are extracted and used later to scope the block fan-out.
2. **Limiter setup** — a per-call series limiter is constructed (label-
   names path only).
3. **Per-block fan-out** — `errgroup` spawns one goroutine per matching
   block. Each goroutine pins its block by holding an index reader for
   the duration of the work, runs the per-block label-names / label-
   values search, applies filter+ordering+limit **per block**, and
   appends the result set plus the block's ULID to shared slices under a
   mutex.
4. **Synchronisation** — `g.Wait()` ensures every per-block goroutine has
   drained before merging; index readers are closed by their defers at
   that point.
5. **Cross-block k-way merge** — the per-block result sets are merged via
   the vendored Prometheus pairwise merge, which re-applies ordering and
   limit across blocks. The merge does **not** re-apply the filter —
   leaves already did that. If only one block matched, the merge wrapper
   is skipped (see [Single-set fast path](#single-set-fast-path)).
6. **Streaming response** — the merged (or single-set) result is drained
   into fixed-size batches and sent over the gRPC stream, preceded by a
   header-only batch carrying the set of queried block ULIDs.

**Where filter / order / limit are applied** — two distinct points inside
the store-gateway:

| Layer            | Filter                                  | Order                        | Limit             |
| ---------------- | --------------------------------------- | ---------------------------- | ----------------- |
| **Per-block**    | ✓                                       | ✓                            | ✓ (per-block cap) |
| **Cross-block**  | (pass-through, leaves already filtered) | ✓ (re-applied across blocks) | ✓ (global cap)    |

The cross-block merge calls the vendored Prometheus pairwise k-way merge
with the per-block sets and the request's ordering and limit; it re-applies
ordering and limit but not the filter.

#### Single-set fast path

When only one block matches the time range and block matchers, the merge
wrapper is skipped and the single result set is streamed directly.
Correctness depends on the per-block stage having applied
filter+order+limit in full — which it does. The single-block test fixtures
(`TestBucketStoreSearchLabelNames`, `TestBucketStoreSearchLabelValues`)
exercise this path.

**Streaming response shape**: the streaming-response writer drains the
merged (or single-set) result set and emits batches over the gRPC stream.
The first message on the wire is a **header-only batch** carrying the
`queried_blocks` response hint — this tells the querier which blocks were
actually consulted, which it needs for its consistency check. Result
batches follow, each capped at `searchBatchSize`. A trailing batch carries
any warnings.

**`request_hints.block_matchers`**: both request types carry an optional
`request_hints.block_matchers` list. When non-empty, those matchers are
evaluated against each block's labels (e.g. shard label, source label) to
scope which blocks the goroutine fan-out covers. Empty disables per-block
filtering and all blocks in the request's time range participate. This is
the channel by which the querier's per-replica block assignment (from its
consistency-check flow) reaches the store-gateway.

**Concurrency**: the per-block fan-out uses `errgroup` derived from the
request context. There is **no explicit cap** on per-block concurrency —
the store-gateway relies on the bucket store's own queue/limiter
(configured at construction) to bound the goroutine count. Each per-block
goroutine pins its block by holding an index reader for the goroutine's
lifetime (the reader is defer-closed on exit and is ref-counted by the
block), lazily ensures the index header is loaded, runs the per-block
label-names / label-values search, applies per-block hints, and appends
the resulting set to a shared collection under a mutex. `g.Wait()` is the
synchronisation point — after it returns, no per-block goroutine is
reading from a block, so all index readers are safely closed by their
defers before the merge is constructed.

**What the store-gateway does NOT do:**

- **Metadata enrichment**: the proto's `Result.Metadata` field is never
  populated by the store-gateway. Metric metadata lives only at the
  ingester (see [Metadata enrichment](#metadata-enrichment-include_metadata)).
- **Tenant federation**: per-tenant scoping happens upstream at the
  querier; the store-gateway sees one tenant per call.

## Error handling

### Ingester

- **Validation failures** (bad filter, negative limit, …) surface as
  `codes.InvalidArgument` and are returned **before** the deferred read-
  error mapper runs — otherwise they would be re-tagged as `codes.Internal`.
- **Tenant / consistency / TSDB errors** flow through the read-error
  mapper and surface with the appropriate gRPC status code.
- **Mid-stream errors from the searcher** are returned by the streaming
  loop and propagate up to the gRPC layer. The wire contract is: any
  results sent before the error are valid; the error closes the stream.

### Store-gateway

- **Invalid matchers / filter / ordering** surface as `codes.InvalidArgument`.
- **Per-block errors** abort the errgroup; the first error is returned by
  `g.Wait()`. The handler maps `context.Canceled` to `codes.Canceled` and
  anything else to `codes.Internal`.
- **Send errors** during streaming are returned from the streaming-response
  writer and propagate up; the merger's terminal error (`rs.Err()`) is
  joined with the send error so traces preserve the upstream cause.

## Benchmarks

### Ingester (`pkg/ingester/ingester_search_test.go`)

- `BenchmarkIngester_SearchLabelValues` — varies label cardinality
  (`mod_10`, `mod_77`, `mod_4199`, `__name__`), filter (none / substring /
  fuzzy Jaro-Winkler), ordering (alpha asc, alpha desc, score desc),
  limit, and `include_metadata`.
- `BenchmarkIngester_SearchLabelNames` — smaller matrix; label-name
  cardinality is fixed at the fixture's 4 names.
- `BenchmarkIngester_LegacyVsSearchLabelValues` — **the parity benchmark.**
  Same head, no filter, alpha asc, large limit; sub-cases keyed on
  `/impl=legacy` and `/impl=new` so `benchstat -col '/impl'` renders the
  cost difference directly. Use this when changing anything in the
  streaming path that might shift the new-vs-legacy gap.

All ingester benchmarks share `prepareSearchBenchmarkIngester`, which
pushes a large series count with prime-modulo labels (mirroring
`BenchmarkIngester_LabelValuesCardinality` in
`label_names_and_values_test.go`) so cardinality buckets line up across
the legacy and search benchmarks. Inspect the fixture for the exact
series count.

### Store-gateway (`pkg/storegateway/bucket_search_test.go`)

- `BenchmarkBucketStoreSearchLabelValues` — cardinality sweep across the
  fixture's low- and high-cardinality labels, with filter (none /
  substring / fuzzy), ordering, and limit varied.
- `BenchmarkBucketStoreSearchLabelNames` — small matrix; label-name
  cardinality is constant.
- `BenchmarkBucketStoreSearchLabelValuesVsLabelValues` — parity benchmark,
  same store, no filter, alpha asc, large limit; sub-cases keyed on
  `/impl=legacy` / `/impl=new` for `benchstat -col '/impl'`.

All store-gateway benchmarks share `prepareBenchmarkSearchStore`, which
builds a fixture identical to `BenchmarkBucketStoreLabelValues` in
`bucket_stores_test.go` so cardinality buckets line up across the legacy
and search benchmarks. The high-cardinality bench is **slow** to set up —
gate `-short` if you only want the fast ones.

## Performance characteristics

Several allocation-reducing optimisations are in place across the path:
batch-envelope and slice pooling in the HTTP handler, a fast-path skip of
matcher filtering in tenant federation when no id-label matcher is
present, single-source / single-set / single-tenant fast paths that
bypass the k-way merge wrapper, and per-ingester cancel funcs that avoid
closure captures in the hot loop. See the benchmark suite in
`*_search_test.go` for measured impact.
