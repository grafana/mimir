# Streaming label/value search — architecture overview

This package holds the **shared search parameters and filter primitives** used
by the experimental streaming label/value search RPCs. The wire-level RPCs and
the HTTP handler live elsewhere; this document is the **single source of
truth** for how the whole feature fits together. The per-source implementation
details are in:

- `pkg/ingester/AGENT.md` — ingester-side streaming search.
- `pkg/storegateway/AGENT.md` — store-gateway-side streaming search.

## What this feature is

Three experimental HTTP endpoints, gated by
`-querier.experimental-search-api-enabled` (default `false`), that let
clients perform substring / fuzzy filtering over label names and label
values without materialising the full result set:

| HTTP method  | Path                                  | Returns                                |
|--------------|---------------------------------------|----------------------------------------|
| `GET`/`POST` | `/api/v1/search/metric_names`         | Filtered, ordered list of metric names |
| `GET`/`POST` | `/api/v1/search/label_names`          | Filtered, ordered list of label names  |
| `GET`/`POST` | `/api/v1/search/label_values`         | Filtered, ordered list of label values |

The wire contract mirrors Prometheus PR #18573. Responses are
**NDJSON** (`application/x-ndjson`): one JSON object per line, flushed
incrementally, terminated by a status trailer.

## Top-level data flow

```
              HTTP (NDJSON)
                 │
                 ▼
  ┌──────────────────────────────────────────────────────────────────────┐
  │           pkg/querier/search_handler.go (HTTP server)                │
  │   parses URL params, builds *streaminglabelvalues.Params + hints,    │
  │   opens a Querier, streams NDJSON via streamSearchNDJSON             │
  └──────────────────────────────┬───────────────────────────────────────┘
                                 │ in-process
                                 ▼
  ┌──────────────────────────────────────────────────────────────────────┐
  │  pkg/querier/tenantfederation/merge_queryable.go (mergeQuerier)      │
  │   per-tenant fan-out via concurrency.ForEachJob;                     │
  │   k-way merge via storage.MergeSearchResultSets (vendored Prometheus)│
  │   single-job & bypassWithSingleID fast paths skip the merge wrapper  │
  └──────────────────────────────┬───────────────────────────────────────┘
                                 │ in-process (one per tenant)
                                 ▼
  ┌──────────────────────────────────────────────────────────────────────┐
  │           pkg/querier/multi_searcher.go (multiQuerier)               │
  │   fans out to all child queriers, k-way merges their result sets     │
  └────────┬──────────────────────────────────────────────────┬──────────┘
           │                                                  │
           │ in-process                                       │ in-process
           ▼                                                  ▼
  ┌──────────────────────────┐                ┌─────────────────────────────────┐
  │ pkg/querier/             │                │ pkg/querier/                    │
  │   distributor_queryable_ │                │   blocks_store_queryable_search │
  │       search.go          │                │ blocksStoreQuerier.SearchLabel* │
  │ distributorQuerier       │                │                                 │
  │   .SearchLabel*          │                │                                 │
  │   ↓ (in-process call)    │                │                                 │
  │ pkg/distributor/         │                │ fan out to store-gateways       │
  │   distributor_search.go  │                │   (gRPC stream)                 │
  │ Distributor.SearchLabel* │                │ → bucketSearcherResultSet       │
  │                          │                │ → k-way merge per-SG sources    │
  │ fan out to quorum-many   │                │                                 │
  │ ingesters (gRPC stream)  │                │                                 │
  │ → ingesterSearchResultSet│                │                                 │
  │ → prefetcher (256 buf)   │                │                                 │
  │ → MergeSearchResultSets  │                │                                 │
  └────────┬─────────────────┘                └────────┬────────────────────────┘
           │ gRPC stream                               │ gRPC stream
           │ (server-streaming)                        │ (server-streaming)
           │                                           │
           │ NB: BOTH boxes above run inside the       │
           │ QUERIER process. `pkg/distributor` is a   │
           │ shared library here (the read-path        │
           │ client wrapper), not the distributor      │
           │ service that handles writes.              │
           ▼                                           ▼
  ┌──────────────────────────┐                ┌─────────────────────────────────┐
  │ pkg/ingester/            │                │ pkg/storegateway/               │
  │   ingester_search.go     │                │   bucket_search.go              │
  │                          │                │                                 │
  │ Ingester.SearchLabel*    │                │ BucketStore.SearchLabel*        │
  │ (TSDB head, single src)  │                │ per-block fan-out (errgroup)    │
  │                          │                │ k-way merge per-block sources   │
  │ → streamSearchResults    │                │ → streamBucketSearchResults     │
  │   (256-result batches)   │                │   (256-result batches)          │
  └──────────────────────────┘                └─────────────────────────────────┘
```

## Where filtering, ordering, limit, and k-way merge happen

There are **two distinct layers** where these operations are applied:

1. **Leaf (per-source) layer** — at the ingester (single in-memory TSDB head)
   and at the store-gateway (per-block, fan-out within the store-gateway).
   Each leaf applies the full `Filter+OrderBy+Limit` to its own data.
2. **Merge (k-way) layer** — at the distributor (over quorum-many ingester
   streams), at the blocks-store querier (over per-store-gateway streams),
   at the multi-searcher (over distributor+blocks-store sources), and at
   the tenant-federation layer (over per-tenant sources).

| Layer                          | Filter? | Order?     | Limit?            | K-way merge?                 |
|--------------------------------|---------|------------|-------------------|------------------------------|
| Ingester (`ingester_search.go`) | ✓       | ✓          | ✓                 | n/a (one source)             |
| Store-gateway per-block        | ✓       | ✓          | ✓ (per-block cap) | n/a (one block)              |
| Store-gateway (across blocks)  |         |            | enforced by merge | `MergeSearchResultSets`      |
| Distributor (over ingesters)   |         |            | enforced by merge | `MergeSearchResultSets`      |
| Blocks-store querier (over SGs)|         |            | enforced by merge | `MergeSearchResultSets`      |
| Multi-searcher (querier fan)   |         |            | enforced by merge | `MergeSearchResultSets`      |
| Tenant federation              |         |            | enforced by merge | `MergeSearchResultSets`      |
| HTTP handler                   |         |            | enforced by `limit+1` probe | n/a                |

The filter / ordering parameters (`Params` from this package and
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

## Wire protocols and batch sizes

| Boundary                          | Protocol      | Batch size                                                  |
|-----------------------------------|---------------|-------------------------------------------------------------|
| Client ↔ querier (HTTP handler)   | **NDJSON over HTTP** (chunked, flushed per batch line) | `batch_size` URL param, **default 100**, max 10000 (`searchDefaultBatchSize` / `maxSearchBatchSize` in `pkg/querier/search_handler.go`) |
| Querier ↔ distributor             | in-process (Go function call) | n/a                                                |
| Distributor ↔ ingester            | **gRPC server-streaming** (`rpc SearchLabel{Names,Values} returns (stream SearchResultBatch)`) | **256 results per `*client.SearchResultBatch`** (`searchBatchSize` in `pkg/ingester/ingester_search.go`) |
| Distributor's prefetch buffer     | Go channel    | **256 elements** (`ingesterSearchPrefetchBuffer` in `pkg/distributor/distributor_search.go`) — one wire batch in flight per ingester |
| Querier ↔ store-gateway           | **gRPC server-streaming** | **256 results per `*storepb.SearchResultBatch`** (`searchBatchSize` in `pkg/storegateway/bucket_search.go`) |

The HTTP handler batches **`batch_size` records per NDJSON line** (default 100,
caller-configurable up to 10000). Each batch line is written to the response
writer and `http.Flusher.Flush()` is called immediately so the client sees
results incrementally.

The gRPC stream batches **256 results per `SearchResultBatch` proto message**.
That constant is shared between ingester and store-gateway and is the
production-tuned trade-off between gRPC framing overhead and memory footprint.

The querier-side prefetcher (`pkg/storage/concurrent_searchresult.go`) wraps
each gRPC stream with a **256-element Go channel** so the merge layer can
read from one stream while another stream's `Recv()` is in flight.

## Tenant federation

`pkg/querier/tenantfederation/merge_queryable.go` wraps the multi-searcher with
per-tenant fan-out. Two fast paths short-circuit the merge:

- **`bypassWithSingleID && len(ids) == 1`**: forwards directly to the upstream
  single-tenant call without injecting the synthetic id label.
- **`len(jobs) == 1` after matcher filtering** (added in this PR): also
  bypasses the merge wrapper for any single-tenant fan-out, regardless of
  the bypass flag, since `MergeSearchResultSets` over a single source is
  pass-through.

Multi-tenant fan-out uses `concurrency.ForEachJob` with a per-mergeQuerier
`maxConcurrency` cap. Result sets are collected in a pre-sized
`[]storage.SearchResultSet` indexed by job index and merged via
`MergeSearchResultSets` after `ForEachJob` returns.

## Metadata enrichment

The `include_metadata=true` URL param on `/api/v1/search/metric_names`
asks for per-metric `Type/Help/Unit` metadata to be inlined on each result.
This enrichment is **applied only at the ingester** and **only when the
label being searched is `__name__`** — store-gateway has no metric
metadata to enrich with. See `pkg/ingester/AGENT.md` for details.

## Performance characteristics (post-optimisation)

The PR landed with several allocation-reducing optimisations:

- HTTP handler: per-request `searchBatchEnvelope` + slice are pooled
  (`searchBatchPool` / `searchBatchPoolScored`); score-bearing and
  non-score-bearing wire records are distinct struct types so the score
  field can be an inline `float64` rather than a heap `*float64`.
- Federation: fast path skips `FilterValuesByMatchers` when no id-label
  matcher is present; single-job fan-out skips the merge wrapper.
- Distributor: per-ingester cancel funcs live on the result-set struct
  rather than in a captured closure.
- Store-gateway: single-set fast path skips the merge wrapper.

See the per-source AGENT.md files and the benchmark suite in
`*_search_test.go` files for measured before/after numbers.
