# Streaming label/value search — architecture overview

The `pkg/streaminglabelvalues` package holds the **shared search parameters and
filter primitives** used by the experimental streaming label/value search RPCs.
The wire-level RPCs and the HTTP handler live elsewhere; this document is the
**single source of truth** for how the whole feature fits together. The
per-source implementation details are in:

- [`ingester.md`](./ingester.md) — ingester-side streaming search.
- [`storegateway.md`](./storegateway.md) — store-gateway-side streaming search.

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

The wire contract mirrors Prometheus PR #18573. Responses are
**NDJSON** (`application/x-ndjson`): one JSON object per line, flushed
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

| Layer                       | Code                                                   |
| --------------------------- | ------------------------------------------------------ |
| HTTP handler                | `pkg/querier/search_handler.go`                        |
| Tenant federation           | `pkg/querier/tenantfederation/merge_queryable.go`      |
| Multi-searcher              | `pkg/querier/multi_searcher.go`                        |
| Distributor querier wrapper | `pkg/querier/distributor_queryable_search.go`          |
| In-process distributor call | `pkg/distributor/distributor_search.go`                |
| Blocks-store querier        | `pkg/querier/blocks_store_queryable_search.go`         |
| Ingester                    | `pkg/ingester/ingester_search.go` (see `ingester.md`)  |
| Store-gateway               | `pkg/storegateway/bucket_search.go` (see `storegateway.md`) |
| Per-stream prefetcher       | `pkg/storage/concurrent_searchresult.go`               |
| Vendored k-way merge        | `vendor/github.com/prometheus/prometheus/storage/`     |

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
| Ingester (`ingester_search.go`) | ✓       | ✓      | ✓                           | n/a (one source)        |
| Store-gateway per-block         | ✓       | ✓      | ✓ (per-block cap)           | n/a (one block)         |
| Store-gateway (across blocks)   |         |        | enforced by merge           | `MergeSearchResultSets` |
| Distributor (over ingesters)    |         |        | enforced by merge           | `MergeSearchResultSets` |
| Blocks-store querier (over SGs) |         |        | enforced by merge           | `MergeSearchResultSets` |
| Multi-searcher (querier fan)    |         |        | enforced by merge           | `MergeSearchResultSets` |
| Tenant federation               |         |        | enforced by merge           | `MergeSearchResultSets` |
| HTTP handler                    |         |        | enforced by `limit+1` probe | n/a                     |

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

## Wire protocols and batch granularity

| Boundary                        | Protocol                            | Batch granularity                                                                                       |
| ------------------------------- | ----------------------------------- | ------------------------------------------------------------------------------------------------------- |
| Client ↔ querier (HTTP handler) | NDJSON over HTTP, chunked + flushed | Caller-controlled via `batch_size` URL param, defaulted and capped by constants in the HTTP handler     |
| Querier ↔ distributor querier   | in-process                          | n/a                                                                                                     |
| Distributor querier ↔ ingester  | gRPC server-streaming               | Fixed (`searchBatchSize`) results per `SearchResultBatch` proto message                                 |
| Distributor's prefetch buffer   | Go channel                          | One wire batch in flight per ingester (`ingesterSearchPrefetchBuffer`)                                  |
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

## Metadata enrichment

The `include_metadata=true` URL param on `/api/v1/search/metric_names`
asks for per-metric `Type/Help/Unit` metadata to be inlined on each result.
This enrichment is **applied only at the ingester** and **only when the
label being searched is `__name__`** — store-gateway has no metric
metadata to enrich with. See [`ingester.md`](./ingester.md) for details.

## Performance characteristics

Several allocation-reducing optimisations are in place across the path:
batch-envelope and slice pooling in the HTTP handler, a fast-path skip of
matcher filtering in tenant federation when no id-label matcher is present,
single-source / single-set / single-tenant fast paths that bypass the
k-way merge wrapper, and per-ingester cancel funcs that avoid closure
captures in the hot loop. See the per-source docs
([`ingester.md`](./ingester.md), [`storegateway.md`](./storegateway.md))
and the benchmark suite in `*_search_test.go` for measured impact.
