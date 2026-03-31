# Search Request Flow

This document describes the end-to-end flow of a search request through the Mimir stack, covering container boundaries, interface layers, merge/dedup logic, and the shared `SearchValueSet` iterator.

## Endpoints

Three HTTP endpoints are exposed on the querier (routed by `pkg/api/handlers.go`):

| Path | Handler |
|------|---------|
| `GET/POST /api/v1/search/metric_names` | `querier.SearchMetricNamesHandler` |
| `GET/POST /api/v1/search/label_names` | `querier.SearchLabelNamesHandler` |
| `GET/POST /api/v1/search/label_values` | `querier.SearchLabelValuesHandler` |

---

## Container Boundaries and File Map

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  HTTP CLIENT                                                                 │
│  GET /api/v1/search/label_values?search=foo&sort_by=score&limit=100          │
└──────────────────────────────────┬───────────────────────────────────────────┘
                                   │ NDJSON streaming response
                                   ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  QUERY-FRONTEND  (pkg/frontend/)                                             │
│                                                                              │
│  querymiddleware/codec.go                                                    │
│  ① DecodeSearchQueryRequest — parses & validates the HTTP request            │
│  scheduler_processor.go                                                      │
│  ② Routes to a querier worker via gRPC QueryResultStream                     │
│  ③ isSearchQueryPath → runStreamingHttpRequest                               │
│     – pipes the gRPC stream directly back to the HTTP client                 │
│     – no buffering: each NDJSON batch is forwarded immediately               │
└──────────────────────────────────┬───────────────────────────────────────────┘
                                   │ gRPC (QueryResultStream)
                                   ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  QUERIER  (pkg/querier/)                                                     │
│                                                                              │
│  search_handler.go : doSearchHandler                                         │
│  ① parseSearchParams → searchParams                                          │
│  ② queryable.Querier() → q.(MimirSearcher) [multiQuerier]                   │
│  ③ buildMimirSearchHints(params) → *MimirSearchHints                         │
│  ④ searchExec → searcher.SearchLabelValues / SearchLabelNames                │
│  ⑤ streamResults → iterates SearcherValueSet → NDJSON batches               │
│                                                                              │
│  querier.go : multiQuerier.SearchLabel{Names,Values}                         │
│  ① clamp limit to MaxLabelNamesLimit / MaxLabelValuesLimit per-tenant        │
│  ② getQueriers() → [ blocksStoreQuerier, distributorQuerier ]                │
│  ③ fanOutSearch(ctx, hints, searchers, call) → SearcherValueSet              │
│                                                                              │
│  multi_searcher.go : fanOutSearch                                            │
│  ① opens each sub-stream sequentially: call(ctx, searcher, hints)            │
│  ② SortBy != 0 → KWayMergeValueSets  (heap merge of pre-sorted streams)     │
│     SortBy == 0 → UnsortedDedupValueSets (concurrent fan-out + dedup)       │
│  ③ returns *labelSearchStream{ch: outCh}                                     │
│                                                                              │
│  ┌───────────────────────────────────┐  ┌──────────────────────────────────┐ │
│  │  blocks_store_queryable_search.go │  │  distributor_queryable_search.go │ │
│  │  blocksStoreQuerier               │  │  distributorQuerier              │ │
│  │                                   │  │                                  │ │
│  │  SearchLabel{Names,Values}:        │  │  SearchLabel{Names,Values}:      │ │
│  │  ① mimirHintsToStoreSearchFilter  │  │  ① check QueryIngestersWithin    │ │
│  │     → storepb.SearchFilter        │  │     (skip if too old)            │ │
│  │  ② searchHintsToLabelHints        │  │  ② delegate to                   │ │
│  │     (limit only if no sort)       │  │     Distributor.SearchLabel*     │ │
│  │  ③ NewSearchValueSet wrapping     │  │  ③ return the distributor's      │ │
│  │     fetchLabel*FromStoreStreaming  │  │     SearcherValueSet directly    │ │
│  │  – produce fan-outs to store-     │  │                                  │ │
│  │    gateways, streams FilteredResult│  │  Returns distributorSearchValueSet│ │
│  │    {Value, Score} into channel    │  │  (backed by KWayMerge /          │ │
│  │  ④ SearchValueSet: final dedup +  │  │   UnsortedDedup over ingesters)  │ │
│  │    sort + limit across gateways   │  │                                  │ │
│  └───────────────────────────────────┘  └──────────────────────────────────┘ │
└──────────────┬──────────────────────────────────────┬────────────────────────┘
               │ gRPC SearchLabel{Names,Values}        │ gRPC SearchLabel{Names,Values}
               ▼                                       ▼
┌──────────────────────────────┐    ┌──────────────────────────────────────────┐
│  STORE-GATEWAY               │    │  DISTRIBUTOR → INGESTERS                 │
│  (pkg/storegateway/)         │    │  (pkg/distributor/ + pkg/ingester/)      │
│  bucket_search.go            │    │                                          │
│                              │    │  distributor.go:                         │
│  SearchLabel{Names,Values}:  │    │  SearchLabel{Names,Values}:              │
│  ① buildSearchFilter from    │    │  ① fan out SearchLabelValuesRequest to   │
│     storepb.SearchFilter     │    │     each ingester replication set        │
│  ② fan out per TSDB block    │    │  ② wrap each gRPC stream in             │
│     (concurrent goroutines)  │    │     ingesterSearch{Names,Values}ValueSet │
│  ③ iterate block results;    │    │  ③ KWayMerge or UnsortedDedup across    │
│     Accept(v) per value to   │    │     ingester streams → outCh             │
│     filter and capture       │    │  ④ return distributorSearchValueSet      │
│     fuzzy score              │    │                                          │
│  ④ push FilteredResult into  │    │  ingester_search.go:                     │
│     SearchValueSet channel   │    │  SearchLabel{Names,Values}:              │
│  ⑤ SearchValueSet:           │    │  ① buildSearchFilter from               │
│     – dedup across blocks    │    │     SearchLabelValuesFilter proto        │
│     – sort via drainAndSort  │    │  ② enforce MaxLabel{Names,Values}Limit   │
│     – enforce byte+count     │    │  ③ produce from TSDB head + blocks:     │
│       limits                 │    │     • label values: LabelValuesFor()    │
│  ⑥ streamStoreSearchResults  │    │       streaming reader + Accept()       │
│     → StoreSearchResult      │    │       to capture fuzzy score            │
│       batches via gRPC       │    │     • label names: LabelNames() +       │
│                              │    │       Accept() per name for score       │
│                              │    │  ④ SearchValueSet: dedup + sort +       │
│                              │    │     limit with optional warning         │
│                              │    │  ⑤ stream SearchLabelValuesResult       │
│                              │    │     {Value, Score} batches via gRPC     │
└──────────────────────────────┘    └──────────────────────────────────────────┘
```

---

## What Each Adapter File Does

### `blocks_store_queryable_search.go` — `blocksStoreQuerier`

Bridges `MimirSearcher` to store-gateway gRPC RPCs.

- `mimirHintsToStoreSearchFilter(hints)` converts `MimirSearchHints` to `storepb.SearchFilter` — carries search terms, operator, fuzz threshold, case sensitivity, sort params
- `searchHintsToLabelHints(hints)` converts the limit for the gRPC `LabelHints`; **limit is not pushed when sorting is requested** (all results needed for a correct global sort)
- `SearchLabel{Names,Values}` returns a `SearchValueSet` whose producer calls `queryWithConsistencyCheck` → `fetchLabel{Names,Values}FromStoreStreaming`
- `fetchLabel*FromStoreStreaming` fans out to multiple store-gateway clients concurrently; each result batch is forwarded as `FilteredResult{Value: r.Value, Score: r.Score}` into the channel
- The `SearchValueSet` at this layer provides final cross-store-gateway deduplication, sorting, and limit enforcement

### `distributor_queryable_search.go` — `distributorQuerier`

Thin adapter between `MimirSearcher` and `Distributor`.

- Applies `QueryIngestersWithin`: returns `emptySearcherValueSet` if the query time range is outside the ingester retention window
- Clamps `minT` to avoid querying further back than ingesters hold data
- Delegates directly to `Distributor.SearchLabel{Names,Values}`; the distributor returns a `SearcherValueSet` already backed by a merge of ingester streams

---

## MimirSearcher Interface

`MimirSearcher` (`pkg/storage/searcher.go`) is the common interface implemented at every layer:

```go
type MimirSearcher interface {
    SearchLabelNames(ctx context.Context, hints *MimirSearchHints,
        matchers ...*labels.Matcher) (SearcherValueSet, annotations.Annotations, error)
    SearchLabelValues(ctx context.Context, name string, hints *MimirSearchHints,
        matchers ...*labels.Matcher) (SearcherValueSet, annotations.Annotations, error)
}
```

| Type | File | Delegates to |
|------|------|--------------|
| `multiQuerier` | `querier.go` | `blocksStoreQuerier` + `distributorQuerier` via `fanOutSearch` |
| `memoryTrackingQuerier` | `memory_tracking_queryable.go` | inner `MimirSearcher` |
| `LazyQuerier` | `pkg/storage/lazyquery/lazyquery.go` | inner `MimirSearcher` |
| `blocksStoreQuerier` | `blocks_store_queryable_search.go` | store-gateway gRPC |
| `distributorQuerier` | `distributor_queryable_search.go` | `Distributor.SearchLabel{Names,Values}` |
| `Distributor` | `pkg/distributor/distributor.go` | ingester gRPC (one per replication set) |

`MimirSearchHints` carries serialisable search parameters forwarded to every node:

```go
type MimirSearchHints struct {
    Search          []string  // search terms
    CaseInsensitive bool
    FuzzThreshold   float64
    Operator        int  // 0=OR, 1=AND
    SortBy          int  // 0=none, 1=alpha, 2=score
    SortOrder       int  // alpha: 0=A→Z, 1=Z→A  |  score: 0=best-first, 1=worst-first
    Limit           int
}
```

---

## SearchValueSet — Shared Streaming Iterator

`SearchValueSet` (`pkg/storage/search_valueset.go`) is the **single iterator type used at every layer**. Each node runs its own instance; the querier merges across node instances.

```
Producer goroutine ──chan FilteredResult──► SearchValueSet ──Next()/At()──► consumer
```

`FilteredResult` carries value and fuzzy-match score end-to-end:

```go
type FilteredResult struct {
    Value string
    Score float64  // -1.0 = exact substring match; 0–1 = Jaro similarity; 0.0 = no filter
}
```

**Unsorted path** (`SortBy == 0`):
- `nextUnsorted()` reads from `ch`, deduplicates by `Value`, enforces byte and count limits inline
- When count limit is reached, `limitHit = true`; any `limitExceededWarning` is merged into `Warnings()`
- `LimitReached()` lets `streamResults` in the search handler set `has_more: true` in the NDJSON response

**Sorted path** (`SortBy != 0`):
- `drainAndSort()` buffers all de-duplicated items from `ch`, sorts via `sortFilteredResultsInline`, truncates to `limit`
- Score-sort: `SortOrder=0` → highest score first; `SortOrder=1` → lowest score first
- Alpha-sort: `SortOrder=0` → A→Z; `SortOrder=1` → Z→A

`NewSearchValueSet` is used by:
- `BucketStore.SearchLabel{Names,Values}` — per-node sort/dedup on the store-gateway
- `blocksStoreQuerier.SearchLabel{Names,Values}` — cross-store-gateway dedup/sort at the querier
- `newingesterSearcherValueSet` — per-ingester sort/dedup

---

## Merge and Dedup — Shared Library Functions

`pkg/storage/search_merge.go` provides two shared functions used by **both** `fanOutSearch` (querier) and `Distributor.SearchLabel{Names,Values}`:

### `UnsortedDedupValueSets` — used when `SortBy == 0`

- Launches one goroutine per `SearcherValueSet`
- Deduplicates by `Value` across all streams into one shared set
- Count limit enforced eagerly: when `len(seen) >= limit`, `limitReached = true` and context is cancelled
- O(n) memory for the dedup map; O(1) per item otherwise

### `KWayMergeValueSets` — used when `SortBy != 0`

- Requires each sub-stream to return items in the same sort order (sort hints are pushed to all sub-nodes)
- Maintains a min-heap of size k — O(log k) per emitted item
- Deduplicates while merging; count limit enforced after each emit

Both write `FilteredResult` to `outCh chan<- FilteredResult` and set `*limitReached`. The caller wraps the channel in a `labelSearchStream` (querier) or `distributorSearchValueSet` (distributor).

### Querier fan-out

```
fanOutSearch(hints, searchers, call)
       │
       ├─ SortBy != 0 ──► KWayMergeValueSets   (heap merge, pre-sorted sub-streams)
       │
       └─ SortBy == 0 ──► UnsortedDedupValueSets  (concurrent + dedup + eager limit)
                                    │
                        *labelSearchStream{ch: outCh}
```

### Distributor fan-out

```
Ingester 1 sorted stream ──ingesterSearchNamesValueSet──┐
Ingester 2 sorted stream ──ingesterSearchNamesValueSet──┼──► KWayMerge / UnsortedDedup ──► distributorSearchValueSet
Ingester N sorted stream ──ingesterSearchNamesValueSet──┘
```

---

## Score Propagation

| Layer | Score source |
|-------|-------------|
| **Ingester** label values | `searchFilter.Accept(v)` in `produceLabelValuesFromReader` |
| **Ingester** label names | `searchFilter.Accept(v)` in `produceLabelNames` per value |
| **Store-gateway** | `searchFilter.Accept(v)` in `produceSearchLabel{Names,Values}` per block result |
| **`blocksStoreQuerier`** | forwards `r.Score` from `StoreSearchResult` proto |
| **`KWayMerge` / `UnsortedDedup`** | preserve `FilteredResult` verbatim |
| **`SearchValueSet`** | clones `Value` (memory safety), preserves `Score` |
| **HTTP response** | `streamResults` → `writer.factory(vs.At())` → JSON `{"Value":"...","Score":0.85}` |

Score semantics:
- `-1.0`: exact substring match (`FilterContains`); labelled `unscored` — distinct from a relevance ranking
- `0.0–1.0`: Jaro similarity (`FilterJaro`); higher = more similar to the search term
- `0.0`: no filter applied, or store-gateway path

---

## NDJSON Response Format

```
{"results": [{"Value": "pod", "Score": 0.85}, {"Value": "namespace", "Score": 0.72}]}
{"results": [{"Value": "container", "Score": 0.61}]}
{"status": "success", "has_more": false, "warnings": ["..."]}
```

- One or more `results` batch lines, each flushed immediately as the batch fills
- A final `status` line: `has_more: true` when the limit was reached with more data available; optional `warnings` array
- On error after streaming has started: `{"status": "error", "error": "..."}`
