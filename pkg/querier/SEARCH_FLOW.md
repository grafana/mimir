# Search Request Flow

This document describes the end-to-end flow of a search request through the Mimir stack, covering container boundaries, interface layers, merge/dedup logic, and the shared iterator types.

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
│  ⑤ streamResults → iterates SearchResultSet → NDJSON batches                │
│     secondary limit guard; primary enforcement is in KWayMerge/UnsortedDedup│
│                                                                              │
│  querier.go : multiQuerier.SearchLabel{Names,Values}                         │
│  ① clamp limit to MaxLabelNamesLimit / MaxLabelValuesLimit per-tenant        │
│  ② getQueriers() → [ blocksStoreQuerier, distributorQuerier ]                │
│  ③ type-assert each querier to MimirSearcher (returns error set on failure)  │
│  ④ fanOutSearch(ctx, hints, searchers, call) → SearchResultSet               │
│                                                                              │
│  multi_searcher.go : fanOutSearch                                            │
│  ① opens each sub-stream sequentially: call(ctx, searcher, hints)            │
│     full hints (incl. SortBy/SortOrder) forwarded so sub-streams pre-sort   │
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
│  │     fetchLabel*FromStoreStreaming  │  │     SearchResultSet directly     │ │
│  │  – fans out to store-gateways,    │  │                                  │ │
│  │    streams SearchResult{Value,    │  │  Returns SearchResultSet         │ │
│  │    Score} into channel            │  │  (backed by KWayMerge /          │ │
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
│     (concurrent goroutines)  │    │     ingesterSearchValueSet               │
│  ③ iterate block results;    │    │  ③ KWayMerge or UnsortedDedup across    │
│     Accept(v) per value to   │    │     ingester streams → outCh             │
│     filter and capture       │    │  ④ return SearchResultSet via            │
│     fuzzy score              │    │     fanOutSearch helper                  │
│  ④ push SearchResult into    │    │                                          │
│     SearchValueSet channel   │    │  ingester_search.go:                     │
│  ⑤ SearchValueSet:           │    │  SearchLabel{Names,Values}:              │
│     – dedup across blocks    │    │  ① buildSearchFilter from               │
│     – sort via drainAndSort  │    │     SearchLabelValuesFilter proto        │
│     – enforce byte+count     │    │  ② enforce MaxLabel{Names,Values}Limit   │
│       limits                 │    │  ③ produce from TSDB head + blocks:     │
│  ⑥ streamStoreSearchResults  │    │     • label values: LabelValuesFor()    │
│     → StoreSearchResult      │    │       streaming reader + Accept()       │
│       batches via gRPC       │    │       to capture fuzzy score            │
│                              │    │     • label names: LabelNames() +       │
│                              │    │       Accept() per name for score       │
│                              │    │  ④ ingesterSearcherValueSet:            │
│                              │    │     dedup + sort (drainAndSort) + limit │
│                              │    │  ⑤ stream SearchLabelValuesResult       │
│                              │    │     {Value, Score} batches via gRPC     │
└──────────────────────────────┘    └──────────────────────────────────────────┘
```

---

## What Each Adapter File Does

### `blocks_store_queryable_search.go` — `blocksStoreQuerier`

Bridges `MimirSearcher` to store-gateway gRPC RPCs.

- `mimirHintsToStoreSearchFilter(hints)` converts `MimirSearchHints` to `storepb.SearchFilter` — carries search terms, fuzz algorithm, fuzz threshold, case sensitivity, sort params
- `searchHintsToLabelHints(hints)` converts the limit for the gRPC `LabelHints`; **limit is not pushed when sorting is requested** (all results needed for a correct global sort)
- `SearchLabel{Names,Values}` returns a `SearchValueSet` whose producer calls `queryWithConsistencyCheck` → `fetchLabel{Names,Values}FromStoreStreaming`
- `fetchLabel*FromStoreStreaming` fans out to multiple store-gateway clients concurrently; each result batch is forwarded as `SearchResult{Value, Score}` into the channel
- The `SearchValueSet` at this layer provides final cross-store-gateway deduplication, sorting, and limit enforcement

### `distributor_queryable_search.go` — `distributorQuerier`

Thin adapter between `MimirSearcher` and `Distributor`.

- Applies `QueryIngestersWithin`: returns `emptySearcherValueSet` if the query time range is outside the ingester retention window
- Clamps `minT` to avoid querying further back than ingesters hold data
- Delegates directly to `Distributor.SearchLabel{Names,Values}`; the distributor returns a `SearchResultSet` already backed by a merge of ingester streams

---

## MimirSearcher Interface

`MimirSearcher` (`pkg/storage/temporary_searcher_interfaces.go`) is the common interface implemented at every layer:

```go
type MimirSearcher interface {
    SearchLabelNames(ctx context.Context, hints *MimirSearchHints,
        matchers ...*labels.Matcher) (SearchResultSet, annotations.Annotations)
    SearchLabelValues(ctx context.Context, name string, hints *MimirSearchHints,
        matchers ...*labels.Matcher) (SearchResultSet, annotations.Annotations)
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
    Search          []string       // search terms (OR logic)
    CaseInsensitive bool
    FuzzAlg         string         // "jarowinkler" | "subsequence" | ""
    FuzzThreshold   float64        // [0.0, 1.0]; 0 = no fuzzy matching
    SortBy          SortBy         // 0=none, 1=alpha, 2=score
    SortOrder       SortDirection  // 0=asc (A→Z / best-first), 1=desc (Z→A / worst-first)
    Limit           int
}
```

---

## Iterator Types

### `SearchResultSet` — common iterator interface

`SearchResultSet` (`pkg/storage/temporary_searcher_interfaces.go`) is the iterator interface used at every layer:

```go
type SearchResultSet interface {
    Next() bool
    At() SearchResult
    Warnings() annotations.Annotations
    Err() error
    Close() error
}
```

`SearchResult` carries value and fuzzy-match score end-to-end:

```go
type SearchResult struct {
    Value string
    Score float64  // -1.0 = exact substring match (unscored); 0–1 = fuzzy score; 0 = no filter applied
}
```

### `SearchValueSet` — producer-goroutine backed iterator

`SearchValueSet` (`pkg/storage/search_valueset.go`) is a concrete `SearchResultSet` backed by a producer goroutine. Used at the store-gateway and ingester layers for per-node dedup, sort, and limit enforcement.

```
Producer goroutine ──chan SearchResult──► SearchValueSet ──Next()/At()──► consumer
```

**Unsorted path** (`SortBy == 0`):
- `nextUnsorted()` reads from `ch`, deduplicates by `Value`, enforces byte and count limits inline
- When count limit is reached, `limitHit = true`; any `limitExceededWarning` is merged into `Warnings()`
- `LimitReached()` lets `streamResults` in the search handler set `has_more: true` in the NDJSON response

**Sorted path** (`SortBy != 0`):
- `drainAndSort()` buffers all de-duplicated items from `ch`, sorts via `sortFilteredResultsInline`, truncates to `limit`
- Score-sort: descending score (best first) by default; Alpha-sort: A→Z by default
- Sort direction is controlled by `SortOrder` from the hints

`NewSearchValueSet` is used by:
- `BucketStore.SearchLabel{Names,Values}` — per-node sort/dedup on the store-gateway
- `blocksStoreQuerier.SearchLabel{Names,Values}` — cross-store-gateway dedup/sort at the querier
- `newingesterSearcherValueSet` — per-ingester sort/dedup on the ingester

### `labelSearchStream` — channel-backed stream at the querier

`labelSearchStream` (`pkg/querier/multi_searcher.go`) is the concrete `SearchResultSet` returned by `fanOutSearch`. The merge goroutine (`KWayMergeValueSets` or `UnsortedDedupValueSets`) writes to a buffered channel; `labelSearchStream` reads from it.

- `LimitReached()` lets `streamResults` detect `has_more: true` without peeking the channel
- `Close()` cancels the merge goroutine and drains the channel to unblock it

---

## Merge and Dedup — Shared Library Functions

`pkg/storage/search_merge.go` provides two shared functions used by **both** `fanOutSearch` (querier) and `Distributor.SearchLabel{Names,Values}`:

### `UnsortedDedupValueSets` — used when `SortBy == 0`

- Launches one goroutine per `SearchResultSet`
- Deduplicates by `Value` (xxhash) across all streams into one shared set
- Count limit enforced eagerly: when `len(seen) >= limit`, `limitReached = true` and context is cancelled
- O(n) memory for the dedup map; O(1) per item otherwise

### `KWayMergeValueSets` — used when `SortBy != 0`

- **Requires each sub-stream to return items in the same sort order** — sort hints are forwarded in full to all sub-nodes so their streams arrive pre-sorted
- Launches one goroutine per sub-stream; goroutines respect context cancellation
- Maintains a heap of size k — O(log k) per emitted item
- Deduplicates while merging; count limit enforced after each emit

Both write `SearchResult` to `outCh chan<- SearchResult` and set `*limitReached`. The caller wraps the channel in a `labelSearchStream` (querier) or uses the result of `fanOutSearch` directly (distributor).

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
Ingester 1 ──ingesterSearchValueSet──┐
Ingester 2 ──ingesterSearchValueSet──┼──► KWayMerge / UnsortedDedup ──► SearchResultSet
Ingester N ──ingesterSearchValueSet──┘
```

---

## Score Propagation

| Layer | Score source |
|-------|-------------|
| **Ingester** label values | `searchFilter.Accept(v)` in `produceLabelValues` |
| **Ingester** label names | `searchFilter.Accept(v)` in `produceLabelNames` per name |
| **Store-gateway** | `searchFilter.Accept(v)` in block result iteration |
| **`blocksStoreQuerier`** | forwards `r.Score` from `StoreSearchResult` proto |
| **`KWayMerge` / `UnsortedDedup`** | preserve `SearchResult` verbatim |
| **`SearchValueSet`** | clones `Value` (memory safety), preserves `Score` |
| **HTTP response** | `streamResults` → `writer.factory(vs.At())` → JSON `{"name":"...","score":0.85}` |

Score semantics:
- `-1.0`: exact substring match (`FilterContains`); labelled `unscored` — short-circuits fuzzy scoring in `FilterChain.Accept`
- `0.0–1.0`: fuzzy score (`FilterJaro` / `FilterSubsequence`); higher = more similar to the search term
- `0.0`: rejected by all filters (`noscore`), or no filter applied

---

## NDJSON Response Format

```
{"results": [{"name": "pod", "score": 0.85}, {"name": "namespace", "score": 0.72}]}
{"results": [{"name": "container", "score": 0.61}]}
{"status": "success", "has_more": false, "warnings": ["..."]}
```

- One or more `results` batch lines, each flushed immediately as the batch fills
- A final `status` line: `has_more: true` when the limit was reached with more data available; optional `warnings` array
- On error after streaming has started: `{"status": "error", "error": "..."}`
