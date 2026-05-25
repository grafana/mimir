# Ingester — streaming label/value search RPCs

This document covers the ingester-side implementation of the streaming
label/value search RPCs (`Ingester.SearchLabelNames`,
`Ingester.SearchLabelValues`). For the overall feature architecture see
[`pkg/streaminglabelvalues/AGENT.md`](../streaminglabelvalues/AGENT.md).

## Scope

The ingester is a **leaf source** in the search fan-out. It exposes two
gRPC server-streaming RPCs (`stream SearchResultBatch`) over the existing
ingester gRPC server. Each RPC reads from the per-tenant TSDB head — a
**single in-memory source** — and streams batches of results back to the
gRPC client.

**Who is the gRPC client?** Despite the file location `pkg/distributor/`,
the search RPC's fan-out code is the **read-path client wrapper** that
runs inside the **querier** process (and any other component that
embeds the querier, e.g. ruler). The querier's `distributorQuerier`
(`pkg/querier/distributor_queryable_search.go`) holds a `*Distributor`
reference and calls into `Distributor.SearchLabel{Names,Values}` in
`pkg/distributor/distributor_search.go`, which is what opens the gRPC
stream to this ingester. The distributor *service* (write path) is not
on the read-path call chain at all — the `Distributor` type is shared
code, not a network hop.

The ingester applies the full filter / order / limit per call. There is no
k-way merge inside the ingester because there is only one source (the
head). Filtering, ordering, and limit propagate from the wire hints
through to Prometheus's `storage.Searcher` interface on the head.

## Files

- `pkg/ingester/ingester_search.go` — RPC handlers, the
  `streamSearchResults` loop, metadata-enrichment decorator, and
  proto/internal-types converters (`buildSearchHints`, `protoToParams`,
  `protoToOrdering`).
- `pkg/ingester/client/ingester.proto` — wire definitions for the RPCs
  and the `SearchResultBatch` carrier.

## Per-call flow

```
                                gRPC server-streaming RPC
                                SearchLabel{Names,Values}(req) →
                                  stream SearchResultBatch
                                                │
                                                ▼
   ┌──────────────────────────────────────────────────────────────────────┐
   │ Ingester.SearchLabel{Names,Values}  (ingester_search.go)             │
   │                                                                      │
   │  1. buildSearchHints(req.Filter, req.Ordering, req.Limit, …)         │
   │       wire SearchFilter → streaminglabelvalues.Params                │
   │                        → compiled Filter via BuildFilter             │
   │       wire LabelMatcher → []*labels.Matcher                          │
   │       int64 Limit       → int (clamped to MaxInt on 32-bit)          │
   │                                                                      │
   │  2. tenant.TenantID(ctx)  — extract tenant ID                        │
   │  3. enforceReadConsistency(ctx, userID)                              │
   │  4. db := i.getTSDB(userID)        — per-tenant TSDB                 │
   │  5. q  := db.Querier(start, end)   — time-bounded querier            │
   │  6. searcher := q.(storage.Searcher) — type-assert to the search    │
   │                                       interface                     │
   │  7. rs := searcher.SearchLabel{Names,Values}(ctx, …, hints, matchers)│
   │       ── single-source SearchResultSet:                              │
   │          filter, order, and limit are applied INSIDE the searcher    │
   │          (no separate merge layer in the ingester)                   │
   │                                                                      │
   │  8. streamSearchResults(ctx, rs, stream.Send, metadataDecorator)     │
   └──────────────────────────────────────┬───────────────────────────────┘
                                          │
                                          ▼
                            stream.Send(*SearchResultBatch) → over gRPC
```

## `streamSearchResults` (the streaming loop)

`streamSearchResults` (lines ~253–289 of `ingester_search.go`) drains the
`storage.SearchResultSet` and emits **fixed-size batches** to the gRPC
stream:

```
   batch := &client.SearchResultBatch{
       Results: make([]Result, 0, searchBatchSize),   // searchBatchSize = 256
   }

   for rs.Next() {                                    // (cancellation check
       v := rs.At()                                   //  fires before iter)
       batch.Results = append(batch.Results,
           SearchResultBatch_Result{Value: v.Value, Score: v.Score})
       if len(batch.Results) >= searchBatchSize {     // 256-result boundary
           if decorate != nil { decorate(batch) }     // (metadata only)
           if err := send(batch); err != nil { return err }
           batch.Results = batch.Results[:0]          // reset, reuse array
       }
   }
   // tail batch: attach warnings, decorate, send
```

**Batch size: 256 results per `*client.SearchResultBatch`** (the
`searchBatchSize` constant). The same envelope and the same underlying
slice are reused across all batches within one call — `stream.Send` is
synchronous (gogoproto marshals before returning) so resetting
`batch.Results[:0]` after each send is safe.

Cancellation is checked **before iteration starts**, and the inner
`storage.Searcher` honours `ctx` itself; a cancelled client stops the
loop at the next `rs.Next()` boundary.

## Where filter / order / limit are applied

All three are applied **inside the underlying `storage.Searcher`**
implementation on the TSDB head, not inside `streamSearchResults`:

| Concern | Constructed where | Applied where |
|---|---|---|
| Filter (`*streaminglabelvalues.Filter`) | `buildSearchHints` → `streaminglabelvalues.BuildFilter(params)` | Inside `searcher.SearchLabel{Names,Values}` — per-candidate-value `Filter.Accept(value)` |
| Ordering (`storage.Ordering`) | `protoToOrdering(wireOrdering)` | Inside the searcher (selects the source iteration order or wraps in a sorting iterator) |
| Limit (`hints.Limit`) | Clamped to `MaxInt` in `buildSearchHints`; `0` means no limit (Prometheus convention) | Inside the searcher (early-stop when emit count reaches limit) |

Because the ingester is a **single-source** leaf, the limit applied here is
the **per-source limit**. The merge layer at the distributor will re-apply
the global limit across all ingesters in the quorum.

## Metadata enrichment (`include_metadata`)

The ingester is the **only** layer where the `include_metadata=true`
URL param has an effect. The `newMetadataBatchDecoratorFunc` helper
(lines ~125–173 of `ingester_search.go`) returns a per-batch decorator
**only when all three are true**:

1. `req.IncludeMetadata == true`
2. `req.Name == "__name__"` (only metric names have `MetricMetadata`)
3. The tenant has at least one recorded metric metadata entry.

When all three hold, each emitted batch is decorated in place just before
`stream.Send`:

```
   decorate := func(batch *SearchResultBatch) {
       mm.mtx.RLock()
       defer mm.mtx.RUnlock()
       for i := range batch.Results {
           if md, ok := mm.metricToMetadata[batch.Results[i].Value]; ok {
               // pick the most-recent entry, deep-copy Help/Unit
               batch.Results[i].Metadata = &mimirpb.MetricMetadata{ ... }
           }
       }
   }
```

A snapshot of the `*userMetricsMetadata` pointer is captured at decorator
construction time. The metadata store's `mtx.RLock()` is held only for the
inner loop. A concurrent `deleteUserMetadata` for the same tenant leaves
the decorator holding a detached-but-race-safe snapshot — acceptable
because RPC lifetimes are bounded.

The decorator runs only when there is at least one result in the batch
(`len(batch.Results) > 0`). Warning-only trailer batches are not decorated.

Store-gateway does NOT enrich metadata; the proto field comment
(`SearchResultBatch.Result.Metadata`) is explicit about this:
> Ignored by sources without metric metadata (e.g. store-gateway).

## Error handling

- **Validation failures** (bad filter, negative limit, …) surface as
  `codes.InvalidArgument` via `status.Error` **before** the deferred
  read-error mapper runs. This avoids them being re-tagged as
  `codes.Internal` by `mapReadErrorToErrorWithStatus`.
- **Tenant / consistency / TSDB errors** flow through
  `mapReadErrorToErrorWithStatus` and surface with the appropriate gRPC
  status code.
- **Mid-stream errors from the searcher** are returned by
  `streamSearchResults` and propagate up to the gRPC layer. The wire
  contract is: any results sent before the error are valid; the error
  closes the stream.

## Benchmarks

Benchmarks live in `pkg/ingester/ingester_search_test.go`:

- `BenchmarkIngester_SearchLabelValues` — varies label cardinality
  (`mod_10`, `mod_77`, `mod_4199`, `__name__`), filter (none / substring
  / fuzzy Jaro-Winkler), ordering (alpha asc, alpha desc, score desc),
  limit, and `include_metadata`.
- `BenchmarkIngester_SearchLabelNames` — smaller matrix; label-name
  cardinality is fixed at the fixture's 4 names.
- `BenchmarkIngester_LegacyVsSearchLabelValues` — **the parity
  benchmark.** Same head, no filter, alpha asc, large limit; sub-cases
  keyed on `/impl=legacy` and `/impl=new` so `benchstat -col '/impl'`
  renders the cost difference directly. Use this when changing anything
  in the streaming path that might shift the new-vs-legacy gap.

All ingester benchmarks share `prepareSearchBenchmarkIngester`, which
pushes 10M series with prime-modulo labels (mirroring
`BenchmarkIngester_LabelValuesCardinality` in
`label_names_and_values_test.go`) so cardinality buckets line up across
the legacy and search benchmarks.
