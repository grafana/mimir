# Ingester — streaming label/value search RPCs

This document covers the ingester-side implementation of the streaming
label/value search RPCs (`Ingester.SearchLabelNames`,
`Ingester.SearchLabelValues`). For the overall feature architecture see
[`README.md`](./README.md).

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
stream to this ingester. The distributor _service_ (write path) is not
on the read-path call chain at all — the `Distributor` type is shared
code, not a network hop.

The ingester applies the full filter / order / limit per call. There is no
k-way merge inside the ingester because there is only one source (the
head). Filtering, ordering, and limit propagate from the wire hints
through to Prometheus's `storage.Searcher` interface on the head.

## Files

- `pkg/ingester/ingester_search.go` — RPC handlers, the streaming loop,
  the metadata-enrichment decorator, and the wire/internal type
  converters.
- `pkg/ingester/client/ingester.proto` — wire definitions for the RPCs
  and the `SearchResultBatch` carrier.

## Per-call flow

A single `SearchLabel{Names,Values}` RPC executes the following steps inside
the handler:

1. **Wire-to-internal conversion** — request hints (filter, ordering, limit,
   label matchers) are converted into the internal `Params` / compiled
   `Filter` / `[]*labels.Matcher` / clamped `int` limit.
2. **Tenant extraction and read-consistency enforcement** — fail fast on
   missing tenant or consistency violations.
3. **Open a time-bounded querier on the per-tenant TSDB head.**
4. **Type-assert to the `storage.Searcher` interface** and invoke the
   appropriate `SearchLabel{Names,Values}` method on it. Filter, ordering,
   and limit are all applied **inside** the searcher — the ingester is a
   single-source leaf, so there is no separate merge layer.
5. **Stream the result set** as fixed-size batches over the gRPC
   server-streaming response, optionally decorating each batch with metric
   metadata (see [Metadata enrichment](#metadata-enrichment-include_metadata)).

## Streaming loop

`streamSearchResults` drains the `storage.SearchResultSet` and emits batches
to the gRPC stream. The batch capacity is the `searchBatchSize` constant
(deliberately matched to the store-gateway's value). The same envelope and
the same underlying slice are reused across all batches within one call —
`stream.Send` is synchronous (gogoproto marshals before returning) so
resetting the slice length to zero after each send is safe.

Cancellation is checked **before iteration starts**, and the inner
`storage.Searcher` honours `ctx` itself; a cancelled client stops the loop
at the next iteration boundary.

## Where filter / order / limit are applied

All three are applied **inside the underlying `storage.Searcher`**
implementation on the TSDB head, not inside the streaming loop. Wire
hints are converted to internal types at request entry (a `0` limit means
"unlimited", per the Prometheus convention; the limit is clamped to fit a
Go `int`). The searcher then evaluates the filter per candidate value,
selects or wraps the source iterator to honour ordering, and stops
emission once the limit is reached.

Because the ingester is a **single-source** leaf, the limit applied here is
the **per-source limit**. The merge layer at the distributor will re-apply
the global limit across all ingesters in the quorum.

## Metadata enrichment (`include_metadata`)

The ingester is the **only** layer where the `include_metadata=true` URL
param has an effect. A per-batch decorator is installed only when all three
conditions hold:

1. The request asked for metadata (`include_metadata` is true).
2. The search target is `__name__` — only metric names have `MetricMetadata`.
3. The tenant has at least one recorded metric metadata entry.

When all three hold, each emitted batch is decorated in place immediately
before it is sent: the decorator iterates the batch's results, looks each
`Value` up in the tenant's metric-metadata map under `RLock`, and attaches
a deep-copied `Type` / `Help` / `Unit` to any match (most-recent entry
wins).

A snapshot of the tenant's metadata pointer is captured at decorator
construction time and the `RLock` is held only for the per-batch inner
loop. A concurrent tenant-metadata delete leaves the decorator holding a
detached snapshot — acceptable because RPC lifetimes are bounded. The
decorator only runs on batches that contain at least one result;
warning-only trailer batches are not decorated.

Store-gateway does NOT enrich metadata; the proto field comment for
`SearchResultBatch.Result.Metadata` is explicit about this:

> Ignored by sources without metric metadata (e.g. store-gateway).

## Error handling

- **Validation failures** (bad filter, negative limit, …) surface as
  `codes.InvalidArgument` and are returned **before** the deferred
  read-error mapper runs — otherwise they would be re-tagged as
  `codes.Internal`.
- **Tenant / consistency / TSDB errors** flow through the read-error
  mapper and surface with the appropriate gRPC status code.
- **Mid-stream errors from the searcher** are returned by the streaming
  loop and propagate up to the gRPC layer. The wire contract is: any
  results sent before the error are valid; the error closes the stream.

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
pushes a large series count with prime-modulo labels (mirroring
`BenchmarkIngester_LabelValuesCardinality` in
`label_names_and_values_test.go`) so cardinality buckets line up across
the legacy and search benchmarks. Inspect the fixture for the exact
series count.
