# `blockLabelValues`: the two strategies and the "no filter func" comment

An analysis of `blockLabelValues` in `pkg/storegateway/bucket.go`, focused on
the significance of the two label-values strategies and whether the comment
"we don't bother with a filter func because it wouldn't really save us work"
is accurate.

## The big picture: what `blockLabelValues` computes

It answers "what are the distinct values of label `X`, restricted to series
matching these matchers?" for a single block. Most of the function is about
*choosing* between two fundamentally different ways to compute that, and
feeding that choice the right cost inputs.

The two strategies are the final `if/else`:

```go
if len(pendingMatchers) > 0 || strategy.preferSeriesToPostings(matchersPostings) {
    values, err = labelValuesFromSeries(...)   // Strategy A
} else {
    values, err = labelValuesFromPostings(...)  // Strategy B
}
```

### Strategy B — `labelValuesFromPostings` (the "postings" path)

- You already have `matchersPostings` = the set of series refs matching the matchers.
- For *each candidate label value*, you fetch its posting list and intersect it
  with `matchersPostings`. A non-empty intersection ⇒ that value is "live" ⇒
  include it.
- Cost ≈ fetching one posting list **per candidate label value**. Cheap when
  there are few candidate values, expensive when a label is high-cardinality
  (think `__name__` or `pod`).

### Strategy A — `labelValuesFromSeries` (the "series" path)

- Take `matchersPostings`, actually load the *series* they point to, and read
  label `X` off each series directly into a set.
- Cost ≈ loading **one series per matched series ref**. Cheap when few series
  match, expensive when the matchers are broad.

> **Insight.** These two paths read the same answer from opposite ends of the
> index. B fans out over *label values* (good for low-cardinality labels / broad
> matchers). A fans out over *matched series* (good for selective matchers /
> high-cardinality labels). Neither dominates — which is why the cost model
> exists.

## How the choice is made — `labelValuesPostingsStrategy`

`pkg/storegateway/bucket_index_postings.go:469`:

```go
func (w labelValuesPostingsStrategy) preferSeriesToPostings(postings []storage.SeriesRef) bool {
    return int64(len(postings)*tsdb.EstimatedSeriesP99Size) < postingsListsTotalSize(w.allLabelValues)
}
```

This is a direct size comparison of the two strategies' dominant cost:

- Series path ≈ `numMatchedSeries × estimatedSeriesSize`
- Postings path ≈ total bytes of all the candidate label-value posting lists
  (`postingsListsTotalSize(allLabelValues)`)

Pick whichever fetches fewer bytes. The `selectPostings` method
(`bucket_index_postings.go:453`) does a richer version of the same trade-off
earlier, when deciding whether to fully apply matchers via postings or leave
some as `pendingMatchers`. And `pendingMatchers` is the *other* trigger for the
series path: if the strategy chose not to apply some matchers through postings,
those matchers can only be honored by filtering loaded series — so you're forced
into Strategy A.

This is why `allApplicableValuesPostingsOffsets` is passed into the strategy
struct: **the list of candidate label values is itself a cost input to the
decision.** That is the load-bearing fact for evaluating the comment.

## Is "we don't bother with a filter func because it wouldn't save us work" accurate?

The truth lives in what the `filter` argument actually does. From
`pkg/storage/indexheader/index/postings.go:260`:

```go
prefixMatches := prefix == "" || strings.HasPrefix(unsafeValue, prefix)
e.matches = prefixMatches && (filter == nil || filter(unsafeValue))
e.isLast  = unsafeValue == noMoreMatchesMarkerVal || (!prefixMatches && prefix < unsafeValue)
```

Two separate things happen here, and conflating them is what makes the comment
look more true than it is:

1. **`prefix`** drives `isLast` → it *terminates the scan*. The loop
   `for d.Err() == nil && !currEntry.isLast` stops once we walk off the prefix
   range. The prefix is also used up front (`labelValuePrefixOffsets`,
   `postings.go:201`) to *binary-search to the right section* and only decode
   that slice of the table. **The prefix genuinely saves work — both scan length
   and bytes read.**

2. **`filter`** only gates `e.matches` → whether the value is `strings.Clone`d
   and appended to the result. It has **zero effect on `isLast`**. So adding a
   filter does *not* shorten the index-header scan at all — every value in the
   prefix range is still decoded. The only local saving is a few `strings.Clone`
   allocations.

So the comment's *implicit premise* — "a filter wouldn't shrink the scan" — is
**correct**. The filter cannot reduce the most immediate work (decoding the
offset table over the prefix range).

### The caveat the comment glosses over

A filter *would* shrink `allApplicableValuesPostingsOffsets`, and that slice
feeds two downstream things:

- **Strategy B's per-value postings fetches.** Consider a regex `X=~"foo.*bar"`.
  `exactMatchOrPrefixForLabelName` extracts only the prefix `"foo"` (via
  `m.Prefix()`), so `"foobaz"` survives into the candidate list. A filter
  applying the *full* regex would drop `"foobaz"`, avoiding a posting-list fetch
  + intersection for it in `labelValuesFromPostings`.
- **The strategy decision itself.** A smaller `allLabelValues` lowers
  `postingsListsTotalSize(...)`, which makes the postings path look cheaper in
  both `preferSeriesToPostings` and `selectPostings`.

So strictly, in the *regex-on-the-label-name + postings-path* case, a filter
**would** save some work.

### Why the author is still essentially right in practice

- **Correctness never needs it.** The full regex matcher is already in
  `matchers`, so it's applied in `ExpandedPostings`. Series with `X="foobaz"`
  are excluded from `matchersPostings`, so the intersection for `"foobaz"` comes
  back empty and it's dropped anyway. The filter is *purely* an optimization,
  never a correctness requirement.
- **The series path can't use it at all.** `labelValuesFromSeries` never fetches
  per-value postings; it reads values off matched series and applies leftover
  matchers via `newFilteringSeriesChunkRefsSetIterator`. A value-filter saves
  literally nothing there.
- **The saving is narrow.** It only materializes for a regex whose
  discriminating part is *beyond* its extractable prefix, *and* when the
  postings path is chosen. For exact matches you've already taken the
  `PostingsOffset` short-circuit (`bucket.go:1622`), and for pure-prefix regexes
  the prefix already is the complete filter.

> **Verdict.** The comment is accurate about the thing that matters most (a
> filter doesn't shorten the index-header scan — only the prefix does), and the
> matchers are re-applied downstream regardless, so a filter is never needed for
> correctness. But "wouldn't save us work" is a slight overstatement: for a
> selective regex going down the postings path, it would avoid some per-value
> posting fetches. The author traded a small, conditional win for not
> duplicating matcher-evaluation logic that already lives in `ExpandedPostings`.

## How to verify the key claim empirically

To *prove* the "no early termination" claim rather than rely on reading the
code: add a focused benchmark in `pkg/storage/indexheader`. Call
`LabelValuesOffsets` over a high-cardinality label with a fixed `prefix` but with
`filter == nil` vs. a `filter` that rejects everything. If the comment's premise
holds, **both should scan the same number of entries and take essentially the
same time** (the all-rejecting filter just skips the `strings.Clone`s). There's
already a `BenchmarkLabelValuesOffsetsIndexV2_WithPrefix` at
`reader_benchmarks_test.go:246` to adapt.

## A better cost axis: predict the number of calls, not the bytes fetched

The current cost model optimizes for the *amount of data fetched*. A sharper
objective is to optimize for the *number of object-storage calls*, and to fold
the cost of `PostingsOffset` lookups into the same model. This section works
through why that's both correct and concretely realizable.

### Where the calls come from, and the merging that hides them

Both candidate paths bottom out in the same `gapBasedPartitioner`
(`pkg/storegateway/partitioner.go`). `partition()` (`partitioner.go:92`) walks a
list of `(start, end)` byte ranges in offset order and merges two ranges when
either:

- they overlap (`p.End >= s`), or
- the gap between them is small enough to bridge: `p.End + maxGapBytes >= s`
  (the "extended" case, `partitioner.go:114`).

The output is a slice of `Part`s, and **`len(parts)` is exactly the number of
`GetRange` calls to object storage**. `maxGapBytes` is the merge threshold — the
number of *useless* bytes the system is willing to fetch to save one round trip
(configured via `PartitionerMaxGapBytes`).

Both paths feed this same machine:

- Postings: `fetchPostings` sorts per-value ranges by `Start` and partitions them
  (`bucket_index_reader.go:488-496`).
- Series: it partitions series refs as `[id, id+MaxSeriesSize)`
  (`bucket_index_reader.go:643`).

And along the chain `labelValuesFromPostings -> FetchPostings -> fetchPostings`,
`PostingsOffset` (`bucket_index_reader.go:470`) is called **once per candidate
label value that misses the postings cache**. The series path makes **zero**
such incremental calls — it consumes `matchersPostings` (already computed by
`ExpandedPostings`) and loads series directly.

**Why `PostingsOffset` now belongs in the call budget at all.** When the index
header's decoding buffer is a `*streamencoding.BucketDecbufFactory`,
`PostingsOffset` reads the offset-table section **from object storage** via
`NewDecbufInSection` (`pkg/storage/indexheader/index/postings.go:114-128`; the
code comment refers to "cached GetRange bucket ops"). So a `PostingsOffset` call
is an object-storage round trip with cost comparable to a posting-list or series
`GetRange` — not the cheap on-disk/mmap'd lookup it used to be. That is the whole
reason it's worth pricing here: these calls are no longer free.

There's an asymmetry that makes them matter even more. The posting-list reads and
series reads are **coalesced by the gap-based partitioner** before hitting the
bucket. The per-value `PostingsOffset` reads are **not** — they're issued one at a
time in the `fetchPostings` loop, each its own bucket section read (overlapping
sections may be served from the GetRange cache, but they are not run through
`partition()`). So for a high-cardinality label, the un-coalesced
`PostingsOffset` reads can dominate the call count, which is precisely the cost
the bytes-based model is blind to.

### Why "bytes" is a lossy proxy for "calls"

Take a label with 10,000 candidate values:

- If their posting lists sit contiguously in the index, the partitioner collapses
  them into **~1 GetRange call**.
- If they're scattered with gaps larger than `maxGapBytes`, that's **~10,000
  calls**.

Both scenarios have **identical total bytes** (`postingsListsTotalSize`). The
current model (`bucket_index_postings.go:469`) cannot tell them apart, because
summing `Off.End - Off.Start` discards exactly the information that determines
the call count: *where the ranges sit relative to each other*.

The model already holds everything it needs to do better. `allLabelValues` is
`[]streamindex.PostingListOffset`, and each carries `.Off index.Range`
(Start/End) — the same `index.Range` the partitioner consumes. Today the model
projects that 2-D spatial information (position + size) down to a 1-D scalar
(total size) and loses the clustering. Predicting call count just means *not*
throwing that away: sort the ranges by `Start` and run the same `partition()`
logic to get a predicted `numParts`. Symmetrically, the series side can be
predicted by partitioning the `matchersPostings` refs as `[ref, ref+MaxSeriesSize)`.
Both sides then compare in the same currency — predicted GetRange calls —
instead of bytes-to-bytes.

### `maxGapBytes` is the call-to-byte exchange rate

Pricing a call against bytes doesn't need an arbitrary constant. The partitioner
has already declared the price of one object-storage call, in bytes:

> `maxGapBytes` = "I will fetch up to this many wasted bytes to avoid one extra
> `GetRange` call."

So a principled unified cost is neither `numParts` alone nor `bytes` alone:

```
cost ≈ bytesFetched + numParts * maxGapBytes
```

In the small-range regime `numParts * maxGapBytes` dominates (the call-count
objective); in the large-range regime `bytesFetched` dominates (a single 1 GB
part is genuinely worse than a single 1 KB part, which pure call-counting can't
see). Calls are the term the current model is *missing*, and `maxGapBytes` says
how to weight it.

Because a `PostingsOffset` read is now itself an object-storage call (see above),
it goes in the *same* currency at the *same* exchange rate — not as a separate
index-header term. The postings path pays for two kinds of bucket calls: the
per-value offset reads (un-coalesced) and the partitioned posting-list reads. The
series path pays for one kind (partitioned series reads) and makes no per-value
offset reads:

```
postingsPathCost = predictedPostingsBytes
                 + predictedPostingsParts * maxGapBytes   (coalesced posting-list reads)
                 + len(allLabelValues)     * maxGapBytes   (un-coalesced PostingsOffset reads)

seriesPathCost   = predictedSeriesBytes
                 + predictedSeriesParts    * maxGapBytes   (coalesced series reads)
                 (no per-value offset reads)
```

The `len(allLabelValues) * maxGapBytes` term is what makes high-cardinality
labels correctly expensive on the postings side: each candidate value is a
potential bucket round trip the partitioner never gets to merge.

Note a self-consistency win, now stronger than before: the postings path's
`PostingsOffset` calls re-derive the `index.Range` values that
`LabelValuesOffsets` already returned. Since each re-derivation now costs a bucket
round trip (not just CPU), threading the already-known offsets through to
`fetchPostings` would eliminate the entire `len(allLabelValues) * maxGapBytes`
term — turning the third cost into zero rather than merely pricing it. A model
that predicts parts *from those same offsets* therefore both prices the cost and
points at how to remove it.

### Caveats before building it

- **Prediction error.** The strategy would partition *estimated* end offsets (the
  sparse-table ends are over-estimates, hence `resizePostings` later), so
  `predictedParts` is an upper bound on merges in some cases. Acceptable for a
  cost model, but it's why an error threshold is the right framing.
- **Sort cost.** `allLabelValues` arrives sorted by value, not offset; predicting
  parts needs an offset-sort on every decision, including trivial ones.
- **Cache blindness.** The partitioner runs on cache *misses* only
  (`FetchMultiPostings` / `FetchMultiSeriesForRefs` filter first). The strategy
  can't know the hit rate ahead of time, so predicted parts overcount when the
  cache is warm — symmetrically on both sides, so the comparison stays fair, just
  inflated.