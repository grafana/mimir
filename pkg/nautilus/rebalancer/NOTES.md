# Rebalancer notes

Out-of-band caveats for future work on the slicer / readcache integration.
Not intended as user-facing docs — keep brief, append findings as they
come up.

## per-partition L collapses to instance total in the readcache path

`partitionLByPID` (in `readcache_collect.go`) maps every partition to
its current owner's instance-total active series:

```go
out[pid] = instanceTotals[owner]
```

This is the simplest faithful translation of the legacy ingester-path
shape ("max over owners of the partition's replica set"), but in
single-owner-per-readcache mode it means **all partitions on the same
readcache share the same L**. The hash-range slicer's first round
(`runSlicer` over per-range rates and `partitionLByPID`) can therefore
not differentiate between two partitions on the same pod, even when
one is hot and the other is cold. The per-range *rate* signal is
still present (the readcache reports per-range active series), but the
slicer's gate logic uses partition-level L for budget enforcement, so
the first round stays quiet whenever readcache count >= partition count.

Observed in the docker rig (`development/mimir-nautilus`) on 2026-05-14:

* 100-metric × 200-series sustained load, 4 active partitions, 2
  readcaches.
* Readcache slicer (second round, partition → instance) responded
  promptly: `moves=2`, then steady state at 20000+20000 split across
  pods.
* Hash-range slicer (first round, range → partition) never fired:
  `actions: null` across every round in the trace.

To unblock the first round we'd need per-partition head series in
`HashRangeStatsResponse`. That is a small proto change plus a tiny
`Readcache.hashRangeStats` accumulation tweak (per-partition loop
already exists for `TotalActiveSeries`). Once those land,
`partitionLByPID` should be rewritten to use the per-partition counts
directly and the "instance total fanout" code can be deleted.

Until then: the partition-to-instance round (the one we currently
drive end-to-end) is sufficient to balance memory across the
readcache fleet — it just can't sub-divide a hot partition by
moving slices between siblings on the same pod.

## locality-aware sharding pins single-metric spikes to one partition

`mimirpb.ShardByMetricNameLocality` packs the metric-name hash into
the top 16 bits and the per-series labels hash into the bottom 16.
With `FineEvenSplit` (contiguous quarter-of-32-bit-space per
partition for n=4), every series for a given metric name lands in
the same partition. A 20000-series spike on a single metric name
will therefore look like a single hot partition, not a uniform load.

When testing the slicer, push across multiple metric names
(`-metrics 100` in the dev rig's `verify-tool spike`) so the spike
spreads across the hash space and exercises both rounds.
