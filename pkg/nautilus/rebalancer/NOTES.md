# Rebalancer notes

Out-of-band caveats for future work on the slicer / readcache integration.
Not intended as user-facing docs — keep brief, append findings as they
come up.

## HashRangeStats is snapshot-only on readcache

`HashRangeStats` copies precomputed `PartitionSeries` and `RangeSeries`
snapshots refreshed every `loadstats.TickInterval` (15s) by
`refreshSeriesStats` — it no longer walks TSDB heads on the RPC path.
If stats look stale or walks fall behind ingest, check for
`hash range series walk failed` warnings and consider lowering partition
count per pod or lengthening the rebalancer poll interval.

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
