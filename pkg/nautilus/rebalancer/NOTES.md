# Rebalancer notes

Out-of-band caveats for future work on the slicer / readcache integration.
Not intended as user-facing docs — keep brief, append findings as they
come up.

## HashRangeStats is snapshot-only on readcache

`HashRangeStats` copies precomputed per-partition range counts
refreshed every `loadstats.TickInterval` (15s) by `refreshSeriesStats`
— it no longer walks TSDB heads on the RPC path. If stats look stale
or walks fall behind ingest, check for `hash range series walk failed`
warnings and consider lowering partition count per pod or lengthening
the rebalancer poll interval.

Each `partitionState` keeps its own `currentRanges`/`historicalRanges`
plus a `rangeCounts` map; the walker buckets that partition's head
series into those keys. `HashRangeStats` emits one entry per
(partition, range) so residue from a previous owner appears as a
distinct entry (with the previous owner's `partition_id`) alongside
growth on the new owner — see "Per-(partition, range) reporting"
below.

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

## Readcache `/metrics` vs consumption stalls

Per-partition reader metrics are registered on the main registerer only
after `PartitionReader` reaches Running (see `startKafkaReader`). Registering
the isolated registry earlier leaked collectors on failed start and could
make `/metrics` return 500 with duplicate-collector errors.

That scrape breakage can make `rate(last_consumed_offset)` look jerky or
zero in Grafana; it is separate from tail consumption stalls (frozen offset
with buffered records still on the fetcher), which need their own fix.

## The slicer balances the wrong quantity (mimir-dev-15, 5/19/2026)

Observed on mimir-dev-15 with 300 active partitions, 22 readcache pods,
~15k hash ranges: imbalance ratio holds at 11–17× and grows over time
rather than shrinking. Multiple compounding causes; nothing here is a
single tuning knob.

### Findings

1.  **L_pid is the wrong signal in the presence of head residue.**
    Series sit in a TSDB head until block compaction (~2h). When the
    slicer moves a hash range from Kafka partition P_old → P_new,
    distributors immediately stop producing to P_old, but P_old's head
    holds the moved series until the next compaction. During that
    window:
    - P_old has high L (residue) but receives zero writes.
    - P_new has growing head + writes.
    - Both look "hot" to the slicer, simultaneously, for up to 2h per
      move.
      Confirmed live: P11 reports L=2.27M with **0** distributor
      samples/30s and owns only 0.01% of the hash space; P7 receives
      **3,049** samples/30s on 0.19% of the hash space but reports
      L=0.

2.  **Per-range counts are ~99% zero because the walk uses current
    ranges as the bucket key.**
    `loadstats.CountSeriesByHashRange` walks the TSDB head, computes
    each series' locality hash, and increments the matching owned range
    if and only if the hash falls inside one. Series whose hash points
    at a previous owner's range are excluded entirely. Live ratio
    Σ rates / Σ L = 22.7K / 34.3M ≈ **0.07%**. The slicer's
    `score = improvement / cost` is being computed from nearly-empty
    bucket counts, so it picks ~4-series and ~140-series ranges to
    move per round when the actual hot ranges sit unobserved in
    residue.

3.  **Hot partitions own zero or near-zero hash space.**
    Snapshot at one round:
    | partition | L | ranges | hash% | writes/30s (one distributor) |
    |---|---|---|---|---|
    | P11 | 2.27M | 7 | 0.01% | 0 |
    | P71 | 632K | **0** | 0.00% | 0 |
    | P5 | 605K | **0** | 0.00% | 0 |
    | P7 | 0 | 118 | 0.19% | 3,049 |
    | P49 | 0 | 44 | 1.94% | 1,201 |
    Phase 3 picks P11/P71/P5 as "hottest" by L, finds nothing to move,
    excludes them from this round (`excludedHot[pid]=true`), and falls
    through to merely-warm partitions where moves are tiny (sub-200-
    series ranges) and do not reduce the actual imbalance the operator
    sees.

4.  **Round-to-round L_pid jumps wildly because the residue is
    transient.**
    Three consecutive rounds, ~5 min apart: max_L = 1.32M (P15) →
    1.7M (P10) → 1.93M (P11) → 2.27M (P11 again). The "hottest"
    partition changes every round as different prior owners are
    mid-compaction. The slicer responds to noise and burns moves on
    whichever residue is largest at the moment.

5.  **`recent_moves` is too small to be a counterweight.**
    After 5h33m of uptime, `recent_moves` tracks 173K series across
    2 partitions vs an actual 19.7M "above mean". The
    `effectiveSource(pid) = L_pid - sumRecentMoves(pid)` adjustment
    therefore barely shifts the slicer's view; it can mark off the
    handful of recent moves but not the bulk of pre-existing residue.
    Also: `recent_moves` is in-memory only — a rebalancer restart
    forgets everything outstanding and the slicer immediately re-
    discovers the same "hot" partitions it had just balanced away.

6.  **Readcache assignment-log coverage is sparse.**
    `Log.LiveEntries(now)` over the readcache assignment log shows
    only **83 of 300 active partitions** in active leases at one
    snapshot, distributed extremely unevenly across the 22 pods
    (one pod has 18, seven pods have 0). Same partitions show up as
    actively-consumed in per-pod `cortex_readcache_memory_series_*`
    counters, so the readcache slicer's _plan_ is reaching readcaches
    over `WatchReadcacheAssignments` — but the rebalancer's own
    in-memory log appears to under-report. Two suspects worth
    investigating before adding more signal:
    - `planReadcacheAssignment` pass 2 assigns zero-load partitions
      to `lightestInstance`, which returns the alphabetically-first
      0-load instance every iteration when load doesn't change → all
      217 "unassigned" partitions pile on one instance. Pass 3
      can't undo the pile because its movement-improvement check is
      false for zero-load moves.
    - Round-to-round `Apply` may be preempting pre-issued successors
      because pass 2's "lightest" instance flips between rounds.

### Why head series can't be made to work as a signal

Any L-based signal carries inherent 2h lag. There is no measurable
attribute of the head that distinguishes "current load" from
"residue", short of explicit per-series timestamps. Attempts to
patch the residue out (variants like "discount recently-moved
series" or "exclude partitions in cooldown") only paper over the
problem when the slicer is the agent doing the moving. They are
useless across rebalancer restarts, and they don't help with cases
where the residue was created by a process the slicer doesn't
remember (initial cold-start move, prior code, etc.).

### Direction worth considering (Q2 trade-offs)

**A. Ingestion-rate signal.** Move the slicer onto a rate signal
(samples/s, or first-seen series/s, EWMA'd over ~30s). Pros: no lag,
no residue confusion, the signal directly tracks where writes are
landing now. Cons: rates aren't memory; under low churn / steady
series, two partitions with similar samples/s can have wildly
different head sizes. Spike-prone if smoothing is wrong.

**B. Hybrid (preferred direction).** Pick the move target by
_ingestion rate_, but gate the per-source movable budget by L_pid.
Concretely: Phase 3 still picks "hottest" by L (memory pressure
ceiling), but selects the range to move by ingest rate within the
hottest partition. Result:

- Slicer can't act on partitions where L is high but ingest is zero
  (residue, P11/P71/P5 above) because the rate-based "best range"
  for them is empty.
- Slicer prefers to move actual hot ranges off actual hot ingesters
  because the rate signal isn't polluted by residue.
- L_pid still caps "don't push a partition into 13× imbalance" —
  rates alone could be too noisy.

**C. Settling intervals.** After a move, mark source + dest + range
as "settling" for `CompactionInterval` (2h). Slicer ignores these
when picking hot/cold. Pros: trivial. Cons: with 5 partitions being
actively visited and ~2h-wide settling, a sizeable fraction of the
cluster is masked at any time, slowing convergence.

**D. Persist `recent_moves` and other slicer state.** Independently
worth doing regardless of A–C; the in-memory loss on restart is a
real regression for any approach that depends on cross-round
bookkeeping.

### Suggested follow-up order (no code yet)

1. Investigate the assignment-log coverage gap (finding 6) — if 217
   partitions are genuinely orphaned for stretches, the imbalance
   isn't really "balance failure", it's "no one consuming Kafka".
   Either readcache slicer pass 2 is pathological or the broadcast
   path is dropping/preempting entries. Until 300/300 partitions
   stay continuously assigned, no L signal will be trustworthy.
2. Add `partition_active_series_creation_rate` (or per-range
   equivalent) to `HashRangeStatsResponse` and rebalancer
   `partitionLByPID`. Use it instead of head series in Phase 3 hot
   selection, with L_pid retained as a movable-budget ceiling.
3. Persist `recent_moves` and `moveCooldowns` alongside the
   assignment log file (`logfile.go`).
4. Only after the above settle: revisit `planReadcacheAssignment`
   pass 2 to spread zero-load partitions deterministically
   (round-robin starting from an offset that shifts each round).

## Phase 3 churn guards (implemented, 7/16/2026)

Observed on mimir-dev-15: >100 hash-range moves per round, with the
same range repeatedly relocated to different partitions, fragmenting
the assignment log (every move appends a preempted lease plus a
successor lease; historical owners grow query fan-out via
`PartitionsOverlappingInterval` until `entry_retention` prunes).

Mechanisms and fixes:

1.  **Move count was unbounded.** `movement_budget` caps hash-space
    fraction, not count, and the `improvement / moveCost` score
    prefers tiny ranges, so a fragmented tiling yields hundreds of
    sub-budget moves. Added `max_moves_per_round` (default 30, 0
    disables) as a hard Phase 3 cap, and `min_move_improvement`
    (default 0.01 × meanL, 0 disables) so noise-level moves no
    longer consume the allowance.
2.  **Cooldown never spanned rounds.** The 90s `move_cooldown`
    default was always expired by the next steady-state round
    (~5 min, lease-driven). Default raised to 15m. Cooldowns are now
    also armed for Phase 1 reassigns and cross-partition Phase 2
    merges (they relocate hash space too; cooldowns only gate
    Phase 3 candidates, so recovery is never delayed), and persisted
    to `move-cooldowns.json` under `data_dir` so restarts don't
    forget them (follow-up 3 above; `recent_moves` no longer exists
    since slicer v5).
3.  **Observability.** `cortex_nautilus_rebalancer_actions_total`
    counter (by kind) accumulates per-round action counts; a
    sustained `kind="move"` increase of max_moves_per_round per
    round means the slicer wants more churn than allowed and the
    load signal deserves a look.

`SlicerVersion` bumped to "7" (new knobs land in `ConfigSnapshot`;
v6 traces zero-fill them and stay replayable).

## Per-(partition, range) reporting (implemented)

Finding 2 above — "per-range counts are ~99% zero" — is addressed by
making the load signal per-(partition, range) instead of per-(range)
and reporting both currently-owned ranges and historical residue.
This is the change the rest of this note refers to.

**Wire shape.**

- `ingester.proto.HashRangeRate` carries `partition_id`. The same
  `(lo, hi)` may appear multiple times in one
  `HashRangeStatsResponse` with different `partition_id`s — one entry
  per partition that has any head series in that range (current
  owner growth + zero or more previous owners' residue).
- `ingester.proto.HashRangeEntry` carries `partition_id`.
  `SetHashRanges` becomes per-partition (the readcache demuxes by
  `partition_id`), and `GetHashRanges` lets the rebalancer rebuild a
  per-(partition, range) view from the readcache's local state.
- `SlicerVersion` bumped to "3"; v2 traces can't be replayed.

**Readcache bookkeeping.** Each `partitionState` owns its own range
state:

- `currentRanges` — what the rebalancer last said this partition
  owns. Updated atomically on `SetHashRanges`.
- `historicalRanges` — hash space that just dropped out of
  `currentRanges`. Walker keeps counting series there so residue is
  visible to the rebalancer. Each historical range is GC'd when the
  walker observes zero residue in it (compaction has cleared the
  head).
- `rangeCounts map[HashRange]int64` — walker output keyed by
  current ∪ historical. Invariant: `Σ rangeCounts` over a
  partition's head equals the number of series in that head once a
  walk completes.

The geometric diff used to move ranges between current and
historical on `setRanges` is in `range_diff.go` (`rangeDiff`,
`rangeUnionPreservingBoundaries`). It deals with splits, merges,
partial shifts, and full moves uniformly: anything in the
old-current but not in the new-current becomes historical residue,
anything in the new-current that was historical is reclaimed.

**Rebalancer.** `loadMap` is now keyed by `(partition_id, range)`:

- `lm.seriesAt(partitionID, range)` is the slicer's per-entry load
  lookup. Phase 3 scoring naturally only consults the current
  owner's load on each range, so residue under a previous owner's
  `partition_id` is invisible to it and doesn't inflate the
  "moving X off P_b will drop P_b's L by X" estimate.
- Residue still contributes to L_pid via `PartitionActiveSeries`
  (unchanged), which is how the slicer correctly sees that a
  previous-owner partition's head pressure is still real until
  compaction.
- `reconstructAssignmentFromReadcache` issues one `GetHashRanges`
  RPC per readcache pod instead of fanning out per ownership pair,
  and filters the response by the log-attested ownership set. This
  fixes a pre-existing over-attribution bug where every range from
  a multi-partition pod was credited to ONE of its partitions.

**Walker.** `refreshSeriesStats` walks each partition's heads using
that partition's own current ∪ historical range list as bucket keys;
the result is applied to that partition's `partitionRanges`. Stale
walks (whose snapshot moved before they finished) are discarded; the
next tick reconciles.
