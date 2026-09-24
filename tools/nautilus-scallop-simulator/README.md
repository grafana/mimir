# Nautilus Scallop simulator

This tool runs the production-shaped `pkg/nautilus/scallop` planner in a
deterministic closed loop. Gaussian fixtures generate the only load signal the
planner sees; dummy readcaches immediately report the load of their currently
assigned ranges without EWMA lag, warmup, residue, or RPC behavior.

Run the Phase 1 weight search from the repository root:

```bash
go run ./tools/nautilus-scallop-simulator -output-dir ./scallop-results
```

The command writes:

- `search-report.json`: every evaluated policy, fixture, round, action, and
  evaluation group.
- `search-summary.csv`: one flattened row per policy and fixture.
- `recommended-policy.json`: the selected policy and its complete fixture
  results.

## Run one fixture

`run-fixture` executes one checked-in fixture through the same closed-loop
simulation and evaluation path, without starting the beam search:

```bash
go run ./tools/nautilus-scallop-simulator run-fixture \
  -fixture single-tenant-growing \
  -replica-balance 0.75 \
  -transition-events 0.05 \
  -transition-load 0.1 \
  -transition-hash-space 0.1 \
  -locality-miss 0.1 \
  -fragmentation 0.05 \
  -resolution 0.001 \
  -json-output ./fixture-result.jsonl \
  -csv-output ./fixture-result.csv
```

Every weight flag is optional and defaults to `scallop.DefaultPolicy`;
partition-balance weight remains fixed at `1`. Standard output is deterministic
JSON Lines: one record per tick followed by one summary record containing all
seven trajectory-wide evaluation groups. The optional output paths write the
equivalent JSONL or CSV records.

## Workload model

Each tenant has a uniform baseline plus independent wrapped spatial Gaussians:

```text
load(hash, tick) =
  baseline
  + sum(amplitude_i(tick) * spatialGaussian_i(hash))
```

Amplitude profiles can be constant, bounded linear growth/decay, or Gaussian
in time. Spatial load is integrated analytically over each current hash range;
traffic is never randomly sampled.

## Planning boundary

At each tick the simulator:

1. Observes exact current loads from dummy readcaches.
2. Builds a Scallop snapshot.
3. Plans range moves, midpoint splits, and adjacent merges.
4. Applies the resulting assignment.
5. Recomputes exact post-plan load at the same tick.
6. Records all evaluation groups and advances simulated time.

Split children exist in post-plan simulator state immediately but are not
eligible for another planner action until the next tick supplies observations.
Scallop never receives Gaussian components or future amplitudes.

## Recommendation

Partition balance has fixed weight `1`. A deterministic beam search varies the
relative replica-balance, transition, locality, fragmentation, and resolution
weights. Policies are ranked by a separate fixed evaluation utility across all
calibration fixtures, using both mean and worst-fixture outcomes.

## Large-cell fixture

`large-cell-static` models 500 partitions, 100 readcaches, and 50 tenants with
distinct static hotspot locations, widths, baselines, and amplitudes. Each
tenant deliberately starts with five coarse ranges—one on each partition
owned by its initial readcache—so the fixture represents a large topology
without pre-creating 25,000 ranges.

The fixture participates in the same 239-policy beam search, closed-loop
simulation, evaluation, and JSON/CSV reporting as every smaller fixture. There
is no fixture category or exclusion flag.

Scallop uses one deterministic bounded search for every topology. Small
candidate sets fit entirely within its source, destination, split, merge, and
fully-scored limits. Larger candidate sets rank likely actions and project only
the bounded shortlist. Reports include legal, admitted, fully-scored, and
per-budget discarded candidate counts so pruning remains visible.

## Tiny-tenant consolidation fixture

`many-tiny-tenants-consolidating` models 50 static low-load tenants, each
bootstrapped with 64 ranges, on 500 partitions and 100 readcaches for eight
ticks. It runs through the same command and beam-search paths as every other
fixture.

The fixture intentionally preserves the current global four-action bottleneck
and deterministic tenant ordering as a Phase 3 baseline. With consolidation-
oriented weights (zero transition and resolution costs, fragmentation `100`),
all 32 available actions are merges for `tiny-00`; every other tenant makes no
progress and all 50 remain unsettled. Per-tick tenant range counts and the final
per-tenant merge summary make that starvation reproducible for Phase 4.
