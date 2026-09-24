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

## Scale fixture

`large-cell-static` models 500 partitions, 100 readcaches, and 50 tenants with
distinct static hotspot locations, widths, baselines, and amplitudes. Each
tenant deliberately starts with five coarse ranges—one on each partition
owned by its initial readcache—so the fixture represents a large topology
without pre-creating 25,000 ranges.

The fixture is validated and loadable, but excluded from the default weight
search. Scallop currently enumerates every legal range destination and
including this topology in every one of the 239 policy evaluations would make
the calibration run impractically expensive.
