# Nautilus Scallop simulator

This tool runs the production-shaped `pkg/nautilus/scallop` planner in a
deterministic closed loop. Gaussian fixtures generate the only load signal the
planner sees; dummy readcaches immediately report the load of their currently
assigned ranges without EWMA lag, warmup, residue, or RPC behavior.

Run the complete Phase 8 weight search from the repository root:

```bash
go run ./tools/nautilus-scallop-simulator -output-dir ./scallop-results
```

The command writes:

- `search-report.json`: every evaluated policy, fixture, round, action, and
  evaluation group.
- `search-summary.csv`: one flattened row per policy and fixture.
- `recommended-policy.json`: the selected policy and its complete fixture
  results.
- `sensitivity-summary.csv`: each one-dimensional and paired perturbation
  around the winner, including eligibility, fixture utility, planner-work, and
  final-cost deltas.

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
  -fragmentation 10 \
  -resolution 10 \
  -move-partition-multiplier 5.623413251903491 \
  -json-output ./fixture-result.jsonl \
  -csv-output ./fixture-result.csv
```

Every weight flag is optional and defaults to `scallop.DefaultPolicy`;
partition-balance weight remains fixed at `1`. `-max-range-partition-share`,
`-max-moves-per-round`, `-max-moves-per-tenant`, `-max-splits-per-round`, and
`-max-splits-per-tenant` override the range load ceiling and action limits
for one run. Standard output is deterministic
JSON Lines: one record per tick followed by one summary record containing all
seven trajectory-wide evaluation groups. The optional output paths write the
equivalent JSONL or CSV records.

## Replay a production snapshot

`replay-snapshot` reads one strict, versioned JSON envelope containing a
complete `scallop.Snapshot` and `scallop.Policy`, calls `scallop.Plan` once,
and writes one deterministic result record:

```bash
curl -o ./scallop-snapshot.json \
  http://localhost:8011/nautilus/rebalancer/rounds/0/replay.json

go run ./tools/nautilus-scallop-simulator replay-snapshot \
  -input ./scallop-snapshot.json
```

The rebalancer endpoint reconstructs the original planner input from its
bounded trace history. It retains only the Scallop-specific inputs that are not
already present in the trace, rather than storing a duplicate assignment and
range-load map.

The output includes a canonical input identity, ordered actions, projected
hash and readcache assignments, initial/final costs, and bounded-search
diagnostics. Replay performs no fixture generation, search, or RPCs.

The complete search also evaluates every policy against the checked-in
anonymized version-3 corpus in `replay-fixtures/phase8.json`: balanced no-op,
partition skew, replica skew, locality hit and miss, split pressure, and
over-fragmentation. These one-shot cases gate action direction, determinism,
and bounded work; they do not contribute to the closed-loop ranking utility.
Replay/direct-plan parity can be checked independently with:

```bash
go test ./tools/nautilus-scallop-simulator \
  -run '^TestReplayCorpusIsVersionedDeterministicAndMatchesDirectReplay$'
```

## Workload model

Each tenant has a uniform baseline plus independent wrapped spatial Gaussians:

```text
load(hash, tick) =
  baseline
  + sum(amplitude_i(tick) * spatialGaussian_i(hash))
```

Amplitude profiles can be constant, bounded linear growth/decay, or Gaussian
in time. A deterministic, mean-preserving log-normal AR(1) multiplier gives
each Gaussian component correlated traffic variation; omitted noise settings
use CV `0.12`, one-tick correlation, and seed `30`, while an explicit zero CV
disables variation. Every checked-in fixture explicitly records CV `0.12`,
one-tick correlation, and seed `30`. The complete multiplier trajectory is
generated before a run, so repeated observations at one tick are identical.
Spatial load is integrated analytically over each current hash range,
preserving exact split/merge additivity.

Scallop's resolution term is the dimensionless, scale-free traffic-weighted
mean hash width:

```text
resolution = sum(range load * normalized hash width) / total observed load
```

Multiplying every observed load by the same constant therefore leaves
resolution and structural decisions unchanged. Reports also retain the raw
`resolution_numerator` (`load * hash width`) as a scale-sensitive diagnostic.

Fixtures set a default `initial_ranges_per_tenant`. A tenant can set
`initial_ranges` to override that default when production-shaped range-count
distributions would otherwise force every tenant to use the dominant tenant's
cardinality. Likewise, a tenant's `settled_ranges` overrides the fixture's
`settled_ranges_per_tenant` evaluation target.

By default each tenant starts on the partitions of one readcache. Setting
`initial_placement` to `spread` instead deals every tenant's ranges round-robin
across all partitions, continuing the cursor between tenants, so a fixture can
start from a mature, balanced cell.

`observation_noise` perturbs only the range loads handed to Scallop, modelling
windowed sample-rate measurement; true loads and every evaluation metric stay
exact. Each range's error variance is proportional to its load, so an average
partition's observed total has the configured
`partition_coefficient_of_variation` however finely its load is divided.
Draws are fixed by seed, tick, tenant, and range.

## Deterministic fixture catalog

The complete unified catalog is:

- `single-tenant-static`, `single-tenant-growing`, and
  `two-tenant-opposing`: the original spatial and temporal controls;
- `large-cell-static`: a broad 500-partition production topology;
- `many-tiny-tenants-consolidating`: fair consolidation of 50 tenants that
  are tiny relative to a partition;
- `dev30-fluctuating`: noisy 300-partition partition-move calibration;
- `balanced-no-op`: exact nonzero partition and replica balance, with ranges
  already fine enough to satisfy the range load ceiling;
- `multi-modal`: several simultaneous spatial modes in one tenant;
- `temporal-burst`: a bounded Gaussian amplitude envelope;
- `hotspot-shift`: decaying and growing fixed-center components;
- `mixed-heavy-tiny`: competing tenant load scales and granularities;
- `same-workload-coarse` and `same-workload-fine`: identical traffic from
  different checked-in initial tilings; and
- `dev30-overfragmented-recovery`: the 45,000/30,000-range recovery case
  described below; and
- `dev30-steady-state`: the long-run Dev-30 consolidation case described
  below.

All fixtures are embedded and traverse the same closed-loop simulation,
evaluation, search, and report path.

## Planning boundary

At each tick the simulator:

1. Observes exact current loads from dummy readcaches.
2. Builds a Scallop snapshot, applying any configured observation noise.
3. Plans range moves, midpoint splits, fair adjacent-merge waves, and logical
   partition moves.
4. Applies both projected placement levels.
5. Recomputes exact post-plan load at the same tick.
6. Records all evaluation groups and advances simulated time.

Split children exist in post-plan simulator state immediately but are not
eligible for another planner action until the next tick supplies observations.
Scallop never receives Gaussian components or future amplitudes.

## Range load ceiling

Fragmentation charges each merge the same benefit however many partitions
exist, and resolution rewards a split by its load times its width. Their
weighted trade-off therefore settles each tenant at a range count independent
of partition count: in a 300-partition cell, the dominant tenants collapsed to
tens of ranges carrying multiple partitions' load each. Such ranges can neither
be moved to balance partitions nor isolate a hotspot.

`Policy.MaxRangePartitionShare` (default `0.125`) bounds range load relative to
mean partition load, which preserves hotspot resolution whatever the weights:

- A range above the ceiling splits regardless of cost, heaviest first, within
  the split action limits.
- A merge may only produce a range up to half the ceiling, leaving hysteresis
  so merged ranges are not immediately re-split.
- A cross-partition merge may not push its destination more than half the
  ceiling above mean partition load. Peak excess prices only the hottest
  partition, so without this bound merges fill every other partition up to an
  observed peak that measurement noise inflates.

Ranges therefore converge to at most the ceiling, and merges never coarsen a
range past half of it. Zero disables all three rules.

## Recommendation

Partition balance has fixed weight `1`. A deterministic beam search varies the
relative replica-balance, transition, locality, fragmentation, and resolution
weights plus the partition-move action multiplier. Policies are ranked by a
separate fixed evaluation utility across all calibration fixtures, using both
mean and worst-fixture outcomes. That utility prices a partition move as 64
ordinary range actions and separately penalizes partition reversals and load
moved back within three ticks. Production-shaped replay cases are eligibility
gates, not ranking inputs.

After selecting the winner, the simulator deterministically perturbs every
searched dimension up and down and evaluates the nearest fragmentation versus
resolution and transition-event versus partition-move-multiplier trade-offs.
The JSON report retains nearby and baseline initial/final cost terms for every
round plus full candidate-work comparisons; the sensitivity CSV records nearby
wins plus per-fixture utility, exact-delta, complete-projection, and final
weighted-cost deltas.

Ineligible policies rank by how many gates they fail before utility, so the
beam moves toward feasibility when no evaluated policy passes. If the search
ends without an eligible policy, the error lists the best policy's failed gates.

The range load ceiling is the ninth searched dimension. Without it, no weight
combination passed both the Dev-30 balance gates and the overfragmented-recovery
consolidation gate: the best policy kept balance only by making merges
unprofitable.

With the ceiling, the search evaluates 221 policies in about 7 minutes on the
M3 Pro development host. The checked-in defaults are the winner of a search
seeded from the previous winner. Seeding a further search from them improves
aggregate utility by only 0.003% by drifting along flat directions, lowering
the fragmentation and locality weights, so the defaults were not moved again.
23 of the 24 sensitivity perturbations stay eligible. The exception is a
smaller ceiling (`0.0885`), which slows overfragmented recovery below 1,000
merges per dominant tenant. A larger ceiling (`0.177`) is eligible but scores
worse.

## Large-cell fixture

`large-cell-static` models 500 partitions, 100 readcaches, and 50 tenants with
distinct static hotspot locations, widths, baselines, and amplitudes. Each
tenant deliberately starts with five coarse ranges—one on each partition
owned by its initial readcache—so the fixture represents a large topology
without pre-creating 25,000 ranges.

The fixture participates in the same complete deterministic beam search,
closed-loop simulation, evaluation, and JSON/CSV reporting as every smaller
fixture. There is no fixture category or exclusion flag.

Scallop uses one deterministic bounded search for every topology. Small
candidate sets fit entirely within its range and partition-move limits. Larger
candidate sets rank likely actions and project only bounded shortlists. Reports
include legal, admitted, fully-scored, and per-budget discarded candidate
counts so pruning remains visible.

## Tiny-tenant consolidation fixture

`many-tiny-tenants-consolidating` models 50 static low-load tenants, each
bootstrapped with 64 ranges, on 500 partitions and 100 readcaches for 16
ticks. It runs through the same command and beam-search paths as every other
fixture. A uniform, noise-free `background` tenant carries about 99.6% of the
load in 6,000 ranges spread across every partition. Without it the "tiny"
tenants would each carry ten partitions' worth of load, and four-range
settlement would create exactly the coarse hotspots the range load ceiling
forbids. Background ranges sit between half the ceiling and the ceiling, so
they neither merge nor split.

The fixture evaluates convergence toward the legacy four-range structural
target without imposing that target as a planner rule, and gates true
partition balance the same way as the Dev-30 fixtures. The default policy caps
each tenant at 256 merges per planning round while retaining a 1,600-merge
global budget. Deterministic round-robin merge waves preserve progress for
every tenant, and all 50 tenants still reach the target within the configured
bound. Per-tick trajectories, settle ticks, no-progress intervals, merge
throughput, and per-tenant merge counts make fairness and convergence directly
inspectable.

## Dev-30 fluctuation fixture

`dev30-fluctuating` models 300 partitions, 100 readcaches, and 200 tenants with
four initial ranges apiece. Its varied static Gaussian hotspots receive the
same default correlated noise as every other fixture over 16 30-second ticks,
making repeated and reversed partition moves visible to the common evaluation
and beam-search paths.

`dev30-overfragmented-recovery` and `dev30-steady-state` mirror the Dev-30 cell
of October 2026: 300 partitions, 100 readcaches, two dominant uniform tenants
carrying 60% and 39% of writes, and 252 single-range tenants sharing the rest.
Both start from a balanced `spread` placement and give Scallop 3% partition-level
observation noise, matching the spread between one- and ten-minute partition
write rates measured in Dev-30 before the October 8 regression.

`dev30-overfragmented-recovery` starts the dominant tenants at the production
45,000 and 30,000 ranges. Four rounds require both to consolidate in every
round by at least 1,000 ranges overall, which rejects the former
four-merges-per-tenant starvation behavior while retaining the global
1,600-merge safety limit.

`dev30-steady-state` runs 48 rounds from 6,000 and 4,000 ranges, long enough
for consolidation to stop. It reproduces the overnight Dev-30 failure under the
former defaults: each merge only has to avoid raising the observed peak, so
merges keep filling partitions that noise made look cold, the dominant tenants
collapse to about a hundred ranges, and true load concentrates on some
partitions while others drain to zero. Under the range load ceiling the same
fixture settles within about 100 ranges of its starting granularity, with
worst partition imbalance below 0.1.

Both fixtures gate true post-plan balance: no round may exceed twice the
fixture imbalance threshold, the final round must be within it, and the final
coldest partition must carry at least one minus the threshold of mean load.
Peak excess alone cannot see drained partitions, so every round records the
coldest partition's share as `partition_floor`.

## Production-scale planner benchmark

Scallop's candidate discovery benchmarks model 1,000 partitions, 300 logical
readcache replicas, and 10,000 tenants at both four ranges per tenant (40,000
ranges) and bootstrap-scale 64 ranges per tenant (640,000 ranges):

```bash
go test ./pkg/nautilus/scallop \
  -run '^$' \
  -bench 'BenchmarkProductionScale(CandidateGeneration|Plan)' \
  -benchmem -benchtime=1x
```

The initial production target is at most five seconds for one 640,000-range
planning round. On an Apple M3 Pro, the Phase 7 implementation plans that
topology in approximately 1.1 seconds with a one-action execution budget.
Wall-clock timings are informational; tests enforce deterministic discovery
work counts, exact-delta counts, and complete-projection bounds instead.

The expanded fourteen-fixture search includes the exact 75,000-range recovery
case; Phase 7 bounded candidate discovery keeps candidate scoring bounded even
though observation and index construction still scale with the checked-in
range count.
