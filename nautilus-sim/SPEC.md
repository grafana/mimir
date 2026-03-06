# Nautilus Sharding Simulator — Specification

ref: DesignDoc_ Mimir_ query locality optimized ingestion.md
ref: Slicer paper: https://storage.googleapis.com/gweb-research2023-media/pubtools/4405.pdf

## Purpose

A standalone Go simulation (no Mimir dependency) that models the Nautilus sharding proposal from the design doc. The goal is to demonstrate:

1. That metric-name-based hashing with hash-range rebalancing can achieve acceptable ingestion balance across partitions.
2. That partition-to-ingester assignment rebalancing can achieve acceptable query-load balance across ingesters.
3. The interaction between the two rebalancing dimensions over time.

The simulation runs in-process with no servers, no network, and no Kafka. All entities are plain Go structs manipulated by a discrete time-step loop.

---

## Entities

### Series

A series is a `(metric_name, labels)` pair. Each series has a hash computed from its labels, where the high 22 bits encode the metric name and the low 10 bits encode the remaining labels. This is the Nautilus hashing scheme.

Series are loaded from an input file or generated synthetically. They are immutable for the duration of the simulation.

### Partition

A partition owns one or more **hash ranges** `[lo, hi)` within the 32-bit hash space. Every series whose hash falls within a partition's hash ranges is assigned to that partition.

Partition state:
- `ID int32` - zero-based partition number
- `HashRanges map[HashRange]struct{}` — the set of hash ranges currently assigned
- `IngestionRate map[HashRange]float64` — derived: per hashrange ingestion rate
- `SeriesScanned float64` — derived: per-partition series scanned during query evaluation (proxy for query load)

### Ingester

An ingester owns one or more partitions. Its query load is the sum of query load across its assigned partitions.

Ingester state:
- `ID int`
- `Partitions map[int32]struct{}` — IDs of assigned partitions

Note: An ingester reports its total CPU usage, but cannot easily attribute CPU to individual partitions. Instead, each partition tracks **series scanned** (the number of series touched during query evaluation), which serves as a per-partition proxy for query load. The query rebalancer uses per-partition series-scanned to decide which partitions to move, and the sum of series-scanned across an ingester's partitions as the proxy for that ingester's total query load.

### Hash Range

A contiguous range within the uint32 spectrum.

```
type HashRange struct {
    Lo uint32 // inclusive
    Hi uint32 // exclusive
}
```

---

## Load Streams

### Partition Load Stream (PL)

At each time step, every partition reports its current **ingestion load** (e.g., samples/sec). This is the input to the ingestion rebalancer.

The load for a partition is derived from the number and "weight" of series assigned to it. To model real-world skew:
- Each metric name has a configurable ingestion weight (samples/sec/series).
- A small number of metric names can be configured to dominate ingestion volume (the "80% of samples in one metric" scenario from the design doc).

### Ingester Load Stream (IL)

At each time step, every partition reports its **series scanned** — a count of series touched during query evaluation. This is the per-partition proxy for query load. An ingester's total query load is the sum of series-scanned across its assigned partitions.

Series-scanned for a partition is derived from the series it holds and a query-weight function. To model realistic, non-random fluctuation:
- Each metric name has a configurable query weight (queries/sec targeting that metric).
- Query weight is modulated by a per-metric sinusoidal function with randomized phase and period, producing smooth, natural-looking load variation over time.
- A small additive noise term provides jitter.
- A partition's series-scanned at time t = sum over its series of (series' metric query weight at time t).

---

## Rebalancers

### Ingestion Rebalancer (hash ranges → partitions)

Balances **ingestion load** across partitions by reassigning hash ranges. Follows Slicer's weighted-move algorithm, adapted from the [Slicer paper (OSDI '16, §4.4)](https://storage.googleapis.com/gweb-research2023-media/pubtools/4405.pdf).

**Input:** Partition load reports (PL) for the current time step.

**Operations:**
- `move-hashrange(range, from_partition, to_partition)` — move a hash range from one partition to another.
- `split-hashrange(range, partition)` — split a hash range into two halves; both stay on the same partition, eligible for movement in a later round.
- `merge-hashrange(range_a, range_b, partition)` — merge two adjacent hash ranges on the same partition into one.

**Goal:** minimize the max/mean ratio of ingestion load across partitions while limiting churn (fraction of hash space reassigned per round).

**Algorithm (phase-based, per round):**

*Phase 1 — Merge.* Defragment by merging adjacent cold hash ranges on the same partition. This prevents unbounded growth in the number of hash ranges. Constraints:
- Minimum hash-range count: only merge if the partition has more than `min_ranges_per_partition` ranges.
- The merged range's load must be below mean range load.
- Merge churn budget: at most `merge_churn_fraction` (e.g. 1%) of the hash space moved per round.

*Phase 2 — Weighted moves.* Move hash ranges from overloaded to underloaded partitions. Each candidate move is scored by **weight = imbalance_reduction / churn**, where:
- `imbalance_reduction` = the decrease in max/mean load ratio caused by the move.
- `churn` = the fraction of the hash space covered by the moved range.
Moves are applied in descending weight order. Only moves from the current most-overloaded partition are considered (since only those can reduce the max/mean ratio). For each hash range on that partition, the candidate move is to the most underloaded partition. After each move, recompute which partition is most overloaded and repeat. Constraints:
- Move churn budget: at most `move_churn_fraction` (e.g. 9%) of the hash space moved per round.

*Phase 3 — Split.* Split hot hash ranges without changing their partition assignment. This captures finer-grained load measurements and opens new move options for the next round. Constraints:
- Only split ranges whose load is at least 2× the mean range load.
- Maximum hash-range count: only split if the partition has fewer than `max_ranges_per_partition` ranges.

**Rebalancing suppression:** If the current max/mean ratio is already below `balance_target`, skip the round entirely to avoid unnecessary churn.

### Query Rebalancer (partitions → ingesters)

Balances **query load** across ingesters by reassigning partitions to ingesters. Query load is measured by series-scanned (the per-partition proxy). Also follows the Slicer weighted-move approach, but simpler since partitions are atomic (no splitting/merging).

**Input:** Per-partition series-scanned reports (IL) for the current time step.

**Operations:**
- `move-partition(partition, from_ingester, to_ingester)` — reassign a partition.

**Goal:** minimize the max/mean ratio of total series-scanned across ingesters while limiting churn.

**Partition move cost model:**

Moving a partition to a new ingester is not free. The receiving ingester must replay the partition's data from Kafka to build up TSDB state before it can serve queries. During this catch-up period:
- The receiving ingester incurs additional CPU/IO load proportional to the partition's ingestion rate and the depth of data it must replay (`catchup_duration` × `ingestion_rate`).
- The old ingester continues to own the partition for queries until catch-up completes, so there is no query-load benefit during the transition.

The simulation models this as follows:
- When a partition is moved, the receiving ingester enters a **catch-up state** for that partition lasting `partition_catchup_steps` time steps.
- During catch-up, the receiving ingester's load increases by `catchup_cost_factor` × the partition's ingestion rate (modeling the replay overhead).
- During catch-up, the partition's series-scanned still counts against the *old* ingester (it is still serving queries for it).
- After catch-up completes, query ownership transfers to the new ingester and the replay load disappears.

This cost is incorporated into the move weight calculation: **weight = imbalance_reduction / move_cost**, where `move_cost` accounts for both the churn (partition size as a fraction of total) and the transient replay load imposed on the receiving ingester. A partition with high ingestion rate is more expensive to move because the catch-up replay is heavier.

**Algorithm (weighted moves, per round):**
1. Identify the most overloaded ingester (highest total series-scanned, including any ongoing catch-up load from prior moves).
2. For each partition on that ingester (excluding partitions still in catch-up from a prior move), compute the weight of moving it to the most underloaded ingester.
3. Apply the highest-weight move. Recompute which ingester is most overloaded and repeat.
4. Stop when the move budget is exhausted, `balance_target` is met, or no move improves balance.

**Constraints:**
- Movement budget per round: at most `query_move_budget` partition reassignments per round.
- An ingester may hold multiple partitions.
- A partition that is currently in catch-up cannot be moved again until catch-up completes.

**Rebalancing suppression:** If the current max/mean ratio is already below `balance_target`, skip the round.

---

## Initial State

Before the simulation loop begins:

1. **Series**: generated synthetically (or loaded from file) and sorted by hash.
2. **Partitions**: `num_partitions` partitions, each assigned one hash range. The `[0, 2^32)` space is divided into `num_partitions` equal-sized contiguous ranges: partition 0 gets `[0, 2^32/num_partitions)`, partition 1 gets `[2^32/num_partitions, 2×2^32/num_partitions)`, etc.
3. **Ingesters**: `num_ingesters` ingesters. Partitions are assigned round-robin: partition `i` is assigned to ingester `i % num_ingesters`.

---

## Simulation Loop

```
for each time step t:
    1. Update load streams (PL, IL) for time t.
    2. If t % ingestion_rebalance_interval == 0:
         Run ingestion rebalancer (hash ranges → partitions).
    3. If t % query_rebalance_interval == 0:
         Run query rebalancer (partitions → ingesters).
    4. Record metrics for time t.
```

---

## Metrics / Output

At each time step, record:

- **Ingestion balance:** min, max, mean, p99, stddev of partition ingestion load.
- **Query balance:** min, max, mean, p99, stddev of ingester total series-scanned.
- **Max/mean ratios** for both dimensions.
- **Reassignment counts** for each rebalancer.
- **Churn:** fraction of hash space moved (ingestion rebalancer), number of partitions moved (query rebalancer).
- **Catch-up:** number of partitions currently in catch-up state, total catch-up load across ingesters.

Output format: CSV, suitable for plotting or graphing in Google Sheets.

---

## Configuration

All parameters are specified via a config file or flags:

| Parameter | Description | Example |
|---|---|---|
| `num_partitions` | Number of Kafka partitions | 385 |
| `num_ingesters` | Number of ingesters | 200 |
| `num_steps` | Number of simulation time steps | 1000 |
| `ingestion_rebalance_interval` | Steps between ingestion rebalancing rounds | 100 |
| `query_rebalance_interval` | Steps between query rebalancing rounds (10× more frequent than ingestion) | 10 |
| `merge_churn_fraction` | Max fraction of hash space moved in merge phase per round | 0.01 |
| `move_churn_fraction` | Max fraction of hash space moved in weighted-move phase per round | 0.09 |
| `min_ranges_per_partition` | Below this, don't merge hash ranges on a partition | 50 |
| `max_ranges_per_partition` | Above this, don't split hash ranges on a partition | 150 |
| `query_move_budget` | Max partition moves per query rebalance round | 3 |
| `partition_catchup_steps` | Time steps for a new ingester to catch up on a moved partition | 5 |
| `catchup_cost_factor` | Multiplier on partition ingestion rate to model replay CPU overhead | 1.5 |
| `balance_target` | Target max/mean ratio (suppress rebalancing when met) | 1.10 |
| `query_noise_amplitude` | Amplitude of per-partition series-scanned noise | 0.05 |
| `series_file` | Path to file with series label sets (optional) | `series.json` |
| `num_synthetic_series` | Number of synthetic series to generate (if no file) | 1000000 |
| `num_synthetic_metrics` | Number of distinct metric names in synthetic data | 5000 |
| `skew_top_metric_fraction` | Fraction of series in the largest metric name | 0.80 |

---

## Synthetic Data Generation

When no real series data is provided, generate synthetic series:

1. Generate `num_synthetic_metrics` metric names.
2. Distribute `num_synthetic_series` across metric names according to a Zipf or configurable skew distribution, where the top metric gets `skew_top_metric_fraction` of all series.
3. For each series, generate a small set of random label key-value pairs.
4. Compute each series' Nautilus hash: high 22 bits from `hash(metric_name)`, low 10 bits from `hash(remaining_labels)`.
5. Assign each metric an **ingestion weight** (samples/sec/series) drawn from a Zipf distribution. This is independent of the series count distribution — a metric with few series can still have high per-series ingestion rate.
6. Assign each metric a **query weight** (queries/sec targeting that metric) drawn from an independent Zipf distribution. Query weight is uncorrelated with ingestion weight — a heavily ingested metric is not necessarily heavily queried, and vice versa.

---

## Non-Goals (for v1)

- Real Kafka, gRPC, or network simulation.
- Multi-tenant simulation (single tenant is sufficient to validate the algorithm).
- Assignment dissemination / clock skew modeling.
- Query fan-out simulation (this is a load-balancing simulator, not a query planner).
- Rebalancing across multiple tenants simultaneously.

---

## Open Questions

1. What does "good enough" balance look like? What max/mean ratio is acceptable? (Can be explored experimentally.)

## Resolved

- **Hash bit layout:** 22 bits for metric name, 10 bits for remaining labels.
- **Rebalancer frequency:** Query rebalancer runs 10× more frequently than ingestion rebalancer.
- **Reassignment cost:** Partition moves have a catch-up cost model (see Query Rebalancer). Hash-range moves between partitions are metadata-only, no data replay.
- **Real vs. synthetic data:** Synthetic data is sufficient for v1.
- **Initial assignment:** Round-robin partition-to-ingester assignment.
