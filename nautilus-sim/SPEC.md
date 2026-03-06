# Nautilus Sharding Simulator — Specification

ref: DesignDoc_ Mimir_ query locality optimized ingestion.md

## Purpose

A standalone Go simulation (no Mimir dependency) that models the Nautilus sharding proposal from the design doc. The goal is to demonstrate:

1. That metric-name-based hashing with hash-range rebalancing can achieve acceptable ingestion balance across partitions.
2. That partition-to-ingester assignment rebalancing can achieve acceptable query-load balance across ingesters.
3. The interaction between the two rebalancing dimensions over time.

The simulation runs in-process with no servers, no network, and no Kafka. All entities are plain Go structs manipulated by a discrete time-step loop.

---

## Entities

### Series

A series is a `(metric_name, labels)` pair. Each series has a hash computed from its labels, where the high-order bits encode the metric name and the low-order bits encode the remaining labels. This is the Nautilus hashing scheme.

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

A contiguous range within the int32 spectrum.

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

Balances **ingestion load** across partitions by reassigning hash ranges. Follows the Slicer algorithm from the design doc:

**Input:** Partition load reports (PL) for the current time step.

**Operations:**
- `move-hashrange(range, from_partition, to_partition)` — move a hash range from one partition to another.
- `split-hashrange(range, partition)` — split a hash range into two halves; They both stay, and are eligible for movement in a later pass.
- `merge-hashrange(range_a, range_b, partition)` — merge two adjacent hash ranges on the same partition into one.

**Constraints:**
- Movement budget per round: at most B hash-range reassignments per rebalancing round.
- Goal: minimize the max/mean ratio of ingestion load across partitions.

**Algorithm sketch (greedy, per round):**
1. Compute mean ingestion load across all partitions.
2. Identify the most overloaded partition.
3. Find its largest hash range. If moving it to the most underloaded partition improves balance, do so. If the range is too large (would overload the target), split it first.
4. Repeat until budget is exhausted or balance target is met.

### Query Rebalancer (partitions → ingesters)

Balances **query load** across ingesters by reassigning partitions to ingesters. Query load is measured by series-scanned (the per-partition proxy).

**Input:** Per-partition series-scanned reports (IL) for the current time step.

**Operations:**
- `move-partition(partition, from_ingester, to_ingester)` — reassign a partition.

**Constraints:**
- Movement budget per round: at most B partition reassignments per rebalancing round.
- An ingester may hold multiple partitions.
- Goal: minimize the max/mean ratio of total series-scanned across ingesters.

**Algorithm sketch (greedy, per round):**
1. Compute mean total series-scanned across all ingesters (sum of partitions' series-scanned per ingester).
2. Identify the most overloaded ingester.
3. Find its partition contributing the most series-scanned. If moving it to the most underloaded ingester improves balance, do so.
4. Repeat until budget is exhausted or balance target is met.

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

Output format: CSV or JSON time series, suitable for plotting or importing into a Grafana dashboard.

---

## Configuration

All parameters are specified via a config file or flags:

| Parameter | Description | Example |
|---|---|---|
| `num_partitions` | Number of Kafka partitions | 385 |
| `num_ingesters` | Number of ingesters | 200 |
| `num_steps` | Number of simulation time steps | 1000 |
| `ingestion_rebalance_interval` | Steps between ingestion rebalancing rounds | 10 |
| `query_rebalance_interval` | Steps between query rebalancing rounds | 10 |
| `ingestion_move_budget` | Max hash-range moves per ingestion rebalance round | 5 |
| `query_move_budget` | Max partition moves per query rebalance round | 3 |
| `balance_target` | Target max/mean ratio (stop rebalancing when met) | 1.10 |
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
4. Compute each series' Nautilus hash: high bits from metric name, low bits from label hash.

---

## Non-Goals (for v1)

- Real Kafka, gRPC, or network simulation.
- Multi-tenant simulation (single tenant is sufficient to validate the algorithm).
- Assignment dissemination / clock skew modeling.
- Query fan-out simulation (this is a load-balancing simulator, not a query planner).
- Rebalancing across multiple tenants simultaneously.

---

## Open Questions

1. What hash function and bit layout to use for the Nautilus hash? (e.g., top 16 bits for metric name, bottom 16 for labels? Or adaptive?)
2. Should the ingestion rebalancer and query rebalancer run at the same frequency, or should one be faster?
3. How should we model the "cost" of a reassignment? (e.g., during a hash-range move, the old and new partitions both serve queries for a transition period.)
4. Should the simulation support loading real series data from the active series API, or is synthetic data sufficient for v1?
5. What does "good enough" balance look like? What max/mean ratio is acceptable?
