# Segmentation Label Analyzer

Analyzes a Mimir tenant to identify good segmentation label candidates. A segmentation label is used to partition series into compartments for sharding.

## What it does

1. **Fetches all label names** from Mimir via `/api/v1/labels`
2. **Gets series counts per label** via `/api/v1/cardinality/label_values`
3. **Queries Loki** for user queries (from query-frontend) and rule queries (from ruler-query-frontend)
4. **Analyzes which labels** appear in queries as valid segmentation candidates (labels with equality or set matchers in all selectors)
5. **Computes a score** for each label and outputs ranked candidates

## Score computation

The score (0-1) considers these factors:

| Factor | Weight | Description |
|--------|--------|-------------|
| **Series coverage** | 40% | Percentage of total series that have this label. Higher is better - we need most series to have the label for effective sharding. |
| **Query coverage** | 40% | Percentage of queries where we can deterministically identify the compartment. Extrapolated across user and rule query observation windows. Penalized by average distinct values per query (see below). |
| **Label values distribution** | 20% | How evenly series are distributed across label values (normalized entropy). A label where 99% of series have one value is bad for balanced sharding. |

### Penalties

**Value count penalty**: Labels with fewer than 10 unique values are penalized proportionally (e.g., 5 values = 0.5x score) since they can't support fine-grained sharding.

**Query values penalty**: If queries typically reference multiple values for a label, the query coverage portion of the score is penalized using exponential decay: `0.8^(avg-1)`. This reflects that such a label is less useful for query routing. The "Avg values/query" column shows this metric - lower is better (ideally 1.0). Note that the QueryCoverage percentage shown in the report is the raw (unpenalized) value.

| Avg values/query | Penalty multiplier |
|------------------|-------------------|
| 1 | 1.00 (no penalty) |
| 2 | 0.80 |
| 3 | 0.64 |
| 4 | 0.51 |
| 5 | 0.41 |
| 10 | 0.13 |

**Note**: Queries using the `info()` function are analyzed but have no valid segmentation label candidates, since `info()` implicitly queries additional metrics making compartment determination impossible.

## TODOs

- [ ] Investigate how to properly support the `info()` function. Currently, queries using `info()` have no valid segmentation label candidates. See [Prometheus info function docs](https://prometheus.io/docs/prometheus/latest/querying/functions/#info).
