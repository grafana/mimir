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
| **Query coverage** | 40% | Percentage of queries where we can deterministically identify the compartment. Extrapolated across user and rule query observation windows. |
| **Label values distribution** | 20% | How evenly series are distributed across label values (normalized entropy). A label where 99% of series have one value is bad for balanced sharding. |

**Penalty**: Labels with fewer than 10 unique values are penalized proportionally (e.g., 5 values = 0.5x score) since they can't support fine-grained sharding.

**Note**: Queries using the `info()` function are analyzed but have no valid segmentation label candidates, since `info()` implicitly queries additional metrics making compartment determination impossible.

## TODOs

- [ ] Investigate how to properly support the `info()` function. Currently, queries using `info()` have no valid segmentation label candidates. See [Prometheus info function docs](https://prometheus.io/docs/prometheus/latest/querying/functions/#info).
