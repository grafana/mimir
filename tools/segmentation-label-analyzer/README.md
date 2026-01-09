# Segmentation Label Analyzer

Analyzes a Mimir tenant to identify good segmentation label candidates. A segmentation label is used to partition series into compartments for sharding.

## What it does

1. **Fetches all label names** from Mimir via `/api/v1/labels`
2. **Gets series counts per label** via `/api/v1/cardinality/label_values`
3. **Queries Loki** for user queries (from query-frontend) and rule queries (from ruler-query-frontend)
4. **Analyzes which labels** appear in queries as valid segmentation candidates (labels with equality or set matchers in all selectors)
5. **Computes a score** for each label and outputs ranked candidates

## Output columns

| Column | Description |
|--------|-------------|
| **Label name** | The label being evaluated as a segmentation candidate |
| **Score** | Overall score (0-1) combining all factors. Higher is better. |
| **Series** | Percentage of total series that have this label |
| **All queries** | Percentage of queries where compartment can be determined (weighted avg of user + rule) |
| **User queries** | Percentage of user queries (from query-frontend) where compartment can be determined |
| **Rule queries** | Percentage of rule queries (from ruler-query-frontend) where compartment can be determined |
| **Unique values** | Number of distinct values for this label |
| **Avg values/query** | Average number of distinct values referenced per query. Lower is better (ideally 1.0). |
| **Series values dist** | Normalized entropy (0-1) of series distribution across values. Higher = more uniform. |
| **Top values series %** | Series percentage for top 3 values (shows concentration) |
| **Query values dist** | Normalized entropy (0-1) of query distribution across values. Higher = more uniform. |
| **Top values queries %** | Query percentage for top 3 values (shows concentration) |

## Score computation

The score (0-1) considers these factors:

| Factor | Weight | Description |
|--------|--------|-------------|
| **Series coverage** | 40% | Percentage of total series that have this label. Higher is better - we need most series to have the label for effective sharding. |
| **Query coverage** | 30% | Percentage of queries where we can deterministically identify the compartment. Extrapolated across user and rule query observation windows. Penalized by average distinct values per query (see below). |
| **Query values distribution** | 15% | How evenly queries are distributed across label values (normalized entropy). A label where 99% of queries use one value means unbalanced query load. |
| **Series values distribution** | 15% | How evenly series are distributed across label values (normalized entropy). A label where 99% of series have one value is bad for balanced sharding. |

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

### Understanding distribution values

The distribution columns ("Series values distribution" and "Query values distribution") show normalized entropy values from 0 to 1:
- **1.0** = perfectly uniform distribution (ideal for balanced sharding/load)
- **0.0** = all items concentrated in a single value (worst case)

To interpret these values practically, consider how much the top value dominates. The table below shows approximate percentages for the most popular value, assuming a "power-law" style distribution:

| Entropy | Interpretation | Top value share (10 values) | Top value share (100 values) | Top value share (1000 values) |
|---------|----------------|----------------------------|-----------------------------|-----------------------------|
| 1.0 | Uniform | 10% | 1% | 0.1% |
| 0.9 | Near uniform | ~15-20% | ~3-5% | ~0.5-1% |
| 0.8 | Slight skew | ~25-30% | ~8-12% | ~2-4% |
| 0.7 | Moderate skew | ~35-45% | ~15-25% | ~5-10% |
| 0.5 | Significant skew | ~60-70% | ~40-50% | ~20-30% |
| 0.3 | Heavy skew | ~80-85% | ~65-75% | ~50-60% |
| 0.0 | Single value | 100% | 100% | 100% |

**Example**: If you see "Query values distribution = 0.8" for a label with 1000 unique values, the top ~2-4% of values likely account for a disproportionate share of queries. This is still reasonably good for load balancing.

**Note**: These are approximate guidelines. The exact distribution shape affects the percentages.

## TODOs

- [ ] Investigate how to properly support the `info()` function. Currently, queries using `info()` have no valid segmentation label candidates. See [Prometheus info function docs](https://prometheus.io/docs/prometheus/latest/querying/functions/#info).
