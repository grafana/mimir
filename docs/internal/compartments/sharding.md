# Sharding

Compartments use **two levels of sharding**: a series is first assigned to a read compartment, and then
to a partition within that compartment.

## Two-level sharding

1. **To a read compartment, by metric name.** Each series (and each metadata item) is assigned to a read
   compartment by hashing the tenant and the metric name:

   ```
   read_compartment = hash(user_id, metric_name) % num_read_compartments
   ```

   The assignment is per tenant: because the user ID is part of the hash, the same metric name can map
   to different compartments for different tenants.

2. **To a partition, by series labels.** Within a read compartment, the series is assigned to a partition
   using the existing series label-hash sharding that Mimir already uses.

## Sharded for the consumer, not the producer

Sharding is driven by the **read** side. The distributor shards across `N` topics, where `N` is the
number of **read** compartments. The number of write compartments can differ and is a deployment
concern; it does not change how series are sharded.

In this phase the number of read compartments is assumed **static**. Resizing the number of compartments
(and resharding existing series) is out of scope.

## Why the metric name is the segmentation label

The purpose of a segmentation label is that a query can be served from only the compartments that hold
the selected values. If a compartment has a problem, only queries that need data from that compartment
are affected — which is what reduces the query blast radius.

A good segmentation label has two properties:

1. The majority of queries filter it with an equality matcher.
2. Series are well distributed across its value space.

In the Prometheus ecosystem the **metric name** fits well: there are typically many distinct metric
names, and they are referenced in nearly every query. Conceptually this is similar to a partition key
in systems such as DynamoDB or Google Spanner, where data is sharded by a well-defined key.

In the OTLP ecosystem the choice is harder: tenants often have a very large number of series but
relatively few distinct metric names, so an OTLP tenant may need a different segmentation label (for
example `cluster` or `namespace`) that most of its queries match with equality.

### Outcome of the largest-tenants analysis

An analysis of the largest Grafana Labs tenants reached two conclusions:

- No tenant had a good segmentation label other than the metric name.
- The metric name is a good segmentation candidate for most tenants.

The table below reports the per-tenant outcome for every analysed tenant (see the column legend below
it). Each tenant is shown as an anonymized letter.

| Tenant | Eco | Unique names | Series dist | Top metric series % | Queries shardable | Avg metrics / query |
|--------|-----|--------------|-------------|---------------------|-------------------|---------------------|
| A  | Prometheus | 29061   | 0.91 | 4%  | 90%  | 1.53 |
| B  | Prometheus | 80942   | 0.96 | 4%  | 100% | 1.52 |
| C  | Prometheus | 51642   | 0.98 | 1%  | 87%  | 1.76 |
| D  | Prometheus | 48454   | 0.68 | 20% | 97%  | 1.34 |
| E  | Prometheus | 82321   | 0.92 | 4%  | 94%  | 1.33 |
| F  | Prometheus | 5527667 | 0.99 | 3%  | 100% | 1.01 |
| G  | Prometheus | 12520   | 0.93 | 7%  | 99%  | 1.51 |
| H  | Prometheus | 47837   | 0.94 | 3%  | 100% | 1.31 |
| I  | Prometheus | 34744   | 0.93 | 4%  | 100% | 1.13 |
| J  | Prometheus | 13474   | 0.70 | 36% | 96%  | 1.05 |
| K  | OTLP | 110268 | 0.99 | 4%  | 96%  | 1.15 |
| L  | OTLP | 7905   | 0.79 | 16% | 77%  | 2.13 |
| M  | OTLP | 7899   | 0.81 | 11% | 76%  | 2.13 |
| N  | OTLP | 8237   | 0.87 | 8%  | 77%  | 2.14 |
| O  | OTLP | 90205  | 0.82 | 19% | 100% | 1.10 |
| P  | OTLP | 15029  | 0.93 | 6%  | 87%  | 1.37 |
| Q  | OTLP | 5883   | 1.00 | 1%  | 85%  | 1.60 |
| R  | OTLP | 7601   | 0.75 | 20% | 96%  | 1.26 |
| S  | OTLP | 449    | 0.20 | 85% | 100% | 1.87 |
| T  | OTLP | 5043   | 0.97 | 4%  | 100% | 1.42 |
| U  | OTLP | 2901   | 0.70 | 33% | 96%  | 1.05 |
| V  | OTLP | 1199   | 0.87 | 9%  | 95%  | 1.63 |
| W  | OTLP | 7194   | 0.87 | 8%  | 100% | 1.86 |
| X  | OTLP | 855    | 1.00 | 2%  | 99%  | 1.06 |
| Y  | OTLP | 6018   | 0.85 | 12% | 90%  | 1.48 |
| Z  | OTLP | 5970   | 0.93 | 7%  | 93%  | 1.28 |
| AA | OTLP | 120196 | 0.90 | 3%  | 100% | 1.00 |

Column legend:

- **Tenant** — anonymized tenant identifier.
- **Eco** — metric ecosystem (Prometheus or OTLP).
- **Unique names** — number of distinct metric names.
- **Series dist** — how evenly series are spread across metric names (0–1; higher means more even, lower
  means concentrated on few names; **higher is better**).
- **Top metric series %** — share of all series that belong to the single highest-cardinality metric name
  (**lower is better**).
- **Queries shardable** — share of queries that can be sharded by metric name, i.e. that filter the
  metric name with an equality matcher (**higher is better**).
- **Avg metrics / query** — average number of distinct metric names referenced per query
  (**lower is better**).

Takeaway: for nearly all tenants the large majority of queries can be sharded by metric name, and for
most tenants no single metric name dominates the series. A minority of tenants concentrate a large
fraction of their series on one metric name (low "Series dist", high "Top metric series %").

### Caveat: limited blast-radius reduction in some cases

The blast-radius reduction is limited when:

- A tenant concentrates a large fraction of its series or queries on a single metric name, so most of
  its load still lands on one compartment.
- A query spans many metric names (many vector selectors, or metric-less queries), since each selector
  may resolve to a different compartment and the query then touches many (or all) of them.
